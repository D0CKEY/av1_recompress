from .gui_imports import *
from .gui_shared import *
from .gui_shared_worker import (
    CombinedStopEvent,
    get_denoise_level_from_task,
    get_hard_rotate_degrees_from_task,
    calculate_effective_max_encoded,
    format_predicted_size_with_audio,
    create_denoise_progress_callback,
    handle_reencode_copy_fallback,
    handle_ab_av1_not_found,
    read_denoise_level_file,
    write_denoise_level_file
)
import time


class NvencWorkerMixin:
    def nvenc_worker(self, worker_index):
        """Background worker for NVENC encoding tasks.
        
        Args:
            worker_index: Index of the worker thread (0-based).
            
        Processes videos from the NVENC queue, managing encoding, validation,
        and VMAF checks.
        """
    
        # Set worker index for logger
        # Use modulo so it works correctly even if there are more workers than loggers
        # The logger object is chosen based on modulo (logger_idx = worker_index % len(nvenc_loggers))
        # The logger object's logger_index does not change, so every worker writes to the log file
        # and console belonging to its own logger object (based on logger_index)
        # FIX #2: Robust fallback when nvenc_loggers is empty — prevents NoneType crash
        if len(self.nvenc_loggers) > 0:
            logger_idx = worker_index % len(self.nvenc_loggers)
            nvenc_logger = self.nvenc_loggers[logger_idx]
            # Set the logger's worker_index to the actual worker_index (for queue messages)
            # For log file selection, we use logger_index (which doesn't change),
            # so every worker writes to the log file belonging to its own logger object
            nvenc_logger.set_worker_index(worker_index)
        elif hasattr(self, 'svt_logger') and self.svt_logger is not None:
            nvenc_logger = self.svt_logger
        else:
            class _SilentLogger:
                def write(self, msg): pass
                def flush(self): pass
                def set_worker_index(self, idx): pass
                def set_current_video_path(self, path): pass
            nvenc_logger = _SilentLogger()
        
        debug_pause.gui_queue = self.encoding_queue
        
        # Add active video
        with self.nvenc_selection_lock:
            self.nvenc_active_videos.add(worker_index)
        
        current_video_path = None  # Store current video to remove it in finally block
        video_path = None  # CRITICAL: Initialize to prevent UnboundLocalError in finally block
        task = None  # CRITICAL: Initialize task wrapper to prevent UnboundLocalError
        worker_should_stop = False  # CRITICAL: Define in outer scope so it's accessible after try block
        final_task_taken = False  # CRITICAL: Prevents taking multiple "final" tasks
        try:
            while True:
                # THREAD-SAFETY FIX: Read encoding state once per iteration
                _, _, graceful_stop_nvenc = self.get_encoding_state()

                # CRITICAL FIX: If we already took a final task in previous iteration, EXIT NOW!
                # Exception: during graceful stop, continue to process remaining VMAF/PSNR tasks
                if final_task_taken and not graceful_stop_nvenc:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: Final task completed, exiting now.\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                
                # Check for app closing flag
                if is_app_closing():
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: Application closing detected, stopping.\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                
                # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
                _, _, graceful_stop = self.get_encoding_state()
                if STOP_EVENT.is_set():
                    if graceful_stop:
                        pass
                    else:
                        with console_redirect(nvenc_logger):
                            print(f"\n{t('log_immediate_stop_nvenc')} #{worker_index + 1}\n")
                    break
                if graceful_stop:
                    # GRACEFUL STOP FIX: NVENC worker ismet atveszi a VMAF feladatokat
                    # az SVT queue-bol, biztosítva a post-encoding VMAF/PSNR szamitas
                    # befejezeset. Ez szukseges, mert NVENC kódolás utan is keletkezhet
                    # VMAF feladat, es az SVT workerek mar leallhattak.
                    vmaf_task = self.get_next_svt_vmaf_task()
                    if vmaf_task is not None:
                        task = vmaf_task
                        worker_should_stop = False
                    else:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: Graceful stop - no more VMAF tasks, exiting.\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        break
                
                # Check if this worker should gracefully shut down (worker count reduced)
                configured_workers = self.get_configured_nvenc_workers()
                worker_should_stop = worker_index >= configured_workers
                
                video_stop_event = CombinedStopEvent(threading.Event(), STOP_EVENT)
                denoised_master_path = None
                keep_denoised_master = False
                
                try:
                    
                    if worker_should_stop:
                        # This worker is no longer needed due to reduced worker count
                        # Check if there are any tasks left - if yes, process ONE FINAL task
                        # If no, shut down gracefully immediately
                        task = self.get_next_nvenc_task()
                        if task is None:
                            # No tasks and worker not needed - shut down gracefully
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: Worker count reduced, no tasks left, stopping.\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            break  # Exit loop and stop worker
                        # If task exists, we'll process it below (one final task)
                        # CRITICAL: Set flag to exit after this task is processed!
                        final_task_taken = True
                    else:
                        # Worker is still configured - get next task normally
                        # LIST-BASED QUEUE: Get next task from pending list
                        task = self.get_next_nvenc_task()

                        if task is None:
                            # No pending tasks - worker auto-stops
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: No more tasks, stopping.\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            break


                
                
                    video_path = task['video_path']
                    # NOTE: video_path is already added to processing set by get_next_nvenc_task()
                    current_video_path = video_path  # Store current video
                    denoised_master_path = None      # Init for cleanup
                    keep_denoised_master = False     # Flag to preserve master for fallback
                    audio_size_mb = None             # Init to prevent UnboundLocalError in predicted size calculation

                    # Create per-video stop event for manual override support
                    # Thread-safe access to video_stop_events
                    with self.video_stop_events_lock:
                        if video_path not in self.video_stop_events:
                            self.video_stop_events[video_path] = threading.Event()
                        raw_video_stop_event = self.video_stop_events[video_path]
                    # NOTE: Do NOT clear() here! If stop_encoding_for_video() already set it,
                    # we must respect that and stop immediately.
                    video_stop_event = CombinedStopEvent(raw_video_stop_event, STOP_EVENT)

                    # FIX #1: finish_nvenc_task MUST be defined before the pre-stopped check below,
                    # otherwise NameError when a video is pre-stopped (stop_encoding_for_video).
                    def finish_nvenc_task():
                        nonlocal current_video_path, keep_denoised_master
                        # STOP EVENT CHECK: Ha stop event miatt állt le, master megőrzése újrafelhasználásra
                        if video_stop_event.is_set() and not keep_denoised_master:
                            keep_denoised_master = True
                            if denoised_master_path and denoised_master_path.exists():
                                with console_redirect(nvenc_logger):
                                    print(f"♻ Zajszűrt master megőrizve (stop event - újrafelhasználásra): {denoised_master_path.name}")
                        # Cleanup master
                        if denoised_master_path and denoised_master_path.exists() and not keep_denoised_master:
                            from .core_preamble_and_imports import DEBUG_MODE
                            if DEBUG_MODE:
                                with console_redirect(nvenc_logger):
                                    print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve: {denoised_master_path.name}")
                            else:
                                try:
                                    master_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                                    denoised_master_path.unlink()
                                    # Sidecar fájl törlése is
                                    denoised_master_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                                    with console_redirect(nvenc_logger):
                                        print(f"[DEL] Zajszűrt mesterdarab törölve (cleanup): {denoised_master_path.name} ({master_size_mb:.1f} MB felszabadítva)")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC cleanup, zajszűrt master takarítás | fájl: {denoised_master_path.name} ({master_size_mb:.1f} MB)\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except (OSError, IOError, AttributeError, ValueError):
                                    pass

                        # Clear current video path in logger
                        if hasattr(nvenc_logger, 'set_current_video_path'):
                            nvenc_logger.set_current_video_path(None)
                        
                        # LIST-BASED QUEUE: Mark task as complete
                        # This removes video_path from processing set
                        if current_video_path:
                            self.complete_nvenc_task(current_video_path)
                            current_video_path = None

                    # Check if stop was already requested for this video (race condition handling)
                    if video_stop_event.is_set():
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"  [WARN] NVENC task for {video_path.name} was pre-stopped, skipping\n")
                            except Exception:
                                pass
                        finish_nvenc_task()
                        continue  # Skip to next task

                    # Set current video path in logger for log storage
                    if hasattr(nvenc_logger, 'set_current_video_path'):
                        nvenc_logger.set_current_video_path(video_path)

                    # GUI FREEZE FIX: Pre-cleanup runs in worker thread (not GUI thread)
                    if task.get('needs_pre_cleanup'):
                        self._reencode_pre_cleanup(task, nvenc_logger)

                    output_file = task['output_file']
                    subtitle_files = task.get('subtitle_files') or []
                    invalid_subtitles = task.get('invalid_subtitles') or []
                    item_id = task['item_id']
                    orig_size_str = task['orig_size_str']
                    initial_min_vmaf = task['initial_min_vmaf']
                    vmaf_step = task['vmaf_step']
                    max_encoded = task['max_encoded']
                    # CRITICAL FIX: Use current resize settings if not explicitly set in task
                    # This allows users to change resize settings while encoding is running
                    resize_enabled = task.get('resize_enabled', getattr(self, 'current_resize_enabled', False))
                    resize_height = task.get('resize_height', getattr(self, 'current_resize_height', 1080))
                    audio_compression_enabled = task.get('audio_compression_enabled', getattr(self, 'current_audio_compression_enabled', False))
                    audio_compression_method = task.get('audio_compression_method', getattr(self, 'current_audio_compression_method', 'fast'))
                    # If combobox value is translated text, convert it
                    if audio_compression_method == t('audio_compression_fast'):
                        audio_compression_method = 'fast'
                    elif audio_compression_method == t('audio_compression_dialogue'):
                        audio_compression_method = 'dialogue'
                    
                    # Manuális quality check paraméter kiolvasása (ha van)
                    manual_quality_check = task.get('manual_quality_check', None)
                    manual_cq_value = task.get('manual_cq_value', task.get('target_cq'))


                    if STOP_EVENT.is_set():
                        # Request thread-safe status revert from main thread
                        self.encoding_queue.put_nowait(("revert_status_if_not_done", item_id, t('status_nvenc_queue'), orig_size_str))
                        with console_redirect(nvenc_logger):
                            print(f"\n{t('log_stop_request_nvenc')} #{worker_index + 1}\n")
                        finish_nvenc_task()
                        break
                
                    # Check if source video exists
                    if not video_path.exists():
                        with console_redirect(nvenc_logger):
                            print(t('log_source_not_found').format(path=video_path))
                        # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                        cached_values = task.get('cached_values', [])
                        completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, t('status_source_missing'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        # save_json references removed - DB save only in start_encoding and stop_encoding
                        finish_nvenc_task()
                        continue
                
                    with console_redirect(nvenc_logger):
                        print(f"\n{'*'*80}")
                        print(f"{t('log_nvenc_processing').format(filename=video_path.name)} (Worker #{worker_index + 1})")
                        print(t('log_full_path').format(path=video_path.absolute()))
                        print(f"{'*'*80}\n")
                        
                        # PROBE SOURCE VIDEO WITH VDUB2
                        vdub_disabled = task.get('vdub_validation_disabled', False)
                        if vdub_disabled:
                            print(f"  [INFO] VirtualDub2 source probe disabled by user setting")
                            probe_success = True
                        else:
                            print(t('log_probing_source'))
                            probe_success = check_source_video_with_vdub2(video_path, stop_event=video_stop_event)

                    if not probe_success:
                        with console_redirect(nvenc_logger):
                            print(t('log_probe_failed'))

                        # Fallback copy
                        copy_dest = get_copy_filename(video_path, self.source_path, self.dest_path)
                        copy_success = copy_video_fallback(video_path, copy_dest, subtitle_files, logger=nvenc_logger, invalid_subtitles=invalid_subtitles)
                        
                        if copy_success:
                            # Update GUI
                            try:
                                orig_size_mb = video_path.stat().st_size / (1024**2)
                                new_size_mb = copy_dest.stat().st_size / (1024**2)
                                orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            except (OSError, IOError, AttributeError, ValueError):
                                # File stat or calculation error - use fallback
                                orig_size_str = "-"
                                new_size_str = "-"
                                new_size_mb = 0.0
                                
                            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            self.video_to_output[video_path] = copy_dest
                            
                            self.encoding_queue.put_nowait(("update", item_id, t('status_completed_copy'), "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                            self.encoding_queue.put_nowait(("progress_bar", 0))
                            
                            # Update DB (async)
                            def update_db_after_copy():
                                try:
                                    self.update_single_video_in_db(
                                        video_path, item_id, t('status_completed_copy'), 
                                        "-", "-", "-", orig_size_str, 
                                        new_size_mb, 0.0, completed_date
                                    )
                                except (OSError, IOError, PermissionError):
                                    # File delete error - non-critical
                                    pass
                            threading.Thread(target=update_db_after_copy, daemon=True).start()
                            
                        else:
                            # Copy failed
                            self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                            self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                            
                        finish_nvenc_task()
                        continue
                
                    # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                    cached_values = task.get('cached_values', [])
                    completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""

                    # Check for stop
                    if not self.is_encoding:
                        # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                        cached_tags = task.get('cached_tags', ())
                        status = cached_values[self.COLUMN_INDEX['status']] if len(cached_values) > self.COLUMN_INDEX['status'] else ""
                        # Do not disturb completed or needs_check status
                        if not is_status_completed(status) and "completed" not in cached_tags and not is_status_needs_check(status) and "needs_check" not in cached_tags:
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                        finish_nvenc_task()
                        continue
                
                    with console_redirect(nvenc_logger):
                        print(f"{t('log_nvenc_slot_acquired')} #{worker_index + 1}\n")
                
                    def status_callback(msg):
                        # Add worker index to status messages from core modules if possible
                        prefix = f"NVENC #{worker_index + 1}"
                        if msg.startswith("NVENC"):
                            msg = msg.replace("NVENC", prefix, 1)

                        self.encoding_queue.put_nowait(("status_only", item_id, msg))
                
                    def progress_callback(msg):
                        # Add worker index to progress messages
                        prefix = f"NVENC #{worker_index + 1}"
                        if msg.startswith("NVENC"):
                            msg = msg.replace("NVENC", prefix, 1)

                        self.encoding_queue.put_nowait(("progress", item_id, msg))
                        self.update_estimated_end_time_from_progress(item_id, msg)
                
                    # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                    cached_values = task.get('cached_values', [])
                    completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                    localized_vmaf = format_localized_number(initial_min_vmaf, decimals=2)
                    self.encoding_queue.put_nowait(("update", item_id, f"NVENC #{worker_index + 1} CRF search (VMAF: {localized_vmaf})...", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    # When actually starting work, set encoding_nvenc tag (orange)
                    self.encoding_queue.put_nowait(("tag", item_id, "encoding_nvenc"))
                
                    # Store start time
                    self.encoding_start_times[item_id] = time.time()
                
                    # ===== DENOISE MASTER CREATION =====
                    # Check if denoise is enabled for this video
                    denoised_master_path = None  # Track for cleanup later
                    crf_search_input = video_path  # Default: use original video
                    original_video_path = video_path  # Store original for reference

                    # CRITICAL: If video_path is a denoised master, find the actual original source
                    # This is necessary to correctly calculate audio/subtitle overhead for size estimation
                    if '_denoised_master' in video_path.name:
                        found_original = self._find_original_source_for_master(video_path)
                        if found_original != video_path:
                            original_video_path = found_original
                            with console_redirect(nvenc_logger):
                                print(f"📂 Zajszűrt master észlelve - eredeti forrás használata sallang mérethez:")
                                print(f"   Master: {video_path.name}")
                                print(f"   Eredeti forrás: {original_video_path.name}")
                        else:
                            # Original source not found - audio overhead will be 0
                            with console_redirect(nvenc_logger):
                                print(f"[WARN] Zajszűrt master észlelve, de eredeti forrás nem található:")
                                print(f"   Master: {video_path.name}")
                                print(f"   A hangsáv mérete 0-ként lesz kezelve a becslésben!")

                    denoise_params_for_metadata = "" # Store applied filters for metadata
                    
                    # Check denoise level from cached tree value: 4=ultra-strong, 3=very-strong, 1=strong, 2=light, 0=disabled
                    denoise_level = get_denoise_level_from_task(
                        task=task,
                        column_index=self.COLUMN_INDEX,
                        video_denoise_enabled=getattr(self, 'video_denoise_enabled', None),
                        video_denoise_lock=getattr(self, 'video_denoise_lock', None)
                    )
                    hard_rotate_degrees = get_hard_rotate_degrees_from_task(
                        task=task,
                        column_index=self.COLUMN_INDEX,
                        video_hard_rotate_degrees=getattr(self, 'video_hard_rotate_degrees', None),
                        video_hard_rotate_lock=getattr(self, 'video_hard_rotate_lock', None)
                    )
                    
                    if denoise_level > 0:
                        # Show worker index in denoising status (prefix format for consistency)
                        denoising_status = f"NVENC {t('status_denoising')}"
                        # Add manual encoding suffix if this is a manual task
                        manual_cq = task.get('target_cq')
                        if manual_quality_check is not None and manual_cq is not None:
                            denoising_status += self._format_manual_status_suffix(manual_cq, manual_quality_check)
                        status_callback(denoising_status)
                        
                        with console_redirect(nvenc_logger):
                            print(f"\n🔇 Denoise enabled - creating lossless master...")

                        # Create master in dest folder
                        master_filename = f"{video_path.stem}_denoised_master.mkv"
                        # FIX #9: Removed redundant output_file redefinition —
                        # output_file is already set from task['output_file'] at line 174
                        denoised_master_path = output_file.parent / master_filename

                        # Ha már létezik a master fájl azonos denoise szinttel, újrahasználjuk
                        _nvenc_master_reused = False
                        if denoised_master_path.exists() and denoised_master_path.stat().st_size > 0:
                            level_file = denoised_master_path.with_suffix('.denoise_level')
                            stored_level = read_denoise_level_file(level_file)
                            if stored_level == denoise_level:
                                with console_redirect(nvenc_logger):
                                    print(f"\n♻ Meglévő master fájl újrahasználata (azonos zajszűrési szint: {denoise_level})")
                                    print(f"   Fájl: {denoised_master_path.name}")
                                    print(f"   Méret: {denoised_master_path.stat().st_size / (1024**2):.1f} MB")
                                    print(f"   ⚡ A zajszűrési fázis kihagyva!\n")
                                crf_search_input = denoised_master_path
                                _nvenc_master_reused = True
                                # VMAF reference és cleanup path beállítása újrahasznált master-hez
                                task['vmaf_reference_path'] = denoised_master_path
                                task['denoised_master_path_for_cleanup'] = denoised_master_path
                            else:
                                # Eltérő denoise szint - régi master törlése, újragenerálás
                                with console_redirect(nvenc_logger):
                                    if stored_level == -1:
                                        print(f"\n[WARN] Meglévő master fájl sidecar nélkül - újragenerálás szükséges")
                                    else:
                                        print(f"\n[WARN] Denoise szint változott ({stored_level} → {denoise_level}) - master újragenerálása")
                                try:
                                    denoised_master_path.unlink()
                                    level_file.unlink(missing_ok=True)
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC denoise szint változott, master újragenerálás | fájl: {denoised_master_path.name}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except OSError:
                                    pass

                        if not _nvenc_master_reused:
                            # Get total frame count and duration for progress calculation
                            total_frames_for_denoise = None
                            total_duration_for_denoise = None
                            try:
                                total_frames_for_denoise = self.get_tree_item_meta(item_id, 'source_frame_count')
                                total_duration_for_denoise = self.get_tree_item_meta(item_id, 'source_duration_seconds')
                            except (OSError, IOError, AttributeError, ValueError):
                                # Log writer error - non-critical
                                pass

                            # Denoise progress callback - updates Progress column
                            def denoise_progress_callback(current_frame, total_frames, fps, speed, current_time_str=None, total_duration_sec=None):
                                """Progress callback for denoising - shows time/total (fps, speed)."""
                                try:
                                    progress_text = ""
                                    # If time info is available, show "current / total" format
                                    if current_time_str and total_duration_sec:
                                        try:
                                            # Format total duration to HH:MM:SS
                                            m, s = divmod(int(total_duration_sec), 60)
                                            h, m = divmod(m, 60)
                                            total_time_str = f"{h:02d}:{m:02d}:{s:02d}"

                                            # Cut decimal part from current time (e.g. 00:04:33.10 -> 00:04:33)
                                            current_time_display = current_time_str.split('.')[0]

                                            progress_text = f"{current_time_display} / {total_time_str}"
                                        except (OSError, IOError, AttributeError, ValueError):
                                            # Log writer error - non-critical
                                            pass

                                    # Use percentage if time calculation failed or not available
                                    if not progress_text:
                                        if total_frames and total_frames > 0:
                                            percent = (current_frame / total_frames) * 100
                                            progress_text = f"{format_localized_number(percent, decimals=1)}%"
                                        else:
                                            progress_text = f"{current_frame:,} frames"

                                    # Append FPS and Speed - REMOVED per user request
                                    # progress_text += f" ({int(fps)} fps, {format_localized_number(speed, decimals=2)}x)"

                                    # Update ONLY progress column
                                    self.encoding_queue.put_nowait(("progress", item_id, progress_text))
                                except (OSError, IOError, PermissionError):
                                    # File delete error - non-critical
                                    pass

                            # Check if Hybrid is available for SMDegrain
                            master_success = False
                            hybrid_path = getattr(self, 'hybrid_path', None)
                            hybrid_path_str = hybrid_path.get() if hybrid_path else ""
                            deband_enabled = bool(task.get('deband_enabled', getattr(self, 'current_deband_enabled', True)))
                            force_8bit_master = bool(task.get('force_8bit_denoised_master', getattr(self, 'current_force_8bit_denoised_master', False)))

                            if hybrid_path_str and get_hybrid_paths(hybrid_path_str):
                                # Try SMDegrain first (higher quality)
                                with console_redirect(nvenc_logger):
                                    print(f"[TARGET] Using SMDegrain (Hybrid/VapourSynth)...")

                                master_success, denoise_info = create_smdegrain_master(
                                    input_path=video_path,
                                    output_path=denoised_master_path,
                                    hybrid_base_path=hybrid_path_str,
                                    use_nvenc=True,  # NVENC = HEVC lossless (fast GPU encoding)
                                    logger=nvenc_logger,
                                    stop_event=video_stop_event,
                                    progress_callback=denoise_progress_callback,
                                    total_frames=total_frames_for_denoise,
                                    total_duration_sec=total_duration_for_denoise,
                                    denoise_level=denoise_level,
                                    deband_enabled=deband_enabled,
                                    force_8bit_master=force_8bit_master,
                                )
                                if master_success == 'partial':
                                    # vspipe had errors (e.g. lsmas last frame) but master is usable
                                    task['denoise_partial'] = True
                                    with console_redirect(nvenc_logger):
                                        print(f"[WARN] SMDegrain részleges siker - a végeredmény ellenőrizendő lesz")
                                if master_success:
                                    denoise_params_for_metadata = denoise_info

                            if not master_success:
                                # Fallback to vaguedenoiser
                                if hybrid_path_str:
                                    with console_redirect(nvenc_logger):
                                        print(f"[WARN] SMDegrain failed, falling back to vaguedenoiser...")

                                master_success, denoise_info = create_denoised_lossless_master(
                                    input_path=video_path,
                                    output_path=denoised_master_path,
                                    use_nvenc=True,  # NVENC = HEVC lossless (fast GPU encoding)
                                    logger=nvenc_logger,
                                    stop_event=video_stop_event,
                                    progress_callback=denoise_progress_callback,
                                    total_frames=total_frames_for_denoise,
                                    total_duration_sec=total_duration_for_denoise,
                                    denoise_level=denoise_level,
                                    force_8bit_master=force_8bit_master,
                                )
                                if master_success:
                                    denoise_params_for_metadata = denoise_info

                            if master_success:
                                with console_redirect(nvenc_logger):
                                    print(f"[OK] Master created: {denoised_master_path.name}")
                                # Sidecar fájl írása a denoise szint tárolásához
                                write_denoise_level_file(denoised_master_path.with_suffix('.denoise_level'), denoise_level)
                                # Use master for CRF search and encoding
                                crf_search_input = denoised_master_path
                                # VMAF reference és cleanup path beállítása
                                task['vmaf_reference_path'] = denoised_master_path
                                task['denoised_master_path_for_cleanup'] = denoised_master_path
                            else:
                                with console_redirect(nvenc_logger):
                                    print(f"[WARN] Master creation failed, using original file without denoising")
                                denoised_master_path = None  # No cleanup needed
                
                    # CRF search
                    # IMPORTANT: Check that the same video_path is used for CRF search and encoding
                    # If denoise is enabled, crf_search_input is the master
                    crf_search_source = crf_search_input  # Use master if denoise, else original
                    video_path_abs = crf_search_source.absolute()
                    try:
                        status_callback(t('status_nvenc_crf_search'))
                        # Clear progress column from previous steps (e.g. denoise)
                        self.encoding_queue.put_nowait(("progress", item_id, "-"))
                        
                        with console_redirect(nvenc_logger):
                            print(t('log_nvenc_crf_search').format(filename=original_video_path.name))
                            if denoised_master_path:
                                print(f"🔇 CRF search on denoised master: {crf_search_source.name}")
                            else:
                                print(t('log_crf_file_check').format(path=video_path_abs))
                            
                            # Max encoded mode check (full video vs video track only)
                            max_encoded_mode = self.max_encoded_mode.get() if hasattr(self, 'max_encoded_mode') else 'full'
                            
                            # Denoise size correction
                            # FONTOS: Az ab-av1 a max-encoded százalékot a VIDEÓSTREAM méretére vonatkoztatja!
                            effective_max_encoded = max_encoded
                            
                            if denoised_master_path and denoised_master_path.exists():
                                # ZAJSZŰRÉS esetén MINDIG kell korrekció (master fájl vs eredeti)
                                try:
                                    orig_size = original_video_path.stat().st_size
                                    master_size = denoised_master_path.stat().st_size
                                    # User requested NOT to use metadata for audio size calculation as it is often unreliable.
                                    # We will instead rely solely on the exact video stream packet scan (below) to deduce the rest.
                                    audio_size_mb = 0
                                    audio_size_bytes = 0
                                    
                                    if master_size > 0:
                                        # PONTOS videó stream mérés (packet scan) indítása MINDIG
                                        from .core_audio_video_ops import get_video_stream_size_bytes_exact
                                        
                                        with console_redirect(nvenc_logger):
                                            print(f"[STATS] Pontos videó stream méret mérése (packet scan)...")
                                        
                                        exact_video_size = get_video_stream_size_bytes_exact(original_video_path)
                                        
                                        if exact_video_size:
                                            original_video_size = exact_video_size
                                            
                                            # Ha van pontos videóméretünk, akkor a hangméretet is pontosíthatjuk (Fájl - Videó)
                                            # Ez tartalmazza a container overheadet is, de biztonságosabb a maradékot hangnak/egyébnek tekinteni.
                                            estimated_audio_overhead = max(0, orig_size - original_video_size)
                                            # Csak akkor használjuk felül, ha a metaadat alapú hangméret gyanús (pl. 0 vagy > overhead)
                                            # De a legbiztosabb, ha ezt használjuk a kivonáshoz a 'Teljes videó' módnál.
                                            if audio_size_mb == 0 or estimated_audio_overhead < audio_size_bytes:
                                                audio_size_bytes = estimated_audio_overhead
                                                audio_size_mb = audio_size_bytes / (1024 * 1024)
                                                
                                            with console_redirect(nvenc_logger):
                                                print(f"   [OK] Videó stream méret: {exact_video_size/(1024**2):.1f} MB")
                                                print(f"   [OK] Korrigált egyéb adat (hang+overhead): {audio_size_mb:.1f} MB")
                                        else:
                                            # Fallback
                                            original_video_size = max(0, orig_size - audio_size_bytes)
                                            if original_video_size <= 0:
                                                original_video_size = orig_size
                                                with console_redirect(nvenc_logger):
                                                     print(f"   [WARN] Pontos mérés sikertelen, teljes fájlméret fallback")
                                        
                                        original_video_size = max(0, original_video_size)
                                        
                                        if max_encoded_mode == 'video':
                                            target_video_size = original_video_size * (max_encoded / 100.0)
                                            expected_final_size = target_video_size + audio_size_bytes
                                        else:
                                            target_final_size = orig_size * (max_encoded / 100.0)
                                            target_video_size = target_final_size - audio_size_bytes
                                            target_video_size = max(0, target_video_size)
                                            expected_final_size = target_final_size
                                        
                                        effective_max_encoded = (target_video_size / master_size) * 100.0
                                        effective_max_encoded = round(effective_max_encoded, 2)
                                        effective_max_encoded = max(0.01, effective_max_encoded)
                                        
                                        mode_label = "Videósáv" if max_encoded_mode == 'video' else "Teljes videó"
                                        with console_redirect(nvenc_logger):
                                            print(f"⚖ Zajszűrés méret korrekció ({mode_label} mód):")
                                            print(f"   Eredeti: {orig_size/(1024**2):.1f} MB, Videó: {original_video_size/(1024**2):.1f} MB")
                                            print(f"   Master: {master_size/(1024**2):.1f} MB, Audio: {audio_size_mb:.1f} MB")
                                            print(f"   Cél videó: {target_video_size/(1024**2):.1f} MB")
                                            print(f"   Várható végső: {expected_final_size/(1024**2):.1f} MB ({(expected_final_size/orig_size)*100:.1f}%)")
                                            print(f"   -> ab-av1: {effective_max_encoded:.2f}%")
                                except Exception as e:
                                    with console_redirect(nvenc_logger):
                                        print(f"[WARN] Size correction error: {e}")
                            elif max_encoded_mode == 'full':
                                # NORMÁL ÁTKÓDOLÁS + teljes videó mód: korrekció kell
                                try:
                                    orig_size = original_video_path.stat().st_size
                                    audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
                                    audio_size_bytes = audio_size_mb * 1024 * 1024
                                    
                                    original_video_size = orig_size - audio_size_bytes
                                    original_video_size = max(0, original_video_size)
                                    
                                    if original_video_size > 0:
                                        target_final_size = orig_size * (max_encoded / 100.0)
                                        target_video_size = target_final_size - audio_size_bytes
                                        target_video_size = max(0, target_video_size)
                                        
                                        effective_max_encoded = (target_video_size / original_video_size) * 100.0
                                        effective_max_encoded = round(effective_max_encoded, 2)
                                        effective_max_encoded = max(0.01, effective_max_encoded)
                                        
                                        with console_redirect(nvenc_logger):
                                            print(f"⚖ Normál méret korrekció (Teljes videó mód):")
                                            print(f"   Eredeti: {orig_size/(1024**2):.1f} MB, Videó: {original_video_size/(1024**2):.1f} MB")
                                            print(f"   Audio: {audio_size_mb:.1f} MB")
                                            print(f"   Cél végső ({max_encoded}%): {target_final_size/(1024**2):.1f} MB")
                                            print(f"   Cél videó: {target_video_size/(1024**2):.1f} MB")
                                            print(f"   -> ab-av1: {effective_max_encoded:.2f}%")
                                except Exception as e:
                                    with console_redirect(nvenc_logger):
                                        print(f"[WARN] Size correction error: {e}")
                            else:
                                # Videósáv mód, normál átkódolás: nincs korrekció
                                with console_redirect(nvenc_logger):
                                    print(f"⚖ Videósáv mód: ab-av1 max-encoded-percent: {max_encoded}% (korrekció nélkül)")

                            cq_result_nvenc = run_crf_search(crf_search_source, encoder='av1_nvenc', initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step, max_encoded_percent=effective_max_encoded, progress_callback=status_callback, logger=nvenc_logger, stop_event=video_stop_event, crf_increment=int(self.crf_increment.get()))
                            print(f"[OK] NVENC CRF search done: {cq_result_nvenc}")
                    except FileNotFoundError as e:
                        # Ab-av1.exe not found - fatal error
                        handle_ab_av1_not_found(
                            error=e,
                            item_id=item_id,
                            orig_size_str=orig_size_str,
                            task=task,
                            column_index=self.COLUMN_INDEX,
                            encoding_queue=self.encoding_queue,
                            stop_event=STOP_EVENT,
                            set_encoding_state=self.set_encoding_state,
                            root=self.root,
                            is_app_closing_func=is_app_closing,
                            messagebox=messagebox,
                            log_writer=LOG_WRITER,
                            logger=nvenc_logger,
                            console_redirect=console_redirect,
                            lang='en'
                        )
                        finish_nvenc_task()
                        continue
                    except EncodingStopped:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        if tree_data:
                            current_values = tree_data.get('values', [])
                            tags = tree_data.get('tags', ())
                        else:
                            current_values = []
                            tags = ()
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        # Do not disturb completed or needs_check status
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                        finish_nvenc_task()
                        continue
                
                    if len(cq_result_nvenc) == 3 and cq_result_nvenc[2] is True:
                        # VMAF fallback elfogyott -> SVT queue-ba (only when third element is exactly True)
                        cq_value_nvenc, vmaf_value_nvenc, fallback_exhausted = cq_result_nvenc
                        with console_redirect(nvenc_logger):
                            print(f"\n[WARN] NVENC VMAF fallback elfogyott -> automatikusan SVT-AV1 queue-ba helyezés")
                    
                        if output_file.exists() and not DEBUG_MODE:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC VMAF fallback elfogyott, SVT queue-ba helyezés | fájl: {output_file.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            try:
                                output_file.unlink()
                            except (OSError, PermissionError):
                                time.sleep(1)
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    pass
                    
                        # LIST-BASED QUEUE: Add fallback task to SVT queue
                        denoised_master_to_pass = None
                        if denoised_master_path and denoised_master_path.exists():
                            keep_denoised_master = True
                            denoised_master_to_pass = denoised_master_path
                            with console_redirect(nvenc_logger):
                                print(f"♻ Denoised master preserválva SVT-AV1 számára: {denoised_master_path.name}")
                        
                        success = self.add_to_svt_queue(
                            video_path=video_path,
                            item_id=item_id,
                            task_type='encode',
                            is_manual=False,
                            output_file=output_file,
                            subtitle_files=subtitle_files,
                            invalid_subtitles=invalid_subtitles,
                            orig_size_str=orig_size_str,
                            initial_min_vmaf=initial_min_vmaf,
                            vmaf_step=vmaf_step,
                            max_encoded=max_encoded,
                            resize_enabled=resize_enabled,
                            resize_height=resize_height,
                            audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method,
                            denoise_params=denoise_params_for_metadata,
                            denoised_master_path=denoised_master_to_pass,
                            reason='nvenc_fallback_exhausted'
                        )
                        
                        if success:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                    else:
                        # Unpack 3-tuple: (crf_value, vmaf_value, predicted_size_mb)
                        # Note: if predicted_size_mb is True, it was already handled above (fallback exhausted)
                        cq_value_nvenc, vmaf_value_nvenc, predicted_size_mb_nvenc = cq_result_nvenc
                    
                        # Apply max CQ limit if set
                        max_cq_limit = getattr(self, 'current_max_cq_limit', 0)
                        if max_cq_limit > 0 and cq_value_nvenc > max_cq_limit:
                            original_cq = cq_value_nvenc
                            cq_value_nvenc = float(max_cq_limit)
                            with console_redirect(nvenc_logger):
                                print(f"[INFO] CQ limited: {format_localized_number(original_cq, decimals=1)} -> {max_cq_limit} (max CQ limit)")
                            vmaf_value_nvenc = None
                            predicted_size_mb_nvenc = None
                
                        # Display predicted size with ~ prefix if available
                        # Add audio streams size for more accurate total file size estimate
                        predicted_size_str = "-"
                        current_audio_size_mb = 0.0
                        total_predicted_mb = 0.0
                        if predicted_size_mb_nvenc is not None and predicted_size_mb_nvenc is not True and predicted_size_mb_nvenc > 0:
                            # Calculate total estimated size
                            current_audio_size_mb = 0.0
                            if audio_size_mb is not None:
                                # Use variable from sizing logic blocks (zajszűrés/full mode)
                                current_audio_size_mb = audio_size_mb
                            else:
                                # Not defined - calculate from ORIGINAL source file
                                try:
                                    # More accurate method: Total file - Video track = Audio + extras
                                    from .core_audio_video_ops import get_video_track_size_bytes
                                    
                                    total_file_size = video_path.stat().st_size
                                    video_track_size = get_video_track_size_bytes(video_path)
                                    
                                    if video_track_size and video_track_size > 0:
                                        # This includes audio + subtitles + container overhead + metadata
                                        audio_and_extras_bytes = total_file_size - video_track_size
                                        current_audio_size_mb = max(0, audio_and_extras_bytes) / (1024 * 1024)
                                    else:
                                        # Fallback: FFprobe audio streams only (less accurate)
                                        current_audio_size_mb = get_audio_streams_total_size_mb(video_path)
                                except (OSError, IOError, ValueError, AttributeError):
                                    # Audio size calculation failed - last resort fallback
                                    try:
                                        current_audio_size_mb = get_audio_streams_total_size_mb(video_path)
                                    except (OSError, IOError, AttributeError, ValueError):
                                        # Audio size calculation failed - use zero
                                        current_audio_size_mb = 0.0
                            
                            total_predicted_mb = predicted_size_mb_nvenc + current_audio_size_mb
                            predicted_size_str = f"~{format_localized_number(total_predicted_mb, decimals=1)} MB"
                            with console_redirect(nvenc_logger):
                                    print(f"[STATS] Becsült végső méret: videó ~{format_localized_number(predicted_size_mb_nvenc, decimals=1)} MB + hang ~{format_localized_number(current_audio_size_mb, decimals=1)} MB = ~{format_localized_number(total_predicted_mb, decimals=1)} MB")
                    
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        vmaf_display = format_localized_number(vmaf_value_nvenc, decimals=2) if vmaf_value_nvenc is not None else "-"
                        # Update GUI with predicted size
                        
                        # Add worker index to encoding status
                        status_msg = t('status_nvenc_encoding')
                        if status_msg.startswith("NVENC"):
                            status_msg = status_msg.replace("NVENC", f"NVENC #{worker_index + 1}", 1)
                        else:
                             status_msg = f"NVENC #{worker_index + 1} {status_msg}"
                        
                        self.encoding_queue.put_nowait(("update", item_id, status_msg, str(int(cq_value_nvenc)), vmaf_display, "-", "-", orig_size_str, predicted_size_str, "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "encoding_nvenc"))
                
                    # Kezdési időpont tárolása
                    self.encoding_start_times[item_id] = time.time()
                
                    # NVENC kódolás
                    # FONTOS: Ellenőrizzük, hogy ugyanaz a path kerül használatra a kódoláshoz, mint a CRF kereséshez
                    if denoised_master_path and denoised_master_path.exists():
                        encoding_source = denoised_master_path
                    else:
                        encoding_source = video_path
                    video_path_abs_check = encoding_source.absolute()
                    if video_path_abs_check != video_path_abs:
                        error_msg = f"FATAL ERROR: The video_path changed between CRF search and encoding!\n\nCRF search file: {video_path_abs}\nEncoding file: {video_path_abs_check}\n\nThis means that CRF search was done for one file, but encoding started for another. This is a critical error!\n\nThe program will stop immediately."
                        with console_redirect(nvenc_logger):
                            print(f"\n{'='*80}")
                            print(f"[WARN][WARN][WARN] FATAL ERROR [WARN][WARN][WARN]")
                            print(f"{'='*80}")
                            print(error_msg)
                            print(f"{'='*80}\n")
                        # Write to log file
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"\n{'='*80}\n")
                                LOG_WRITER.write(f"[WARN][WARN][WARN] FATAL ERROR [WARN][WARN][WARN]\n")
                                LOG_WRITER.write(f"{'='*80}\n")
                                LOG_WRITER.write(f"{error_msg}\n")
                                LOG_WRITER.write(f"{'='*80}\n\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        # Immediate stop
                        STOP_EVENT.set()
                        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                        self.set_encoding_state(graceful_stop_requested=True)
                        # MessageBox error message (in GUI thread)
                        # DEADLOCK FIX: Check if app is closing before calling root.after()
                        try:
                            if not is_app_closing() and self.root.winfo_exists():
                                self.root.after(0, lambda: messagebox.showerror(
                                    "FATAL ERROR",
                                    error_msg
                                ))
                                # Várunk egy kicsit, hogy a MessageBox megjelenjen
                                time.sleep(0.5)
                        except tk.TclError:
                            pass  # root already destroyed
                        raise ValueError(error_msg)
                    nvenc_fallback_requested = False
                    additional_skipped_subtitles = []  # Initialize for encode_video case
                    extracted_audio_tracks = []  # Track for cleanup
                    video_only_temp_file = None  # Track for cleanup
                    
                    try:
                        with console_redirect(nvenc_logger):
                            # ===== NVENC WORKFLOW: SEPARATE ENCODE + MERGE (MATCHING SVT-AV1) =====
                            # 1. Encode video-only to temp file
                            # 2. Extract audio tracks from source to separate files
                            # 3. Merge video + audio + subtitles/metadata
                            
                            print(f"\n{'='*80}")
                            print(f"🎬 NVENC WORKFLOW: Szétbontás-összerakás módszer")
                            print(f"{'='*80}")
                            print(f"[SCAN] Encoding file check (full path): {video_path_abs_check}")
                            if denoised_master_path:
                                print(f"🔇 Using denoised master for encoding: {encoding_source.name}")
                            
                            # Step 1: Encode video-only to temp file
                            video_only_temp_file = output_file.with_name(f"{output_file.stem}_video_only.mkv")
                            print(f"\n📽 1. LÉPÉS: Videó-only AV1 kódolás")
                            print(f"   Forrás: {encoding_source.name}")
                            print(f"   Temp fájl: {video_only_temp_file.name}")
                            print(f"   Target file: {output_file.absolute()}")
                            
                            # DEBUG_MODE: Skip video encode if file already exists
                            from .core_preamble_and_imports import DEBUG_MODE
                            if DEBUG_MODE and video_only_temp_file.exists() and video_only_temp_file.stat().st_size > 0:
                                print(f"\n[STOP] DEBUG: Meglévő video-only fájl használata (gyors teszt mód)")
                                print(f"   Fájl: {video_only_temp_file.name}")
                                print(f"   Méret: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")
                                print(f"   ⚡ Video-only kódolás kihagyva!\n")
                                success_video = True
                            else:
                                # Temporarily disable audio for this encode
                                success_video, _ = encode_single_attempt(
                                    encoding_source,
                                    video_only_temp_file,
                                    cq_value_nvenc,
                                    [],  # No subtitle files for video-only
                                    'av1_nvenc',
                                    progress_callback,
                                    stop_event=video_stop_event,
                                    vmaf_value=vmaf_value_nvenc,
                                    resize_enabled=resize_enabled,
                                    resize_height=resize_height,
                                    audio_compression_enabled=False,  # No audio compression for video-only
                                    audio_compression_method='fast',
                                    logger=nvenc_logger,
                                    pre_invalid_subtitles=[],
                                    cached_video_metadata=None,
                                    original_input_path=None,  # Video-only encode
                                    denoise_enabled=(denoised_master_path is not None),
                                    denoise_params=denoise_params_for_metadata,
                                    include_audio=False,  # Video-only encoding
                                    hard_rotate_degrees=hard_rotate_degrees
                                )
                            
                            if not success_video or not video_only_temp_file.exists():
                                print(f"[ERROR] Videó-only kódolás sikertelen!")
                                # Temp fájlok megőrzése hiba esetén
                                if video_only_temp_file and video_only_temp_file.exists():
                                    print(f"[STOP] HIBA: Video-only temp fájl megőrizve hibakereséshez: {video_only_temp_file.name}")
                                success_nvenc = False
                                # Potenciális fallback kezelés: ha a videó kódolás azért bukott el, 
                                # mert az NVENC (driver) valamiért nem szerette, akkor mehet SVT-be.
                                nvenc_fallback_requested = True 
                                raise Exception("Video-only encoding failed")
                            else:
                                if not (DEBUG_MODE and video_only_temp_file.exists()):
                                    print(f"[OK] Videó-only kódolás kész: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")

                                # Step 2: Extract audio tracks from source
                                print(f"\n🎵 2. LÉPÉS: Hangsávok kibontása a forrásból")
                                print(f"   Forrás: {original_video_path.name}")
                                
                                extracted_audio_tracks = extract_audio_tracks_with_metadata(
                                    original_video_path,
                                    output_file.parent,
                                    logger=nvenc_logger,
                                    stop_event=video_stop_event
                                )
                                
                                # Step 3: Merge video + audio + subtitles/metadata
                                print(f"\n[TOOL] 3. LÉPÉS: Összefűzés (videó + hang + feliratok)")
                                
                                success_merge = merge_video_audio_subtitles(
                                    video_only_temp_file,
                                    extracted_audio_tracks,
                                    original_video_path,
                                    output_file,
                                    logger=nvenc_logger,
                                    stop_event=video_stop_event,
                                    external_subtitle_files=subtitle_files
                                )
                                
                                if success_merge:
                                    print(f"[OK] Összefűzés sikeres!")
                                    success_nvenc = True
                                    
                                    # CSAK sikeres merge után töröljük a temp fájlokat
                                    # Cleanup temp video file
                                    if video_only_temp_file and video_only_temp_file.exists():
                                        if DEBUG_MODE:
                                            print(f"[STOP] DEBUG: Video-only temp fájl megőrizve: {video_only_temp_file.name}")
                                        else:
                                            try:
                                                video_only_temp_file.unlink()
                                                print(f"[DEL] Video-only temp fájl törölve")
                                                if LOG_WRITER:
                                                    try:
                                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC sikeres összefűzés, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                                        LOG_WRITER.flush()
                                                    except Exception:
                                                        pass
                                            except OSError as e:
                                                print(f"[WARN] Temp fájl törlés hiba: {e}")
                                     
                                    # Cleanup extracted audio files
                                    if extracted_audio_tracks:
                                        cleanup_extracted_audio_files(extracted_audio_tracks, logger=nvenc_logger)
                                else:
                                    print(f"[ERROR] Összefűzés sikertelen!")
                                    success_nvenc = False
                                    # HIBA ESETÉN NE TÖRÖLJÜK a temp fájlokat!
                                    print(f"\n[STOP] HIBA: Temp fájlok megőrizve újraindításhoz:")
                                    if video_only_temp_file and video_only_temp_file.exists():
                                        print(f"   - Video-only: {video_only_temp_file.name}")
                                    if extracted_audio_tracks:
                                        for track in extracted_audio_tracks:
                                            if track.get('path') and track['path'].exists():
                                                print(f"   - Audio: {track['path'].name}")
                                    print(f"   Kis javítással újrakezdhető a művelet!\n")
                                    raise Exception("Merge failed")
                            
                            print(f"{'='*80}\n")

                    except NVENCFallbackRequired:
                        nvenc_fallback_requested = True
                        success_nvenc = False
                        # Cleanup on fallback
                        if video_only_temp_file and video_only_temp_file.exists() and not DEBUG_MODE:
                            try:
                                video_only_temp_file.unlink()
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC fallback, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except OSError:
                                pass
                        if extracted_audio_tracks:
                            cleanup_extracted_audio_files(extracted_audio_tracks, logger=nvenc_logger)

                    except EncodingStopped:
                        # Cleanup temp files if interrupt
                        if video_only_temp_file and video_only_temp_file.exists():
                            if DEBUG_MODE:
                                with console_redirect(nvenc_logger):
                                    print(f"[STOP] DEBUG: Video-only temp fájl megőrizve: {video_only_temp_file.name}")
                            else:
                                try:
                                    video_only_temp_file.unlink()
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC EncodingStopped, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except OSError:
                                    pass
                        if extracted_audio_tracks:
                            cleanup_extracted_audio_files(extracted_audio_tracks, logger=nvenc_logger)
                            
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        if tree_data:
                            current_values = tree_data.get('values', [])
                            tags = tree_data.get('tags', ())
                        else:
                            current_values = []
                            tags = ()
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                        finish_nvenc_task()
                        continue
                    except Exception as e:
                        # Ha leállítás történt, ne fallbackeljen SVT-be, hanem álljon le tisztán
                        if STOP_EVENT.is_set():
                            # Cleanup temp files if interrupt
                            if video_only_temp_file and video_only_temp_file.exists():
                                if DEBUG_MODE:
                                    with console_redirect(nvenc_logger):
                                        print(f"[STOP] DEBUG: Video-only temp fájl megőrizve: {video_only_temp_file.name}")
                                else:
                                    try:
                                        video_only_temp_file.unlink()
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC azonnali leállítás, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                                LOG_WRITER.flush()
                                            except Exception:
                                                pass
                                    except OSError:
                                        pass
                            if extracted_audio_tracks:
                                cleanup_extracted_audio_files(extracted_audio_tracks, logger=nvenc_logger)
                            
                            # A státusz visszaállítást a stop_encoding_immediate végzi, de biztos ami biztos
                            # itt is jelezhetjük a logban
                            with console_redirect(nvenc_logger):
                                print(f"[STOP] Folyamat megszakítva (Azonnali leállítás)")
                                
                            finish_nvenc_task()
                            continue

                        # General error fallback to SVT-AV1
                        with console_redirect(nvenc_logger):
                            print(f"\n[WARN] NVENC encoding error: {e}")
                            print(f"[WARN] NVENC error -> placing in SVT-AV1 queue")
                        success_nvenc = False
                        nvenc_fallback_requested = True
                        
                        # Cleanup on error
                        if video_only_temp_file and video_only_temp_file.exists() and not DEBUG_MODE:
                            try:
                                video_only_temp_file.unlink()
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC hiba fallback, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except OSError:
                                pass
                        if extracted_audio_tracks:
                            cleanup_extracted_audio_files(extracted_audio_tracks, logger=nvenc_logger)
                
                    if nvenc_fallback_requested:
                        with console_redirect(nvenc_logger):
                            print(f"\n[WARN] NVENC fallback -> placing in SVT-AV1 queue")
                    
                        if output_file.exists() and not DEBUG_MODE:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC fallback SVT-be, output törlés | fájl: {output_file.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            try:
                                output_file.unlink()
                            except (OSError, PermissionError):
                                time.sleep(1)
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    pass
                    
                        # LIST-BASED QUEUE: Fallback to SVT (nvenc_fallback_during_encode)
                        denoised_master_to_pass = None
                        if denoised_master_path and denoised_master_path.exists():
                            keep_denoised_master = True
                            denoised_master_to_pass = denoised_master_path
                            with console_redirect(nvenc_logger):
                                print(f"♻ Denoised master preserválva SVT-AV1 számára: {denoised_master_path.name}")

                        self.add_to_svt_queue(
                            video_path=video_path, item_id=item_id, task_type='encode', is_manual=False,
                            output_file=output_file, subtitle_files=subtitle_files,
                            invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                            initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                            max_encoded=max_encoded, resize_enabled=resize_enabled,
                            resize_height=resize_height, audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method,
                            denoise_params=denoise_params_for_metadata,
                            denoised_master_path=denoised_master_to_pass,
                            reason='nvenc_fallback_during_encode'
                        )
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                
                    if not self.is_encoding:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                
                    is_valid = False
                    used_encoder = "NVENC"
                    final_cq = cq_value_nvenc
                    final_vmaf = vmaf_value_nvenc
                
                    if not success_nvenc and not nvenc_fallback_requested:
                        # If encoding failed but no fallback was requested, fallback to SVT-AV1
                        with console_redirect(nvenc_logger):
                            print(f"\n[WARN] NVENC encoding failed -> placing in SVT-AV1 queue")
                    
                        if output_file.exists() and not DEBUG_MODE:
                            try:
                                output_file.unlink()
                            except (OSError, PermissionError):
                                # Retry once after a short delay
                                time.sleep(1)
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    pass # Ignore if still fails, SVT worker might overwrite or fail explicitly
                    
                        # LIST-BASED QUEUE: Fallback to SVT (nvenc_encoding_failed)
                        denoised_master_to_pass = None
                        if denoised_master_path and denoised_master_path.exists():
                            keep_denoised_master = True
                            denoised_master_to_pass = denoised_master_path
                            with console_redirect(nvenc_logger):
                                print(f"♻ Denoised master preserválva SVT-AV1 számára: {denoised_master_path.name}")

                        self.add_to_svt_queue(
                            video_path=video_path, item_id=item_id, task_type='encode', is_manual=False,
                            output_file=output_file, subtitle_files=subtitle_files,
                            invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                            initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                            max_encoded=max_encoded, resize_enabled=resize_enabled,
                            resize_height=resize_height, audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method,
                            denoise_params=denoise_params_for_metadata,
                            denoised_master_path=denoised_master_to_pass,
                            reason='nvenc_encoding_failed'
                        )
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json references removed - DB save only in start_encoding and stop_encoding
                        finish_nvenc_task()
                        continue
                
                    if success_nvenc:
                        # Check if video is already "Completed"
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        if tree_data:
                            current_values = tree_data.get('values', [])
                            tags_before_validation = tree_data.get('tags', ())
                        else:
                            current_values = task.get('cached_values', [])
                            tags_before_validation = task.get('cached_tags', ())
                        
                        status_before_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        is_already_completed = (
                            is_status_completed(status_before_validation) or 
                            "completed" in tags_before_validation
                        )
                    
                        if is_already_completed:
                            is_valid = True
                            finish_nvenc_task()
                            continue
                    
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        vmaf_display = format_localized_number(vmaf_value_nvenc, decimals=2) if vmaf_value_nvenc is not None else "-"
                        # Denoise params are already in file metadata, don't add to VMAF column
                        # if denoise_params_for_metadata:
                        #     vmaf_display += f" [{denoise_params_for_metadata}]"
                        self.encoding_queue.put_nowait(("update", item_id, "NVENC validating...", str(int(cq_value_nvenc)), vmaf_display, "-", "100%", orig_size_str, "-", "-", completed_date))
                    
                        if not self.is_encoding:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            finish_nvenc_task()
                            continue
                    
                        # Validation
                        try:
                            with console_redirect(nvenc_logger):
                                # Check if VirtualDub validation is disabled (read from task snapshot)
                                vdub_disabled = task.get('vdub_validation_disabled', False)
                                if vdub_disabled:
                                    # Skip VirtualDub validation
                                    is_valid = True
                                    print("  [INFO] VirtualDub2 validation disabled by user setting")
                                else:
                                    is_valid = validate_encoded_video_vlc(output_file, encoder='av1_nvenc', stop_event=video_stop_event, source_path=video_path)
                        except EncodingStopped:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            finish_nvenc_task()
                            continue
                    
                        # Re-check after validation
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        if tree_data:
                            current_values = tree_data.get('values', [])
                            tags_after_validation = tree_data.get('tags', ())
                        else:
                            current_values = []
                            tags_after_validation = ()
                        status_after_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        is_now_completed = (
                            is_status_completed(status_after_validation) or 
                            "completed" in tags_after_validation
                        )
                    
                        if is_now_completed:
                            is_valid = True
                            finish_nvenc_task()
                            continue
                    
                        if not self.is_encoding:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            finish_nvenc_task()
                            continue
                    
                        # Process validation result
                        if is_valid is None:
                            # [STOP] DEBUG PAUSE: NVENC Validation Error (None)
                            debug_pause("NVENC Validation Failed (Unexpected End)", "Fallback to SVT-AV1", str(video_path))

                            # SVT queue fallback
                            with console_redirect(nvenc_logger):
                                print(f"\n[WARN] NVENC 'unexpected end of stream' -> SVT-AV1 queue")
                        
                            if output_file.exists() and not DEBUG_MODE:
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    time.sleep(1)
                                    try:
                                        output_file.unlink()
                                    except (OSError, PermissionError):
                                        pass
                        
                            # LIST-BASED QUEUE: Fallback to SVT (unexpected_end)
                            denoised_master_to_pass = None
                            if denoised_master_path and denoised_master_path.exists():
                                keep_denoised_master = True
                                denoised_master_to_pass = denoised_master_path
                                with console_redirect(nvenc_logger):
                                    print(f"♻ Denoised master preserválva SVT-AV1 számára: {denoised_master_path.name}")

                            self.add_to_svt_queue(
                                video_path=video_path, item_id=item_id, task_type='encode', is_manual=False,
                                output_file=output_file, subtitle_files=subtitle_files,
                                invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                                max_encoded=max_encoded, resize_enabled=resize_enabled,
                                resize_height=resize_height, audio_compression_enabled=audio_compression_enabled,
                                audio_compression_method=audio_compression_method,
                                denoised_master_path=denoised_master_to_pass,
                                reason='unexpected_end'
                            )
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            finish_nvenc_task()
                            continue
                        elif not is_valid:
                            # [STOP] DEBUG PAUSE: NVENC Validation Failed
                            debug_pause("NVENC Validation Failed (Low VMAF/Bad File)", "Fallback to SVT-AV1", str(video_path))

                            # SVT queue fallback
                            with console_redirect(nvenc_logger):
                                print(f"\n[WARN] NVENC invalid -> SVT-AV1 queue")
                        
                            if output_file.exists() and not DEBUG_MODE:
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    time.sleep(1)
                                    try:
                                        output_file.unlink()
                                    except (OSError, PermissionError):
                                        pass
                        
                            # LIST-BASED QUEUE: Fallback to SVT (invalid)
                            denoised_master_to_pass = None
                            if denoised_master_path and denoised_master_path.exists():
                                keep_denoised_master = True
                                denoised_master_to_pass = denoised_master_path
                                with console_redirect(nvenc_logger):
                                    print(f"♻ Denoised master preserválva SVT-AV1 számára: {denoised_master_path.name}")

                            self.add_to_svt_queue(
                                video_path=video_path, item_id=item_id, task_type='encode', is_manual=False,
                                output_file=output_file, subtitle_files=subtitle_files,
                                invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                                max_encoded=max_encoded, resize_enabled=resize_enabled,
                                resize_height=resize_height, audio_compression_enabled=audio_compression_enabled,
                                audio_compression_method=audio_compression_method,
                                denoised_master_path=denoised_master_to_pass,
                                reason='invalid'
                            )
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            finish_nvenc_task()
                            continue
                
                    if is_valid:

                        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(original_video_path, output_file)
                        orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                        # Ha a zajszűrés részleges sikerrel járt, needs_check státusz
                        if task.get('denoise_partial'):
                            status_text = t('status_needs_check_nvenc')
                        else:
                            status_text = get_completed_status_for_encoder(used_encoder)
                        final_vmaf_display = format_localized_number(final_vmaf, decimals=2) if final_vmaf is not None else "-"
                        # Denoise params are already in file metadata, don't add to VMAF column
                        # if denoise_params_for_metadata:
                        #     final_vmaf_display += f" [{denoise_params_for_metadata}]"
                        nvenc_vmaf_ref = task.get('vmaf_reference_path', None)
                        nvenc_master_cleanup = task.get('denoised_master_path_for_cleanup', None)
                        # Preserve master for VMAF/PSNR if it will be needed
                        if denoised_master_path and denoised_master_path.exists():
                            vmaf_will_run = (manual_quality_check and manual_quality_check != 'none') or (hasattr(self, 'auto_vmaf_psnr') and self.auto_vmaf_psnr.get())
                            if vmaf_will_run:
                                keep_denoised_master = True
                        self.mark_encoding_completed(
                            item_id,
                            status_text,
                            str(int(final_cq)),
                            final_vmaf_display,
                            "-",
                            orig_size_display,
                            new_size_mb,
                            change_percent,
                            manual_quality_check=manual_quality_check,
                            manual_cq_value=manual_cq_value,
                            vmaf_reference_path=nvenc_vmaf_ref,
                            denoised_master_path=nvenc_master_cleanup
                        )
                        # Copy invalid subtitles from task + any additional ones found during encoding
                        all_invalid_subtitles = list(invalid_subtitles) + additional_skipped_subtitles
                        self._copy_invalid_subtitles(all_invalid_subtitles, output_file)

                    
                        with self.nvenc_worker_stats_lock:
                            self.nvenc_worker_stats['completed'] += 1
                    else:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.clear_encoding_times(item_id)
                        self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        # save_json references removed - DB save only in start_encoding and stop_encoding
                    
                        with self.nvenc_worker_stats_lock:
                            self.nvenc_worker_stats['failed'] += 1
                    
                        if output_file.exists() and not DEBUG_MODE:
                            try:
                                output_file.unlink()
                            except (OSError, PermissionError):
                                # Retry once after a short delay
                                time.sleep(1)
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    pass # Ignore if still fails, SVT worker might overwrite or fail explicitly
                    with console_redirect(nvenc_logger):
                        print(f"[OK] NVENC worker #{worker_index + 1} slot released\n")
                
                    finish_nvenc_task()
                finally:
                    # Clean up per-video stop event
                    try:
                        if hasattr(self, 'video_stop_events') and video_path in self.video_stop_events:
                            with self.video_stop_events_lock:
                                self.video_stop_events.pop(video_path, None)
                    except (KeyError, RuntimeError, AttributeError):
                        pass
                    
                    # STOP EVENT CHECK: Ha stop event miatt állt le, master megőrzése (finally safety net)
                    if video_stop_event.is_set() and not keep_denoised_master:
                        keep_denoised_master = True
                        if denoised_master_path and denoised_master_path.exists():
                            with console_redirect(nvenc_logger):
                                print(f"♻ Zajszűrt master megőrizve (stop event/finally - újrafelhasználásra): {denoised_master_path.name}")

                    # Safety net: clean denoised master if finish_nvenc_task() was not reached
                    if denoised_master_path and denoised_master_path.exists() and not keep_denoised_master:
                        from .core_preamble_and_imports import DEBUG_MODE
                        if not DEBUG_MODE:
                            try:
                                master_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                                denoised_master_path.unlink()
                                denoised_master_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                                with console_redirect(nvenc_logger):
                                    print(f"[DEL] Zajszűrt mesterdarab törölve (finally): {denoised_master_path.name} ({master_size_mb:.1f} MB felszabadítva)")
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: NVENC finally safety-net, zajszűrt master takarítás | fájl: {denoised_master_path.name} ({master_size_mb:.1f} MB)\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except (OSError, IOError, PermissionError):
                                pass

                    # PER-VIDEO STOP RACE CONDITION FIX:
                    # Ha per-video stop event miatt állt le (pl. manuális CQ override), megvárjuk,
                    # amíg a GUI szál hozzáadja a replacement manuális taskot a queue-hoz.
                    # A manual_override_ready_events[video_path] event-et a GUI szál set-eli
                    # a task queue-ba helyezése UTÁN. Timeout 5s (safety net, ha a GUI nem jelez).
                    if raw_video_stop_event.is_set() and not STOP_EVENT.is_set():
                        override_ready = self.manual_override_ready_events.pop(video_path, None)
                        if override_ready:
                            override_ready.wait(timeout=5.0)
                            with console_redirect(nvenc_logger):
                                print(f"♻ Override task kész - worker folytathat")

                # CRITICAL FIX: Re-check worker count at loop END (in case it changed during task processing)
                # During graceful stop, loop back to check for more VMAF tasks instead of breaking
                if graceful_stop:
                    continue

                configured_workers_end = self.get_configured_nvenc_workers()
                worker_should_stop_end = worker_index >= configured_workers_end

                if worker_should_stop or worker_should_stop_end:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[INFO] NVENC worker #{worker_index + 1}: Processed task, stopping due to reduced worker count (initial={worker_should_stop}, final={worker_should_stop_end}).\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                
                # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
                _, _, graceful_stop_check = self.get_encoding_state()
                if graceful_stop_check and not STOP_EVENT.is_set():
                    with console_redirect(nvenc_logger):
                        print(f"\n[STOP] Stop requested -> NVENC worker #{worker_index + 1} interrupted\n")

                with console_redirect(nvenc_logger):
                    print(f"\n{'#'*80}\n### NVENC WORKER #{worker_index + 1} COMPLETED ###\n{'#'*80}\n")

        except EncodingStopped:
            # EncodingStopped is expected when user stops encoding - not an error
            if nvenc_logger:
                with console_redirect(nvenc_logger):
                    print(f"\n{t('log_stop_request_nvenc')} #{worker_index + 1}\n")
        except Exception as e:
            if nvenc_logger:
                with console_redirect(nvenc_logger):
                    print(t('log_critical_error').format(worker=f'NVENC #{worker_index}', error=e))
                    import traceback
                    traceback.print_exc()
        finally:
            with self.nvenc_selection_lock:
                self.nvenc_active_videos.discard(worker_index)
            
            # LIST-BASED QUEUE: Mark task as complete
            # CRITICAL FIX: This must be in the OUTER finally block to ensure cleanup happens
            # even if an exception occurs before the inner finally block
            if current_video_path:
                self.complete_nvenc_task(current_video_path)
            
            # KRITIKUS: console_redirect stack teljes törlése a worker befejezésekor
            # Ez biztosítja, hogy ne maradjon beakadt logger a stack-ben
            try:
                from .core_paths_tools_logging import STDOUT_ROUTER
                STDOUT_ROUTER.clear_all_loggers()
            except (ImportError, AttributeError):
                # Import error - non-critical
                pass  # Csendes hiba - ne zavarjuk meg a folyamatot

    def process_manual_nvenc_tasks_worker(self):
        """Processes manual NVENC re-encoding tasks."""
        self.manual_nvenc_active = True
        try:
            while self.manual_nvenc_tasks:
                # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
                _, _, graceful_stop = self.get_encoding_state()
                if graceful_stop and not STOP_EVENT.is_set():
                    break
                if STOP_EVENT.is_set():
                    break

                task = self.manual_nvenc_tasks.pop(0)
                video_path = task['video_path']

                # Create per-video stop event for manual override support
                # Thread-safe access to video_stop_events
                with self.video_stop_events_lock:
                    if video_path not in self.video_stop_events:
                        self.video_stop_events[video_path] = threading.Event()
                    raw_video_stop_event = self.video_stop_events[video_path]
                # NOTE: Do NOT clear() here! If stop_encoding_for_video() already set it,
                # we must respect that and stop immediately.
                video_stop_event = CombinedStopEvent(raw_video_stop_event, STOP_EVENT)

                # Check if stop was already requested for this video (race condition handling)
                if video_stop_event.is_set():
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"  [WARN] Manual NVENC task for {video_path.name} was pre-stopped, skipping\n")
                        except Exception:
                            pass
                    continue  # Skip to next task

                # Add to processing set
                if hasattr(self, 'nvenc_processing_videos'):
                    self.nvenc_processing_videos.add(video_path)

                try:
                    # GUI FREEZE FIX: Pre-cleanup runs in worker thread (not GUI thread)
                    if task.get('needs_pre_cleanup'):
                        self._reencode_pre_cleanup(task, self.nvenc_logger)

                    output_file = task['output_file']
                    subtitle_files = task.get('subtitle_files') or []
                    invalid_subtitles = task.get('invalid_subtitles') or []
                    item_id = task['item_id']
                    orig_size_str = task['orig_size_str']
                    target_cq = task['target_cq']
                    vmaf_value = task.get('vmaf_value', None)
                    # CRITICAL FIX: Use current resize settings if not explicitly set in task
                    resize_enabled = task.get('resize_enabled', getattr(self, 'current_resize_enabled', False))
                    resize_height = task.get('resize_height', getattr(self, 'current_resize_height', 1080))
                    audio_compression_enabled = task.get('audio_compression_enabled', getattr(self, 'current_audio_compression_enabled', False))
                    audio_compression_method = task.get('audio_compression_method', getattr(self, 'current_audio_compression_method', 'fast'))
                    if audio_compression_method == t('audio_compression_fast'):
                        audio_compression_method = 'fast'
                    elif audio_compression_method == t('audio_compression_dialogue'):
                        audio_compression_method = 'dialogue'
                    
                    # CRITICAL FIX: Extract manual_quality_check from task
                    # This was MISSING - causing manual VMAF/PSNR to NOT trigger!
                    manual_quality_check = task.get('manual_quality_check', None)
                    manual_cq_value = task.get('manual_cq_value', task.get('target_cq'))
                    hard_rotate_degrees = get_hard_rotate_degrees_from_task(
                        task=task,
                        column_index=self.COLUMN_INDEX,
                        video_hard_rotate_degrees=getattr(self, 'video_hard_rotate_degrees', None),
                        video_hard_rotate_lock=getattr(self, 'video_hard_rotate_lock', None)
                    )
    
                    # Manual NVENC re-encoding - process directly (no queue)
                    # KRITIKUS: Állítsuk be a current_video_path-ot a logger-ben, hogy a naplóbejegyzések bekerüljenek a video_logs-ba
                    if hasattr(self.nvenc_logger, 'set_current_video_path'):
                        self.nvenc_logger.set_current_video_path(video_path)
                    
                    # KRITIKUS: console_redirect stack törlése a manuális újrakódolás előtt
                    # Ez biztosítja, hogy ne maradjon ott semmi az előző műveletekből
                    try:
                        from .core_paths_tools_logging import STDOUT_ROUTER
                        STDOUT_ROUTER.clear_all_loggers()
                    except (AttributeError, TypeError, ValueError):
                        # Queue or DB error - non-critical
                        pass  # Csendes hiba - ne zavarjuk meg a folyamatot
                    
                    with console_redirect(self.nvenc_logger):
                        print(f"\n{'*'*80}\nMANUAL NVENC RE-ENCODING: {video_path.name}\nCQ: {target_cq}\n{'*'*80}")
                        
                        # PROBE SOURCE VIDEO WITH VDUB2
                        vdub_disabled = task.get('vdub_validation_disabled', False)
                        if vdub_disabled:
                            print(f"  [INFO] VirtualDub2 source probe disabled by user setting")
                            probe_success = True
                        else:
                            print(f"[SCAN] Probing source video with VirtualDub2...")
                            probe_success = check_source_video_with_vdub2(video_path, stop_event=video_stop_event)

                    if not probe_success:
                        with console_redirect(self.nvenc_logger):
                            print(f"[WARN] Source video probe failed! Fallback to copy.")
                        
                        # Fallback copy
                        copy_dest = get_copy_filename(video_path, self.source_path, self.dest_path)
                        copy_success = copy_video_fallback(video_path, copy_dest, subtitle_files, logger=self.nvenc_logger, invalid_subtitles=invalid_subtitles)
                        
                        if copy_success:
                            # Update GUI
                            try:
                                orig_size_mb = video_path.stat().st_size / (1024**2)
                                new_size_mb = copy_dest.stat().st_size / (1024**2)
                                orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            except (OSError, IOError, AttributeError, ValueError):
                                # File stat or calculation error - use fallback
                                orig_size_str = "-"
                                new_size_str = "-"
                                new_size_mb = 0.0
                                
                            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            self.video_to_output[video_path] = copy_dest
                            
                            self.encoding_queue.put_nowait(("update", item_id, t('status_completed_copy'), "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                            self.encoding_queue.put_nowait(("progress_bar", 0))
                            
                            # Update DB (async)
                            def update_db_after_copy_manual():
                                try:
                                    self.update_single_video_in_db(
                                        video_path, item_id, t('status_completed_copy'), 
                                        "-", "-", "-", orig_size_str, 
                                        new_size_mb, 0.0, completed_date
                                    )
                                except (OSError, IOError, PermissionError):
                                    # File delete error - non-critical
                                    pass
                            threading.Thread(target=update_db_after_copy_manual, daemon=True).start()
                            
                        else:
                            # Copy failed
                            self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                            self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                            
                        continue
    
                    # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                    tree_data = self.request_tree_data_sync(item_id)
                    current_values = tree_data.get('values', []) if tree_data else []
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    vmaf_display = format_localized_number(vmaf_value, decimals=2) if vmaf_value is not None else "-"
                    self.encoding_queue.put_nowait(("update", item_id, f"NVENC encoding... (CQ {int(target_cq)})", str(int(target_cq)), vmaf_display, "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put_nowait(("tag", item_id, "encoding"))
                    self.encoding_start_times[item_id] = time.time()
    
                    def progress_callback(msg):
                        self.encoding_queue.put_nowait(("progress", item_id, msg))
                        self.update_estimated_end_time_from_progress(item_id, msg)
    
                    stop_encoding = False
                    success_nvenc = False
                    additional_skipped_subtitles = []
                    with console_redirect(self.nvenc_logger):
                        try:
                            success_nvenc, additional_skipped_subtitles = encode_single_attempt(
                                video_path,
                                output_file,
                                target_cq,
                                subtitle_files,
                                'av1_nvenc',
                                progress_callback,
                                stop_event=video_stop_event,
                                vmaf_value=vmaf_value,
                                resize_enabled=resize_enabled,
                                resize_height=resize_height,
                                audio_compression_enabled=audio_compression_enabled,
                                audio_compression_method=audio_compression_method,
                                pre_invalid_subtitles=invalid_subtitles,
                                hard_rotate_degrees=hard_rotate_degrees
                            )
                        except EncodingStopped:
                            stop_encoding = True
    
                    if stop_encoding:
                        break
    
                    if not self.is_encoding:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        if tree_data:
                            current_values = tree_data.get('values', [])
                            tags = tree_data.get('tags', ())
                        else:
                            current_values = []
                            tags = ()
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                        break
    
                    is_valid = False
                    used_encoder = "NVENC"
                    final_cq = target_cq
                    final_vmaf = vmaf_value if vmaf_value is not None else "-"
    
                    if success_nvenc:
                        # Manual re-encode: Always validate, ignore "Completed" status (which might be stale)
                        # We refresh current_values to get the latest completed_date if needed
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, "NVENC validating...", str(int(target_cq)), "-", "-", "100%", orig_size_str, "-", "-", completed_date))
    
                        if not self.is_encoding:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            if tree_data:
                                current_values = tree_data.get('values', [])
                                tags = tree_data.get('tags', ())
                            else:
                                current_values = []
                                tags = ()
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                                orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                                # save_json references removed - DB save only in start_encoding and stop_encoding
                            break
    
                        stop_validation = False
                        try:
                            with console_redirect(self.nvenc_logger):
                                # Check if VirtualDub validation is disabled (read from task snapshot)
                                vdub_disabled = task.get('vdub_validation_disabled', False)
                                if vdub_disabled:
                                    # Skip VirtualDub validation
                                    is_valid = True
                                    print("  [INFO] VirtualDub2 validation disabled by user setting")
                                else:
                                    is_valid = validate_encoded_video_vlc(output_file, encoder='av1_nvenc', stop_event=video_stop_event, source_path=video_path)
                        except EncodingStopped:
                            stop_validation = True
    
                        if stop_validation:
                            break
    
                        if is_valid is None:
                            with console_redirect(self.nvenc_logger):
                                print("\n[WARN] NVENC 'unexpected end of stream' -> placing in SVT-AV1 queue")
                            if output_file.exists() and not DEBUG_MODE:
                                try:
                                    output_file.unlink()
                                except (OSError, PermissionError):
                                    time.sleep(1)
                                    try:
                                        output_file.unlink()
                                    except (OSError, PermissionError):
                                        pass
                            # LIST-BASED QUEUE: Fallback to SVT (manual_reencode_cq_nvenc_failed)
                            self.add_to_svt_queue(
                                video_path=video_path, item_id=item_id, task_type='encode', is_manual=True,
                                output_file=output_file, subtitle_files=subtitle_files,
                                invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                                initial_min_vmaf=getattr(self, 'current_min_vmaf', 97.5), vmaf_step=getattr(self, 'current_vmaf_step', 0.25),
                                max_encoded=getattr(self, 'current_max_encoded_percent', 72.0),
                                resize_enabled=getattr(self, 'current_resize_enabled', False), resize_height=getattr(self, 'current_resize_height', 1080),
                                audio_compression_enabled=getattr(self, 'current_audio_compression_enabled', False),
                                audio_compression_method=getattr(self, 'current_audio_compression_method', 'fast'),
                                target_cq=target_cq, skip_crf_search=True,
                                reason='manual_reencode_cq_nvenc_failed'
                            )
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                            continue
                        elif is_valid:
                            orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                            vmaf_display = final_vmaf if isinstance(final_vmaf, str) else format_localized_number(final_vmaf, decimals=2)
                            orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                            # Ha a zajszűrés részleges sikerrel járt, needs_check státusz
                            if task.get('denoise_partial'):
                                manual_status = t('status_needs_check_nvenc')
                            else:
                                manual_status = get_completed_status_for_encoder(used_encoder)
                            manual_vmaf_ref = task.get('vmaf_reference_path', None)
                            manual_master_cleanup = task.get('denoised_master_path_for_cleanup', None)
                            self.mark_encoding_completed(
                                item_id,
                                manual_status,
                                str(int(final_cq)),
                                vmaf_display,
                                "-",
                                orig_size_display,
                                new_size_mb,
                                change_percent,
                                manual_quality_check=manual_quality_check,
                                manual_cq_value=manual_cq_value,
                                vmaf_reference_path=manual_vmaf_ref,
                                denoised_master_path=manual_master_cleanup
                            )
                            self._copy_invalid_subtitles(invalid_subtitles, output_file)

                        else:
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            current_values = tree_data.get('values', []) if tree_data else []
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.clear_encoding_times(item_id)
                            self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                            self.encoding_queue.put_nowait(("progress_bar", 0))
                            # save_json references removed - DB save only in start_encoding and stop_encoding
                    else:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        if item_id in self.estimated_end_dates:
                            del self.estimated_end_dates[item_id]
                        self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        # save_json references removed - DB save only in start_encoding and stop_encoding

                    # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
                    _, _, graceful_stop_inner = self.get_encoding_state()
                    if graceful_stop_inner:
                        break
                finally:
                    # Clean up per-video stop event
                    try:
                        if hasattr(self, 'video_stop_events') and video_path in self.video_stop_events:
                            with self.video_stop_events_lock:
                                self.video_stop_events.pop(video_path, None)
                    except (KeyError, RuntimeError, AttributeError):
                        pass
                    # Remove from processing set
                    if hasattr(self, 'nvenc_processing_videos'):
                        self.nvenc_processing_videos.discard(video_path)
                    # Clear current video path in logger
                    if hasattr(self.nvenc_logger, 'set_current_video_path'):
                        self.nvenc_logger.set_current_video_path(None)
                    # KRITIKUS: console_redirect stack teljes törlése, hogy ne maradjon ott semmi
                    # Ez biztosítja, hogy a VMAF/PSNR számításnál a logger helyesen működjön
                    try:
                        from .core_paths_tools_logging import STDOUT_ROUTER
                        STDOUT_ROUTER.clear_all_loggers()
                    except (AttributeError, TypeError, ValueError):
                        # Queue or DB error - non-critical
                        pass  # Csendes hiba - ne zavarjuk meg a folyamatot
        finally:
            self.manual_nvenc_active = False
            # KRITIKUS: console_redirect stack teljes törlése a worker befejezésekor is
            try:
                from .core_paths_tools_logging import STDOUT_ROUTER
                STDOUT_ROUTER.clear_all_loggers()
            except (ImportError, AttributeError, OSError, IOError):
                # Import or file error - non-critical
                pass  # Csendes hiba - ne zavarjuk meg a folyamatot
            # DEADLOCK FIX: Check if app is closing before calling root.after()
            if hasattr(self, 'root'):
                try:
                    if not is_app_closing() and self.root.winfo_exists():
                        self.root.after(0, self._on_manual_nvenc_worker_finished)
                except tk.TclError:
                    pass  # root already destroyed

    def _on_manual_nvenc_worker_finished(self):
        """Updates UI after manual NVENC worker stops."""
        self.manual_nvenc_active = False
        
        # Check if there are pending tasks that need to be processed
        if self.is_encoding and self.has_pending_tasks():
            # If encoding is running but encoding_worker is not alive, restart it
            if not getattr(self, 'encoding_worker_running', False):
                # Restart the encoding worker to process remaining videos
                if hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive():
                    # Worker is already running, no need to restart
                    pass
                else:
                    # Restart the encoding system to process remaining tasks
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(encoding_worker_running=True)
                    self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                    self.encoding_worker_thread.start()
                    self.root.after(100, self.check_encoding_queue)
            return  # Don't reset UI, encoding should continue
        
        self._reset_encoding_ui_if_idle()

    def _on_nvenc_toggle(self, *args):
        new_value = self.nvenc_enabled.get()
        old_value = getattr(self, '_last_nvenc_enabled_value', new_value)
        if not hasattr(self, '_last_nvenc_enabled_value'):
            self._last_nvenc_enabled_value = new_value
        
        # Ha kódolás fut és az érték változott
        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.nvenc_enabled, new_value, old_value):
                # Mégse: checkbox visszaállítása és return
                self._update_nvenc_checkbox_text()
                return
        
        # Változtatás elfogadva
        self._last_nvenc_enabled_value = new_value
        
        self._update_nvenc_checkbox_text()
        if not new_value:
            # Move NVENC queue items to SVT queue when NVENC is disabled
            self._move_nvenc_queue_to_svt()
            self.normalize_queue_statuses()
        else:
            # Move SVT queue items to NVENC queue when NVENC is enabled
            self._move_svt_queue_to_nvenc()
            self.normalize_queue_statuses()
        self.update_start_button_state()
    
    def _move_nvenc_queue_to_svt(self):
        """Move all items from NVENC queue to SVT queue when NVENC is disabled."""
        moved_count = 0
        
        # LIST-BASED QUEUE: Move tasks from NVENC list to SVT list
        with self.task_list_lock:
            # Get all pending NVENC tasks
            tasks_to_move = list(self.pending_nvenc_tasks)
            
            # Clear NVENC list
            self.pending_nvenc_tasks.clear()
            
            # Add each task to SVT queue with updated reason
            for task in tasks_to_move:
                if isinstance(task, dict):
                    # Update reason
                    task['reason'] = 'nvenc_disabled_fallback'
                    
                    # Extract necessary info
                    video_path = task.get('video_path')
                    item_id = task.get('item_id')
                    
                    # Check if already in SVT processing
                    if video_path and video_path not in self.svt_processing_videos:
                        # Add to SVT queue
                        success = self.add_to_svt_queue(
                            video_path=video_path,
                            item_id=item_id,
                            task_type=task.get('type', 'encode'),
                            is_manual=task.get('type') != 'encode',
                            output_file=task.get('output_file'),
                            subtitle_files=task.get('subtitle_files', []),
                            invalid_subtitles=task.get('invalid_subtitles', []),
                            orig_size_str=task.get('orig_size_str', '-'),
                            initial_min_vmaf=task.get('initial_min_vmaf', getattr(self, 'current_min_vmaf', 97.5)),
                            vmaf_step=task.get('vmaf_step', getattr(self, 'current_vmaf_step', 0.25)),
                            max_encoded=task.get('max_encoded', getattr(self, 'current_max_encoded_percent', 72.0)),
                            # CRITICAL FIX: Use current resize settings
                            resize_enabled=task.get('resize_enabled', getattr(self, 'current_resize_enabled', False)),
                            resize_height=task.get('resize_height', getattr(self, 'current_resize_height', 1080)),
                            audio_compression_enabled=task.get('audio_compression_enabled', getattr(self, 'current_audio_compression_enabled', False)),
                            audio_compression_method=task.get('audio_compression_method', getattr(self, 'current_audio_compression_method', 'fast')),
                            reason='nvenc_disabled_fallback'
                        )
                        
                        if success:
                            # Update status in GUI
                            orig_size_str = task.get('orig_size_str', '-')
                            if item_id:
                                self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            moved_count += 1
        
        # Also clear the processing videos set
        if hasattr(self, 'nvenc_processing_videos'):
            self.nvenc_processing_videos.clear()
        
        if moved_count > 0:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"NVENC disabled: moved {moved_count} tasks to SVT queue\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    # File or DB error - non-critical
                    pass

    def _move_svt_queue_to_nvenc(self):
        """Move all items from SVT queue to NVENC queue when NVENC is enabled."""
        moved_count = 0
        
        # LIST-BASED QUEUE: Move tasks from SVT list to NVENC list
        with self.task_list_lock:
            # Get all pending SVT tasks
            tasks_to_move = list(self.pending_svt_tasks)
            
            # Clear SVT list
            self.pending_svt_tasks.clear()
            
            # Add each task to NVENC queue with updated reason
            for task in tasks_to_move:
                if isinstance(task, dict):
                    # VMAF tasks must stay in SVT queue (NVENC worker cannot process them)
                    if task.get('type') == 'vmaf':
                        self.pending_svt_tasks.append(task)
                        continue

                    # Update reason to indicate NVENC priority
                    task['reason'] = 'nvenc_enabled_priority'
                    
                    # Extract necessary info for add_to_nvenc_queue
                    video_path = task.get('video_path')
                    item_id = task.get('item_id')
                    
                    # Check if already in NVENC processing
                    if video_path and video_path not in self.nvenc_processing_videos and video_path not in getattr(self, 'svt_processing_videos', set()):
                        # Add to NVENC queue (will handle duplicates internally)
                        success = self.add_to_nvenc_queue(
                            video_path=video_path,
                            item_id=item_id,
                            is_manual=task.get('type') != 'encode',  # VMAF tasks are manual-like
                            output_file=task.get('output_file'),
                            subtitle_files=task.get('subtitle_files', []),
                            invalid_subtitles=task.get('invalid_subtitles', []),
                            orig_size_str=task.get('orig_size_str', '-'),
                            initial_min_vmaf=task.get('initial_min_vmaf', getattr(self, 'current_min_vmaf', 97.5)),
                            vmaf_step=task.get('vmaf_step', getattr(self, 'current_vmaf_step', 0.25)),
                            max_encoded=task.get('max_encoded', getattr(self, 'current_max_encoded_percent', 72.0)),
                            # CRITICAL FIX: Use current resize settings
                            resize_enabled=task.get('resize_enabled', getattr(self, 'current_resize_enabled', False)),
                            resize_height=task.get('resize_height', getattr(self, 'current_resize_height', 1080)),
                            audio_compression_enabled=task.get('audio_compression_enabled', getattr(self, 'current_audio_compression_enabled', False)),
                            audio_compression_method=task.get('audio_compression_method', getattr(self, 'current_audio_compression_method', 'fast')),
                            reason='nvenc_enabled_priority'
                        )
                        
                        if success:
                            # Update status in GUI
                            orig_size_str = task.get('orig_size_str', '-')
                            if item_id:
                                self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            moved_count += 1
        
        if moved_count > 0:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"NVENC enabled: moved {moved_count} tasks to NVENC queue\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    # File or calculation error - non-critical
                    pass

    def _update_nvenc_checkbox_text(self):
        if not hasattr(self, 'nvenc_checkbutton'):
            return
        nvenc_text = t('nvenc_enabled')
        if getattr(self, 'detected_gpu_name', None):
            nvenc_text += f" ({self.detected_gpu_name})"
        self.nvenc_checkbutton.config(text=nvenc_text)

    def refresh_nvenc_console_tabs(self, desired_count):
        if not hasattr(self, 'nvenc_console_frames'):
            return
        try:
            desired = int(desired_count)
        except (ValueError, TypeError, tk.TclError):
            desired = 1
        desired = max(1, min(self.max_nvenc_consoles, desired))
        self.current_nvenc_console_count = desired
        
        # Check which worker threads are still alive
        active_workers = set()
        if hasattr(self, 'nvenc_worker_threads'):
            for idx, thread in enumerate(self.nvenc_worker_threads):
                if thread and thread.is_alive():
                    active_workers.add(idx)
        
        for idx, frame in enumerate(self.nvenc_console_frames):
            # Show tab if: worker is configured OR thread is still alive (graceful shutdown)
            should_show = idx < desired or idx in active_workers
            state = 'normal' if should_show else 'hidden'
            
            # VISUAL INDICATOR: Mark workers that are shutting down (graceful stop)
            # A worker is "shutting down" if: it's still alive BUT index >= desired (no longer needed)
            is_shutting_down = idx >= desired and idx in active_workers
            
            # Build tab title with shutdown indicator
            base_title = f"{t('nvenc_console')} {idx + 1}"
            if is_shutting_down:
                # Add visual indicator that this worker is completing its current task and will close
                tab_title = f"{base_title} 🔴 (bezárás...)"
            else:
                tab_title = base_title
            
            try:
                self.notebook.tab(frame, state=state)
                self.notebook.tab(frame, text=tab_title)
            except tk.TclError:
                continue


    def _refresh_nvenc_console_tab_titles(self):
        if hasattr(self, 'nvenc_console_frames'):
            for idx, frame in enumerate(self.nvenc_console_frames):
                try:
                    self.notebook.tab(frame, text=f"{t('nvenc_console')} {idx + 1}")
                except tk.TclError:
                    continue

    def get_configured_nvenc_workers(self):
        if threading.current_thread() is threading.main_thread():
            try:
                workers = int(self.nvenc_worker_count.get())
            except (ValueError, TypeError, tk.TclError):
                workers = getattr(self, 'current_nvenc_worker_count', 1)
        else:
            workers = getattr(self, 'current_nvenc_worker_count', 1)
        return max(1, min(self.max_nvenc_consoles if hasattr(self, 'max_nvenc_consoles') else 3, workers))

    # NOTE: update_nvenc_workers_label moved to SettingsControlMixin (takes precedence in MRO)
    # The implementation in SettingsControlMixin provides dynamic worker scaling
    # and automatic debounced saving. See gui_settings_control.py for details.
    def _update_nvenc_workers_label_moved_to_settings_control(self, value):
        """DEPRECATED: Moved to SettingsControlMixin."""
        pass

    def get_configured_svt_workers(self):
        """Returns the configured number of SVT-AV1 workers (1-3)."""
        if threading.current_thread() is threading.main_thread():
            try:
                workers = int(self.svt_worker_count.get())
            except (ValueError, TypeError, tk.TclError):
                workers = getattr(self, 'current_svt_worker_count', 1)
        else:
            workers = getattr(self, 'current_svt_worker_count', 1)
        return max(1, min(3, workers))

    def refresh_svt_console_tabs(self, desired_count):
        """Dynamically show/hide SVT console tabs based on worker count."""
        if not hasattr(self, 'svt_console_frames'):
            return
        try:
            desired = int(desired_count)
        except (ValueError, TypeError, tk.TclError):
            desired = 1
        desired = max(1, min(self.max_svt_consoles, desired))
        self.current_svt_console_count = desired
        
        # Check which worker threads are still alive
        active_workers = set()
        if hasattr(self, 'svt_worker_threads'):
            for idx, thread in enumerate(self.svt_worker_threads):
                if thread and thread.is_alive():
                    active_workers.add(idx)
        
        for idx, frame in enumerate(self.svt_console_frames):
            # Show tab if: worker is configured OR thread is still alive (graceful shutdown)
            should_show = idx < desired or idx in active_workers
            state = 'normal' if should_show else 'hidden'
            
            # VISUAL INDICATOR: Mark workers that are shutting down (graceful stop)
            # A worker is "shutting down" if: it's still alive BUT index >= desired (no longer needed)
            is_shutting_down = idx >= desired and idx in active_workers
            
            # Build tab title with shutdown indicator
            base_title = f"{t('svt_console')} {idx + 1}"
            if is_shutting_down:
                # Add visual indicator that this worker is completing its current task and will close
                tab_title = f"{base_title} 🔴 (bezárás...)"
            else:
                tab_title = base_title
            
            try:
                self.notebook.tab(frame, state=state)
                self.notebook.tab(frame, text=tab_title)
            except tk.TclError:
                continue



    def _start_delayed_nvenc_workers(self, target_count):
        """Starts delayed NVENC workers only if there is real pending NVENC work."""
        if hasattr(self, '_delayed_nvenc_start_timer'):
            self._delayed_nvenc_start_timer = None

        with self.task_list_lock:
            configured = self.get_configured_nvenc_workers()
            pending_tasks = len(getattr(self, 'pending_nvenc_tasks', []))
            if pending_tasks <= 0:
                return

            if not hasattr(self, 'nvenc_worker_threads'):
                self.nvenc_worker_threads = []
            self.nvenc_worker_threads = [t for t in self.nvenc_worker_threads if t and t.is_alive()]
            alive_workers = len(self.nvenc_worker_threads)

            if configured <= alive_workers:
                return

            new_workers_needed = min(configured - alive_workers, pending_tasks)
            # BUG FIX: Find actually used indices to avoid duplicates.
            used_indices = {getattr(t, 'worker_index', None) for t in self.nvenc_worker_threads}
            started = 0
            for candidate_idx in range(configured):
                if started >= new_workers_needed:
                    break
                if candidate_idx not in used_indices:
                    thread = threading.Thread(target=self.nvenc_worker, args=(candidate_idx,), daemon=True)
                    thread.worker_index = candidate_idx
                    thread.start()
                    self.nvenc_worker_threads.append(thread)
                    started += 1

        msg = f"➕ {new_workers_needed} új NVENC worker elindítva."
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"{msg}\n")
                LOG_WRITER.flush()
            except Exception:
                pass
        if hasattr(self, 'status_label'):
            self.status_label.config(text=msg)

    def _start_delayed_svt_workers(self, target_count):
        """Starts delayed SVT workers only if there is real pending SVT work."""
        if hasattr(self, '_delayed_svt_start_timer'):
            self._delayed_svt_start_timer = None

        with self.task_list_lock:
            configured = self.get_configured_svt_workers()
            pending_tasks = len(getattr(self, 'pending_svt_tasks', []))
            if pending_tasks <= 0:
                return

            if not hasattr(self, 'svt_worker_threads'):
                self.svt_worker_threads = []
            self.svt_worker_threads = [t for t in self.svt_worker_threads if t and t.is_alive()]
            alive_workers = len(self.svt_worker_threads)

            if configured <= alive_workers:
                return

            new_workers_needed = min(configured - alive_workers, pending_tasks)
            # BUG FIX: Find actually used indices to avoid duplicates.
            used_indices = {getattr(t, 'worker_index', None) for t in self.svt_worker_threads}
            started = 0
            for candidate_idx in range(configured):
                if started >= new_workers_needed:
                    break
                if candidate_idx not in used_indices:
                    thread = threading.Thread(target=self.svt_worker, args=(candidate_idx,), daemon=True)
                    thread.worker_index = candidate_idx
                    thread.start()
                    self.svt_worker_threads.append(thread)
                    started += 1

        msg = f"➕ {new_workers_needed} új SVT worker elindítva."
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"{msg}\n")
                LOG_WRITER.flush()
            except Exception:
                pass
        if hasattr(self, 'status_label'):
            self.status_label.config(text=msg)

    def _find_original_source_for_master(self, master_path):
        """
        Find the original source file for a denoised master.

        When a denoised master is loaded into the queue (e.g., video_denoised_master.mkv),
        this method searches for the original source file (e.g., video.mkv) to accurately
        calculate audio/subtitle overhead for size estimation.

        Args:
            master_path: Path to the denoised master file

        Returns:
            Path: Original source path if found, otherwise master_path as fallback
        """
        # Only process files with _denoised_master in name
        if '_denoised_master' not in master_path.name:
            return master_path

        # Reconstruct original filename stem (remove _denoised_master suffix)
        original_stem = master_path.stem
        if original_stem.endswith('_denoised_master'):
            original_stem = original_stem[:-len('_denoised_master')]

        # Method 1: Search in video_items (queue list) - most reliable
        if hasattr(self, 'video_items'):
            for video_path, item_id in self.video_items.items():
                # Match by stem (filename without extension) and ensure it's not the master itself
                if video_path.stem == original_stem and video_path != master_path:
                    if video_path.exists():
                        return video_path

        # Method 2: Search in file system with common video extensions
        # Try same directory first (in case both source and master are in target folder)
        for ext in ['.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.webm', '.flv']:
            potential_path = master_path.parent / f"{original_stem}{ext}"
            if potential_path.exists() and potential_path != master_path:
                return potential_path

        # Method 3: Search parent directory (source might be one level up)
        if master_path.parent.parent.exists():
            for ext in ['.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.webm', '.flv']:
                potential_path = master_path.parent.parent / f"{original_stem}{ext}"
                if potential_path.exists():
                    return potential_path

        # Not found - return master as fallback (will result in 0 audio overhead, but prevents crash)
        return master_path

    def _format_manual_status_suffix(self, cq, quality_check):
        """Format status suffix for manual encoding.

        Returns suffix like " (M CQ:14 VMAF)" or " (M CQ:14)".
        """
        suffix = f" (M CQ:{cq}"

        if quality_check == "vmaf":
            suffix += " VMAF"
        elif quality_check == "psnr":
            suffix += " PSNR"
        elif quality_check == "both":
            suffix += " VMAF PSNR"
        # 'none' esetén nincs minőség ellenőrzés

        suffix += ")"
        return suffix
