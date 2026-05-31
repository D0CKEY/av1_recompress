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
    write_denoise_level_file,
    read_denoise_params_file,
    write_denoise_params_file
)
import time



class SvtWorkerMixin:
    def svt_worker(self, worker_index=0):
        """Background worker for SVT-AV1 encoding tasks.
        
        Args:
            worker_index: Index of the worker thread (0-based).
        
        Processes videos from the SVT queue, managing encoding, validation,
        and VMAF checks.
        """
    
        # Alacsony CPU prioritás beállítása
        set_low_priority()
        
        debug_pause.gui_queue = self.encoding_queue
        
        # Select logger based on worker_index
        if hasattr(self, 'svt_loggers') and self.svt_loggers:
            logger_idx = worker_index % len(self.svt_loggers)
            svt_logger = self.svt_loggers[logger_idx]
            svt_logger.set_worker_index(worker_index)
        else:
            # Fallback to single logger if multi-console not initialized
            svt_logger = self.svt_logger
        
        final_task_taken = False  # CRITICAL: Prevents taking multiple "final" tasks
        video_path = None  # CRITICAL: Initialize to prevent UnboundLocalError in finally block
        task = None  # Ensure task is defined in outer scope
        worker_should_stop = False  # Initialize to prevent UnboundLocalError
        while True:
            # THREAD-SAFETY FIX: Read encoding state once per iteration
            _, _, graceful_stop = self.get_encoding_state()
            
            # CRITICAL FIX: If we already took a final task in previous iteration, EXIT NOW!
            # Exception: during graceful stop, continue to process remaining VMAF/PSNR tasks
            if final_task_taken and not graceful_stop:
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: Final task completed, exiting now.\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                break
            
            # Check for app closing flag
            if is_app_closing():
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: Application closing detected, stopping.\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                break
            
            if STOP_EVENT.is_set():
                # Különbséget teszünk a graceful stop timeout és az azonnali leállítás között
                if graceful_stop:
                    pass
                else:
                    # Valódi azonnali leállítás
                    with console_redirect(svt_logger):
                        print(f"\n{t('log_immediate_stop_svt')} #{worker_index + 1}\n")
                break
            
            if graceful_stop:
                # GRACEFUL STOP FIX: Process remaining VMAF/PSNR tasks before exiting.
                # This ensures post-encoding VMAF/PSNR calculations complete even during
                # graceful stop - the worker only exits after all queued VMAF tasks are done.
                task = self.get_next_svt_vmaf_task()
                if task is None:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: Graceful stop - no more VMAF tasks, exiting.\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                worker_should_stop = False
            else:
                # Normal operation: check worker count and get next task
                configured_workers = self.get_configured_svt_workers()
                worker_should_stop = worker_index >= configured_workers
                
                if worker_should_stop:
                    task = self.get_next_svt_task()
                    if task is None:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: Worker count reduced, no tasks left, stopping.\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        break
                    final_task_taken = True
                else:
                    task = self.get_next_svt_task()

                    if task is None:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: No more tasks, stopping.\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        break
            
            # Extract video path for completion tracking
            # CRITICAL: video_path is REQUIRED - use [] to fail-fast if missing
            video_path = task['video_path']
            
            try:
                self._process_svt_task(task, svt_logger, worker_index)
            except EncodingStopped:
                # EncodingStopped is expected when user stops encoding - not an error
                with console_redirect(svt_logger):
                    print(f"\n{t('log_stop_request_svt')}\n")
                try:
                    # Try to revert status if not already completed
                    if task and isinstance(task, dict) and 'item_id' in task:
                        item_id = task['item_id']
                        orig_size_str = task.get('orig_size_str', "-")
                        # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                        cached_values = task.get('cached_values', [])
                        cached_tags = task.get('cached_tags', ())
                        status = cached_values[self.COLUMN_INDEX['status']] if len(cached_values) > self.COLUMN_INDEX['status'] else ""
                        # Don't touch completed or needs_check items
                        if not is_status_completed(status) and "completed" not in cached_tags and not is_status_needs_check(status) and "needs_check" not in cached_tags:
                            completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                except (KeyError, IndexError, AttributeError, NameError):
                    # Cached values errors during stop are not critical
                    pass
                _, _, graceful_stop_check = self.get_encoding_state()
                if STOP_EVENT.is_set() or graceful_stop_check:
                    break
                # CRITICAL: Also check worker_should_stop - even after EncodingStopped,
                # if this worker should shut down, don't loop back for more tasks!
                if worker_should_stop:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: EncodingStopped received but worker should stop, exiting.\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                continue
            except Exception as e:
                with console_redirect(svt_logger):
                    print(t('log_critical_error').format(worker='SVT-AV1', error=e))
                    import traceback
                    traceback.print_exc()
                
                # Try to mark task as failed if we have item_id
                try:
                    if task and isinstance(task, dict) and 'item_id' in task:
                         item_id = task['item_id']
                         orig_size_str = task.get('orig_size_str', "-")
                         self.encoding_queue.put_nowait(("update", item_id, t('status_error_with_msg').format(msg=str(e)[:50]), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                         self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                except (tk.TclError, KeyError, IndexError, TypeError, AttributeError):
                    # Tree/GUI errors during error handling are not critical
                    pass
            finally:
                # Cleanup denoised master if _process_svt_task set it up but didn't clean
                if isinstance(task, dict):
                    dm_path = task.get('denoised_master_path_for_cleanup')
                    dm_delete = task.get('_should_delete_master', False)
                    if dm_delete and dm_path:
                        try:
                            if dm_path.exists():
                                dm_size_mb = dm_path.stat().st_size / (1024 * 1024)
                                dm_path.unlink()
                                level_file = dm_path.with_suffix('.denoise_level')
                                if level_file.exists():
                                    level_file.unlink()
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: zajszűrt master végleges takarítás (finally) | fájl: {dm_path.name} ({dm_size_mb:.1f} MB)\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            else:
                                level_file = dm_path.with_suffix('.denoise_level')
                                if level_file.exists():
                                    level_file.unlink()
                        except OSError:
                            pass
                # LIST-BASED QUEUE: Mark task as complete
                # This removes video_path from processing set, allowing it to be re-queued if needed
                if video_path:
                    self.complete_svt_task(video_path)
            
            # CRITICAL FIX: Re-check worker count at loop END (in case it changed during task processing)
            # During graceful stop, loop back to check for more VMAF tasks instead of breaking
            if graceful_stop:
                continue

            configured_workers_end = self.get_configured_svt_workers()
            worker_should_stop_end = worker_index >= configured_workers_end

            # CRITICAL FIX: If this worker should stop (worker count reduced),
            # exit now after processing one final task. Do NOT loop back for more tasks!
            if worker_should_stop or worker_should_stop_end:
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[INFO] SVT worker #{worker_index + 1}: Processed task, stopping due to reduced worker count (initial={worker_should_stop}, final={worker_should_stop_end}).\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                break  # Exit worker loop

        # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
        _, _, graceful_stop_final = self.get_encoding_state()
        if graceful_stop_final and not STOP_EVENT.is_set():
            with console_redirect(svt_logger):
                print(f"\n{t('log_graceful_stop_svt')}\n")
    
        with console_redirect(svt_logger):
            print(f"\n{'#'*80}\n{t('log_svt_worker_finished')}\n{'#'*80}\n")
        
        # Ha nincs több feladat a queue-ban ÉS az encoding_worker nem fut, ellenőrizzük a pending feladatokat
        # (Mert ha az encoding_worker fut, akkor ő kezeli a folytatást)
        # LIST-BASED QUEUE: Check if there are more pending SVT tasks
        with self.task_list_lock:
            has_more_svt_tasks = len(self.pending_svt_tasks) > 0 if hasattr(self, 'pending_svt_tasks') else False
        encoding_worker_running = getattr(self, 'encoding_worker_running', False)
        
        if not has_more_svt_tasks and not encoding_worker_running and self.is_encoding:
            # KRITIKUS JAVÍTÁS: Ellenőrizzük, hogy van-e még pending feladat a tree-ben
            # Ha igen ÉS is_encoding True, újraindítjuk az encoding worker-t a GUI thread-en
            # Ez biztosítja, hogy az egyedi SVT újrakódolás után a queue-ban váró NVENC feladatok is feldolgozásra kerülnek

            def check_and_restart_encoding():
                """GUI thread-en fut - ellenőrzi és újraindítja az encoding worker-t ha szükséges."""
                acquired = self.encoding_state_lock.acquire(timeout=0.05)
                if not acquired:
                    if hasattr(self, 'root') and not is_app_closing():
                        try:
                            if self.root.winfo_exists():
                                self.root.after(500, check_and_restart_encoding)
                        except tk.TclError:
                            pass
                    return
                try:
                    if self.is_encoding and self.has_pending_tasks():
                        # Van még pending feladat, de az encoding_worker nem fut
                        if not getattr(self, 'encoding_worker_running', False):
                            # Ellenőrizzük, hogy az encoding_worker_thread tényleg nem fut-e
                            if not (hasattr(self, 'encoding_worker_thread') and
                                    self.encoding_worker_thread and
                                    self.encoding_worker_thread.is_alive()):
                                # Újraindítjuk az encoding worker-t
                                with console_redirect(svt_logger):
                                    print(f"[INFO] SVT worker kész, de van még pending feladat -> encoding worker újraindítása\n")
                                self.encoding_worker_running = True
                                if hasattr(self, '_refresh_encoding_worker_snapshot'):
                                    self._refresh_encoding_worker_snapshot()
                                self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                                self.encoding_worker_thread.start()
                                self.root.after(100, self.check_encoding_queue)
                                self.root.after(200, self._ensure_svt_workers_running)
                                return  # Ne küldjünk finished üzenetet, mert az encoding folytatódik
                            # A worker már fut, ezért az encoding folytatódik.
                            self.encoding_worker_running = True
                            return
                finally:
                    self.encoding_state_lock.release()

                # Nincs több pending feladat vagy is_encoding False
                # Számoljuk meg a befejezett videókat
                completed = 0
                failed = 0
                needs_check = 0
                for video_path, item_id in self.video_items.items():
                    try:
                        current_values = self.tree.item(item_id, 'values')
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        if is_status_completed(status) or "completed" in tags:
                            if is_status_needs_check(status) or "needs_check" in tags:
                                needs_check += 1
                            else:
                                completed += 1
                        elif "[ERROR]" in status or "failed" in tags or "Hiba" in status:
                            failed += 1
                    except (tk.TclError, KeyError, IndexError, AttributeError):
                        pass

                # Küldjük el a "finished" üzenetet
                self.encoding_queue.put_nowait(("finished", completed, failed, needs_check))

            # A GUI thread-en futtatjuk az ellenőrzést és a potenciális újraindítást
            if hasattr(self, 'root'):
                try:
                    if not is_app_closing() and self.root.winfo_exists():
                        self.root.after(0, check_and_restart_encoding)
                except tk.TclError:
                    pass  # root already destroyed



    def _reencode_pre_cleanup(self, task, svt_logger):
        """Pre-processing cleanup for re-encode tasks (runs in worker thread).

        Deletes old output files, copy files, temp files and subtitle copies
        at the destination. Also resolves subtitle files if not yet done.
        This runs in the SVT worker thread to avoid blocking the GUI thread
        on slow network shares.

        Args:
            task: Task dictionary (modified in-place with resolved subtitles).
            svt_logger: Logger instance for console output.
        """
        video_path = task['video_path']
        output_file = task['output_file']

        with console_redirect(svt_logger):
            print(f"[INFO] Pre-cleanup: régi fájlok törlése ({video_path.name})")

        # 1. Régi output fájl törlése (.av1.mkv)
        try:
            if output_file.exists():
                if hasattr(self, '_delete_output_file_with_probe_guard'):
                    self._delete_output_file_with_probe_guard(
                        output_file,
                        video_path=video_path,
                        item_id=task.get('item_id'),
                        log_context="svt_pre_cleanup"
                    )
                else:
                    output_file.unlink()
        except (OSError, IOError, PermissionError) as e:
            with console_redirect(svt_logger):
                print(f"[WARN] Output fájl törlése sikertelen: {e}")

        # 2. Régi _video_only.mkv temp fájl törlése
        try:
            video_only_temp = output_file.with_name(f"{output_file.stem}_video_only.mkv")
            if video_only_temp.exists():
                video_only_temp.unlink()
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: újrakódolás előkészítés, régi temp fájl | fájl: {video_only_temp.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
        except (OSError, IOError, PermissionError):
            pass

        # 3. Régi másolat törlése (eredeti kiterjesztés, pl. video.mp4)
        try:
            from .core_audio_video_ops import get_copy_filename
            copy_file = get_copy_filename(video_path, self.source_path, self.dest_path)
            if copy_file.exists() and copy_file != output_file:
                copy_file.unlink()
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: újrakódolás előkészítés, régi másolat | fájl: {copy_file.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
        except (OSError, IOError, PermissionError, AttributeError, TypeError, ValueError):
            pass

        # 4. Régi feliratmásolatok törlése a célmappában
        try:
            from .core_subtitles_and_metadata import find_subtitle_files
            if self.dest_path and self.source_path:
                try:
                    relative_path = video_path.relative_to(self.source_path)
                except ValueError:
                    relative_path = Path(video_path.name)
                dest_sub_dir = self.dest_path / relative_path.parent
                video_stem = video_path.stem

                subtitle_files_list = find_subtitle_files(video_path)
                for sub_path, lang_part in subtitle_files_list:
                    if lang_part:
                        dest_sub_name = f"{video_stem}.{lang_part}{sub_path.suffix}"
                    else:
                        dest_sub_name = f"{video_stem}{sub_path.suffix}"
                    dest_sub = dest_sub_dir / dest_sub_name
                    try:
                        if dest_sub.exists():
                            dest_sub.unlink()
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: újrakódolás előkészítés, régi felirat | fájl: {dest_sub.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                    except (OSError, IOError, PermissionError):
                        pass
        except (ImportError, AttributeError, OSError, IOError):
            pass

        # 5. Feliratok keresése és validálása (ha még nem történt meg)
        if task.get('subtitle_files') is None:
            try:
                from .core_subtitles_and_metadata import find_subtitle_files, split_valid_invalid_subtitles
                raw_subs = find_subtitle_files(video_path)
                valid, invalid = split_valid_invalid_subtitles(raw_subs)
                task['subtitle_files'] = valid
                task['invalid_subtitles'] = invalid
                if invalid and LOG_WRITER:
                    try:
                        for sub_path, _, reason in invalid:
                            LOG_WRITER.write(f"[WARN] Hibás felirat kihagyva: {sub_path.name} ({reason})\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            except Exception:
                task['subtitle_files'] = []
                task['invalid_subtitles'] = []

    def _process_svt_task(self, task, svt_logger, worker_index=0):
        """Processes a single SVT-AV1 task (encoding or VMAF calculation).

        Args:
            task: Task dictionary with 'type' field ('encode' or 'vmaf')
            svt_logger: Logger instance for console output
            worker_index: Worker thread index
        """
        # Determine task type (default: 'encode' for backward compatibility)
        task_type = task.get('type', 'encode')
        
        if task_type == 'vmaf':
            # VMAF/PSNR calculation task - delegate to separate handler
            return self._process_vmaf_task(task, svt_logger, worker_index)
        
        
        # ENCODING TASK (original logic)
        video_path = task['video_path']
        # Set current video path in logger for log storage
        if hasattr(svt_logger, 'set_current_video_path'):
            svt_logger.set_current_video_path(video_path)
        
        # NOTE: video_path is already added to svt_processing_videos by get_next_svt_task()
        # Don't add it again here to avoid race conditions!

        # Create per-video stop event for manual override support
        # This allows stop_encoding_for_video() to stop only THIS video's encoding
        # while global Stop button (STOP_EVENT) still stops ALL videos
        # Thread-safe access to video_stop_events
        with self.video_stop_events_lock:
            if video_path not in self.video_stop_events:
                self.video_stop_events[video_path] = threading.Event()
            raw_video_stop_event = self.video_stop_events[video_path]
        # NOTE: Do NOT clear() here! If stop_encoding_for_video() already set it,
        # we must respect that and stop immediately. Only clear for genuinely new tasks.
        # The check below handles pre-set stop events.
        # Combined event: responds to EITHER video-specific OR global stop
        video_stop_event = CombinedStopEvent(raw_video_stop_event, STOP_EVENT)

        # Check if stop was already requested for this video (race condition handling)
        if video_stop_event.is_set():
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"  [WARN] SVT task for {video_path.name} was pre-stopped, skipping\n")
                except Exception:
                    pass
            return

        denoised_master_path = None  # Track for cleanup scope
        should_delete_master = False # Initialize to prevent UnboundLocalError in finally block
        audio_size_mb = None  # Track audio size for predicted file size calculation (prevents UnboundLocalError)
        success_svt = False  # Initialize to prevent UnboundLocalError on all execution paths
        post_encode_quality_check_queued = False  # Set only when mark_encoding_completed queues VMAF/PSNR

        # GUI FREEZE FIX: Pre-cleanup runs in worker thread (not GUI thread)
        # This handles file deletion and subtitle resolution for re-encode tasks
        if task.get('needs_pre_cleanup'):
            self._reencode_pre_cleanup(task, svt_logger)

        try:
            output_file = task['output_file']
            subtitle_files = task.get('subtitle_files') or []
            invalid_subtitles = task.get('invalid_subtitles') or []
            item_id = task['item_id']
            orig_size_str = task['orig_size_str']
            initial_min_vmaf = task['initial_min_vmaf']
            vmaf_step = task['vmaf_step']
            max_encoded = task['max_encoded']
            # THREAD-SAFETY FIX: Read svt_preset and crf_increment from task dict
            # (pre-cached on GUI thread in add_to_svt_queue) instead of calling
            # self.svt_preset.get() / self.crf_increment.get() from this worker thread.
            #
            # PRESET LIVE-UPDATE: the preset is a global encoder setting, not per-task.
            # Prefer the live GUI-thread cache (self.current_svt_preset, a plain int kept
            # in sync by update_svt_preset_label) so a mid-run preset change is honoured by
            # encodes that START after the change. Fall back to the task's frozen value,
            # then to 2. Already-running encodes are unaffected (preset is read once here,
            # at task start).
            live_preset = getattr(self, 'current_svt_preset', None)
            svt_preset = live_preset if isinstance(live_preset, int) and live_preset > 0 else task.get('svt_preset', 2)
            crf_increment = task.get('crf_increment', 1)

            # Show the actual preset used for this encode in the tree's Preset column and
            # persist it (so it survives restart without re-probing the output file).
            if hasattr(self, '_apply_preset_to_row'):
                self._apply_preset_to_row(item_id, 'svt-av1', svt_preset)

            # CRITICAL FIX: Use current resize settings if not explicitly set in task
            # This allows users to change resize settings while encoding is running
            resize_enabled = task.get('resize_enabled', self.resize_enabled.get())
            resize_height = task.get('resize_height', self.resize_height.get())
            audio_compression_enabled = task.get('audio_compression_enabled', self.audio_compression_enabled.get())
            audio_compression_method = task.get('audio_compression_method', self.audio_compression_method.get())
            # Ha a combobox értéke fordított szöveg, konvertáljuk
            if audio_compression_method == t('audio_compression_fast'):
                audio_compression_method = 'fast'
            elif audio_compression_method == t('audio_compression_dialogue'):
                audio_compression_method = 'dialogue'
            reason = task['reason']
            
            # Manuális quality check paraméter kiolvasása (ha van)
            manual_quality_check = task.get('manual_quality_check', None)
            manual_cq_value = task.get('manual_cq_value', task.get('target_cq'))
            force_max_cq_post_vmaf = False
            queue_status_text = self._build_manual_queue_status_text(
                t('status_svt_queue'),
                manual_cq_value,
                manual_quality_check
            ) if hasattr(self, '_build_manual_queue_status_text') else t('status_svt_queue')
            
            if STOP_EVENT.is_set():
                # Thread-safe státusz visszaállítás kérése a főszáltól
                self.encoding_queue.put_nowait(("revert_status_if_not_done", item_id, queue_status_text, orig_size_str))
                
                with console_redirect(svt_logger):
                    print(f"\n{t('log_stop_request_svt')}\n")
                return
            
            # Ellenőrizzük, hogy a forrás videó létezik-e
            if not video_path.exists():
                with console_redirect(svt_logger):
                    print(t('log_source_not_found').format(path=video_path))
                # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                cached_values = task.get('cached_values', [])
                completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put_nowait(("update", item_id, t('status_source_missing'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                return
            
            with console_redirect(svt_logger):
                print(f"\n{'*'*80}")
                print(t('log_svt_processing').format(filename=video_path.name))
                print(t('log_full_path').format(path=video_path.absolute()))
                print(t('log_reason').format(reason=reason))
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
                with console_redirect(svt_logger):
                    print(t('log_probe_failed'))
                
                # Fallback copy
                copy_dest = get_copy_filename(video_path, self.source_path, self.dest_path)
                copy_success = copy_video_fallback(video_path, copy_dest, subtitle_files, logger=svt_logger, invalid_subtitles=invalid_subtitles)
                
                if copy_success:
                     # Update GUI & DB
                     orig_size_mb = 0.0
                     new_size_mb = 0.0
                     try:
                        orig_size_mb = video_path.stat().st_size / (1024**2)
                        new_size_mb = copy_dest.stat().st_size / (1024**2)
                        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                     except (OSError, IOError, AttributeError, ValueError, ZeroDivisionError):
                        # File access or calculation errors - use defaults
                        orig_size_str = "-"
                        new_size_str = "-"
                        
                     completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                     self.video_to_output[video_path] = copy_dest
                     
                     self.encoding_queue.put_nowait(("update", item_id, t('status_completed_copy'), "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                     self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                     self.encoding_queue.put_nowait(("progress_bar", 0))
                     
                     # Update DB
                     def update_db_after_copy_svt():
                        try:
                            self.update_single_video_in_db(
                                video_path, item_id, t('status_completed_copy'), 
                                "-", "-", "-", orig_size_str, 
                                new_size_mb, 0.0, completed_date
                            )
                        except (OSError, IOError, sqlite3.Error, AttributeError):
                            # DB update errors are not critical, ignore them
                            pass
                     self._start_db_thread(update_db_after_copy_svt, name="SvtCopyCompletionDB", daemon=True)
                else:
                     # Failed
                     self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                     self.encoding_queue.put_nowait(("tag", item_id, "failed"))

                return
                

        
            # Párhuzamosítás kezelése:
            # Mivel már konfigurálható a párhuzamos SVT workerek száma, itt szándékosan KIHAGYJUK 
            # a CPU_WORKER_LOCK-ot, hogy lehetővé tegyük a beállított számú párhuzamos kódolást.
            # A globális lock csak akkor lenne szükséges, ha szigorúan sorosítani akarnánk minden CPU műveletet.
            if True:
                # Leállítás ellenőrzés
                if not self.is_encoding:
                    # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                    cached_values = task.get('cached_values', [])
                    cached_tags = task.get('cached_tags', ())
                    status = cached_values[self.COLUMN_INDEX['status']] if len(cached_values) > self.COLUMN_INDEX['status'] else ""
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if not is_status_completed(status) and "completed" not in cached_tags and not is_status_needs_check(status) and "needs_check" not in cached_tags:
                        completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, queue_status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        return

                # Státusz frissítés: SVT-AV1 queue-ban vár
                # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                cached_values = task.get('cached_values', [])
                completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put_nowait(("update", item_id, queue_status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "encoding_svt"))
                
                with console_redirect(svt_logger):
                    print(f"{t('log_svt_slot_acquired')}\n")
        
                def status_callback_svt(msg):
                # Add worker index to status messages from core modules if possible
                    prefix = f"SVT-AV1 #{worker_index + 1}"
                    if msg.startswith("SVT-AV1"):
                        msg = msg.replace("SVT-AV1", prefix, 1)
                    
                    self.encoding_queue.put_nowait(("status_only", item_id, msg))
    
                def progress_callback_svt(msg):
                # Also inject worker index into progress messages
                    prefix = f"SVT-AV1 #{worker_index + 1}"
                    if msg.startswith("SVT-AV1"):
                        msg = msg.replace("SVT-AV1", prefix, 1)

                    self.encoding_queue.put_nowait(("progress", item_id, msg))
                    # Becsült befejezési idő számítása a progress alapján (frame szám alapján számolódik)
                    self.update_estimated_end_time_from_progress(item_id, msg)
    
                # THREAD-SAFETY FIX: Use cached values from task dict instead of direct tree access
                cached_values = task.get('cached_values', [])
                completed_date = cached_values[self.COLUMN_INDEX['completed_date']] if len(cached_values) > self.COLUMN_INDEX['completed_date'] else ""
                # Kezdeti státusz a cél VMAF értékkel
                localized_vmaf = format_localized_number(initial_min_vmaf, decimals=2)
                
                # Use worker index prefix
                self.encoding_queue.put_nowait(("update", item_id, t('status_crf_search_vmaf').format(encoder=f"SVT-AV1 #{worker_index + 1}", vmaf=localized_vmaf), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                
                # Kezdési időpont tárolása
                self.encoding_start_times[item_id] = time.time()
                
                # ===== DENOISE MASTER CREATION =====
                # Check if denoise is enabled for this video
                denoised_master_path = None  # Track for cleanup later
                should_delete_master = False # Only delete if we created it
                crf_search_input = video_path  # Default: use original video
                original_video_path = video_path  # Store original for reference

                # CRITICAL: If video_path is a denoised master, find the actual original source
                # This is necessary to correctly calculate audio/subtitle overhead for size estimation
                if '_denoised_master' in video_path.name:
                    found_original = self._find_original_source_for_master(video_path)
                    if found_original != video_path:
                        original_video_path = found_original
                        with console_redirect(svt_logger):
                            print(f"📂 Zajszűrt master észlelve - eredeti forrás használata sallang mérethez:")
                            print(f"   Master: {video_path.name}")
                            print(f"   Eredeti forrás: {original_video_path.name}")
                    else:
                        # Original source not found - audio overhead will be 0
                        with console_redirect(svt_logger):
                            print(f"[WARN] Zajszűrt master észlelve, de eredeti forrás nem található:")
                            print(f"   Master: {video_path.name}")
                            print(f"   A hangsáv mérete 0-ként lesz kezelve a becslésben!")

                denoise_params_for_metadata = task.get('denoise_params', "") # Store applied filters for metadata
                
                # OPTIMIZATION: Cache original video metadata to avoid re-querying from large lossless master
                original_video_metadata = None
                
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
                
                # Cleanup: Remove video from denoise tracking dict if it was there
                try:
                    with self.video_denoise_lock:
                        if video_path in self.video_denoise_enabled:
                            del self.video_denoise_enabled[video_path]
                except (KeyError, AttributeError):
                    # Denoise dict cleanup error - non-critical
                    pass
                
                # Define output file globally for this iteration
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)

                if denoise_level > 0:
                    def _with_denoise_level_marker(params_text, level):
                        params_text = str(params_text or "").strip()
                        marker = f"DenoiseLevel:{level}"
                        if not params_text:
                            return marker
                        if re.search(r'\bdenoise(?:[_\s-]*level)?\s*[:=]', params_text, re.IGNORECASE):
                            return params_text
                        return f"{params_text} - {marker}"

                    denoise_params_for_metadata = _with_denoise_level_marker(
                        denoise_params_for_metadata,
                        denoise_level
                    )
                    # OPTIMIZATION: Query original video metadata ONCE before denoising
                    # This avoids slow ffprobe on large lossless master later
                    with console_redirect(svt_logger):
                        print(f"[STATS] Eredeti videó metaadatainak lekérdezése (cache)...")
                        try:
                            from .core_audio_video_ops import get_video_info, get_video_color_metadata
                            orig_duration, orig_fps = get_video_info(video_path)
                            orig_color_meta = get_video_color_metadata(video_path)
                            original_video_metadata = {
                                'duration_seconds': orig_duration,
                                'video_fps': orig_fps,
                                'color_metadata': orig_color_meta
                            }
                            print(f"   [OK] Metaadatok mentve (tartam: {orig_duration:.1f}s, FPS: {orig_fps:.2f})")
                        except (ValueError, OSError, RuntimeError) as e:
                            print(f"   [WARN] Metaadat lekérdezés hiba: {e}")
                            original_video_metadata = None
                    
                    # Check for reused master from NVENC fallback
                    reused_master = False
                    if 'denoised_master_path' in task:
                        existing_master = task.get('denoised_master_path')
                        if existing_master and existing_master.exists():
                             denoised_master_path = existing_master
                             reused_master = True
                             should_delete_master = True # Take ownership
                             with console_redirect(svt_logger):
                                 print(f"\n♻ Meglévő zajszűrt mesterdarab újrahasznosítása: {denoised_master_path.name}")
                                 print(f"   (NVENC fallback-ből származik, kíméli a CPU-t)")
                             crf_search_input = denoised_master_path
                             reused_params = read_denoise_params_file(denoised_master_path.with_suffix('.denoise_params'))
                             if reused_params:
                                 denoise_params_for_metadata = _with_denoise_level_marker(reused_params, denoise_level)

                    if not reused_master:
                        # Show worker index in denoising status (prefix format for consistency)
                        denoising_status = f"SVT-AV1 {t('status_denoising')}"
                        # Add manual encoding suffix if this is a manual task
                        manual_cq = task.get('target_cq')
                        if manual_quality_check is not None and manual_cq is not None:
                            denoising_status += self._format_manual_status_suffix(manual_cq, manual_quality_check)
                        status_callback_svt(denoising_status)
                        
                        with console_redirect(svt_logger):
                            print(f"\n🔇 Zajszűrés bekapcsolva - lossless mesterdarab létrehozása...")
                        
                        # Create master in dest folder
                        master_filename = f"{video_path.stem}_denoised_master.mkv"
                        denoised_master_path = output_file.parent / master_filename
                        should_delete_master = True
                        
                        # Ha már létezik a master fájl azonos denoise szinttel, újrahasználjuk
                        _master_reused = False
                        if denoised_master_path.exists() and denoised_master_path.stat().st_size > 0:
                            level_file = denoised_master_path.with_suffix('.denoise_level')
                            stored_level = read_denoise_level_file(level_file)
                            if stored_level == denoise_level:
                                with console_redirect(svt_logger):
                                    print(f"\n♻ Meglévő master fájl újrahasználata (azonos zajszűrési szint: {denoise_level})")
                                    print(f"   Fájl: {denoised_master_path.name}")
                                    print(f"   Méret: {denoised_master_path.stat().st_size / (1024**2):.1f} MB")
                                    print(f"   ⚡ A zajszűrési fázis kihagyva!\n")
                                crf_search_input = denoised_master_path
                                _master_reused = True
                                # VMAF reference és cleanup path beállítása újrahasznált master-hez
                                task['vmaf_reference_path'] = denoised_master_path
                                task['denoised_master_path_for_cleanup'] = denoised_master_path
                                reused_params = read_denoise_params_file(denoised_master_path.with_suffix('.denoise_params'))
                                if reused_params:
                                    denoise_params_for_metadata = _with_denoise_level_marker(reused_params, denoise_level)
                            else:
                                # Eltérő denoise szint - régi master törlése, újragenerálás szükséges
                                with console_redirect(svt_logger):
                                    if stored_level == -1:
                                        print(f"\n[WARN] Meglévő master fájl sidecar nélkül - újragenerálás szükséges")
                                    else:
                                        print(f"\n[WARN] Denoise szint változott ({stored_level} → {denoise_level}) - master újragenerálása")
                                try:
                                    denoised_master_path.unlink()
                                    level_file.unlink(missing_ok=True)
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: denoise szint változott, master újragenerálás | fájl: {denoised_master_path.name}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except OSError:
                                    pass
                        if not _master_reused:
                            # Get total frame count and duration for progress calculation
                            total_frames_for_denoise = None
                            total_duration_for_denoise = None
                            try:
                                total_frames_for_denoise = self.get_tree_item_meta(item_id, 'source_frame_count')
                                total_duration_for_denoise = self.get_tree_item_meta(item_id, 'source_duration_seconds')
                            except (KeyError, AttributeError, TypeError):
                                # Dictionary access error - use None values
                                pass
                            
                            # Ensure Duration and Frames columns are updated in the GUI
                            # Use update_info message (handled in gui_queue_management.py)
                            dur_str_update = format_seconds_hms(total_duration_for_denoise) if total_duration_for_denoise else None
                            frm_str_update = str(total_frames_for_denoise) if total_frames_for_denoise else None
                            if dur_str_update or frm_str_update:
                                raw_info = {}
                                if total_duration_for_denoise:
                                    raw_info['source_duration_seconds'] = total_duration_for_denoise
                                if total_frames_for_denoise:
                                    raw_info['source_frame_count'] = total_frames_for_denoise
                                if total_duration_for_denoise and total_frames_for_denoise:
                                    raw_info['source_fps'] = total_frames_for_denoise / total_duration_for_denoise
                                self.encoding_queue.put_nowait(("update_info", item_id, dur_str_update, frm_str_update, raw_info))
                            
                            # Denoise progress callback - updates Progress column
                            def denoise_progress_callback(current_frame, total_frames, fps, speed, current_time_str=None, total_duration_sec=None):
                                """Progress callback for denoising - shows time/total (fps, speed)."""
                                # Debug: show what we receive
                                debug_print(f"[DEBUG] Denoise progress: frame={current_frame}, total={total_frames}, time={current_time_str}, duration={total_duration_sec}")
                                
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
                                        except (ValueError, TypeError, AttributeError):
                                            # Time formatting failed - fallback to percentage
                                            pass
                                    
                                    # Use percentage if time calculation failed or not available
                                    if not progress_text:
                                        if total_frames and total_frames > 0:
                                            percent = (current_frame / total_frames) * 100
                                            progress_text = f"{format_localized_number(percent, decimals=1)}%"
                                        else:
                                            progress_text = f"{current_frame:,} {t('txt_frames')}"
                                    
                                    # Append FPS and Speed - REMOVED per user request
                                    # progress_text += f" ({int(fps)} fps, {format_localized_number(speed, decimals=2)}x)"
                                    
                                    # Update ONLY progress column
                                    try:
                                        self.encoding_queue.put_nowait(("progress", item_id, progress_text))
                                    except (AttributeError, TypeError, ValueError):
                                        # Queue error - non-critical
                                        pass
                                    
                                    current_denoised_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                                    self.encoding_queue.put_nowait(("update_new_size", item_id, f"{current_denoised_size_mb:.2f} MB"))
                                except (OSError, IOError, AttributeError):
                                    # File stat or queue error - non-critical
                                    pass
                            
                            # Check if Hybrid is available for SMDegrain
                            master_success = False
                            hybrid_path = getattr(self, 'hybrid_path', None)
                            hybrid_path_str = hybrid_path.get() if hybrid_path else ""
                            deband_enabled = bool(task.get('deband_enabled', getattr(self, 'current_deband_enabled', True)))
                            force_8bit_master = bool(task.get('force_8bit_denoised_master', getattr(self, 'current_force_8bit_denoised_master', False)))
                            
                            hybrid_components = get_hybrid_paths(hybrid_path_str) if hybrid_path_str else None
                            
                            if hybrid_path_str and hybrid_components:
                                # Try SMDegrain first (higher quality)
                                with console_redirect(svt_logger):
                                    print(f"[TARGET] SMDegrain használata (Hybrid/VapourSynth)...")
                                
                                master_success, denoise_info = create_smdegrain_master(
                                    input_path=video_path,
                                    output_path=denoised_master_path,
                                    hybrid_base_path=hybrid_path_str,
                                    use_nvenc=False,  # SVT-AV1 = CPU lossless (x264 CRF 0)
                                    logger=svt_logger,
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
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] SMDegrain részleges siker - a végeredmény ellenőrizendő lesz")
                                if master_success:
                                    denoise_params_for_metadata = denoise_info
                            elif hybrid_path_str:
                                with console_redirect(svt_logger):
                                    print(f"[WARN] Hybrid útvonal megadva ('{hybrid_path_str}'), de az összetevők hiányoznak!")
                                    try:
                                        base = Path(hybrid_path_str)
                                        if not base.exists():
                                            print(f"   [ERROR] A mappa nem létezik!")
                                        else:
                                            if not (base / '64bit' / 'vsscripts').exists():
                                                print(f"   [ERROR] Hiányzik: 64bit/vsscripts")
                                            # Check plugins
                                            plugin_paths = [
                                                base / '64bit' / 'vsPlugins',
                                                base / '64bit' / 'vsfilters' / 'Support',
                                                base / '64bit' / 'vsfilters',
                                                base / 'vsPlugins',
                                            ]
                                            plugins_found = False
                                            for p in plugin_paths:
                                                if p.exists():
                                                    plugins_found = True
                                                    break
                                            
                                            if not plugins_found:
                                                print(f"   [ERROR] Hiányzik: 64bit/vsPlugins (vagy alternatívái)")
                                            
                                            # Check vspipe specifically
                                            vspipe_paths = [
                                                base / '64bit' / 'vapoursynth' / 'vspipe.exe',
                                                base / '64bit' / 'VapourSynth' / 'vspipe.exe',
                                                Path(os.environ.get('LOCALAPPDATA', '')) / 'Programs' / 'VapourSynth' / 'core' / 'vspipe.exe',
                                                Path(os.environ.get('PROGRAMFILES', '')) / 'VapourSynth' / 'core' / 'vspipe.exe',
                                            ]
                                            vspipe_found = False
                                            for p in vspipe_paths:
                                                if p.exists():
                                                    vspipe_found = True
                                                    print(f"   [OK] vspipe.exe megtalálva: {p}")
                                                    break
                                            if not vspipe_found:
                                                print(f"   [ERROR] vspipe.exe nem található a szokásos helyeken!")
                                    except (OSError, IOError, AttributeError, KeyError) as e:
                                        print(f"   Hiba az ellenőrzés közben: {e}")
                            
                            if not master_success:
                                # Fallback to vaguedenoiser
                                if hybrid_path_str:
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] SMDegrain sikertelen, visszaállás vaguedenoiser-re...")
                                
                                master_success, denoise_info = create_denoised_lossless_master(
                                    input_path=video_path,
                                    output_path=denoised_master_path,
                                    use_nvenc=False,  # SVT-AV1 = CPU lossless (x264 CRF 0)
                                    logger=svt_logger,
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
                                with console_redirect(svt_logger):
                                    print(f"[OK] Mesterdarab létrehozva: {denoised_master_path.name}")
                                # Sidecar fájl írása a denoise szint tárolásához
                                write_denoise_level_file(denoised_master_path.with_suffix('.denoise_level'), denoise_level)
                                denoise_params_for_metadata = _with_denoise_level_marker(denoise_params_for_metadata, denoise_level)
                                write_denoise_params_file(denoised_master_path.with_suffix('.denoise_params'), denoise_params_for_metadata)
                                # Use master for CRF search and encoding
                                crf_search_input = denoised_master_path
                                
                                # CRITICAL: Store BOTH original reference AND denoised master path for VMAF
                                # Original reference: Used for VMAF comparison (the true source)
                                # Denoised master: Needed for potential cleanup AFTER VMAF finishes
                                if denoised_master_path and denoised_master_path.exists():
                                    # USER REQUEST: Use denoised master as reference if available!
                                    task['vmaf_reference_path'] = denoised_master_path
                                else:
                                    task['vmaf_reference_path'] = original_video_path
                                
                                task['denoised_master_path_for_cleanup'] = denoised_master_path
                                
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[OK] Denoised master tracking for VMAF:\n")
                                        LOG_WRITER.write(f"  Original reference: {original_video_path.name}\n")
                                        LOG_WRITER.write(f"  Denoised master: {denoised_master_path.name}\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            else:
                                with console_redirect(svt_logger):
                                    print(f"[WARN] Mesterdarab létrehozás sikertelen, eredeti fájl használata zajszűrés nélkül")
                                denoised_master_path = None  # No cleanup needed
                                should_delete_master = False
                
                # Ellenőrizzük, hogy skip_crf_search van-e (manuális újrakódolás)
                skip_crf_search = task.get('skip_crf_search', False)
                target_cq = task.get('target_cq')

                # DEBUG_MODE: Ha létezik a video-only fájl, ugorjuk át a CRF keresést is
                video_only_temp_file_check = output_file.with_name(f"{output_file.stem}_video_only.mkv")
                from .core_preamble_and_imports import DEBUG_MODE
                if DEBUG_MODE and video_only_temp_file_check.exists() and video_only_temp_file_check.stat().st_size > 0:
                    if not skip_crf_search:
                         with console_redirect(svt_logger):
                            print(f"\n[STOP] DEBUG: Meglévő video-only fájl észlelve: {video_only_temp_file_check.name}")
                            print(f"   ⚡ CRF keresés lépésének átugrása!")
                    skip_crf_search = True
                    # Dummy érték a logikához, ha nincs megadva (kódolás úgyis skipelve lesz)
                    if target_cq is None:
                        target_cq = 30
                
                if skip_crf_search and target_cq is not None:
                    # Manuális újrakódolás - skip CRF search, használjuk a target_cq-t
                    cq_value_svt = target_cq
                    vmaf_value_svt = task.get('vmaf_value', None)
                    predicted_size_mb = None  # No predicted size for manual re-encode
                    if vmaf_value_svt is None:
                        vmaf_value_svt = "-"  # VMAF nincs analízálva manuális újrakódolásnál
                    with console_redirect(svt_logger):
                        print(t('log_svt_manual_reencode').format(filename=video_path.name))
                        print(f"   {t('log_target_crf').format(crf=target_cq)}")
                else:
                    # Normál folyamat - CRF keresés
                    # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a CRF kereséshez és a kódoláshoz
                    # Ha denoise be van kapcsolva, a crf_search_input a mesterdarab
                    crf_search_source = crf_search_input  # Use master if denoise, else original
                    video_path_abs_svt = crf_search_source.absolute()
                    try:
                        if not skip_crf_search:
                            status_callback_svt(t('status_svt_crf_search'))
                            # Clear progress column from previous steps (e.g. denoise)
                            self.encoding_queue.put_nowait(("progress", item_id, "-"))
                            
                        with console_redirect(svt_logger):
                            print(t('log_svt_crf_search').format(filename=original_video_path.name))
                            if denoised_master_path:
                                print(f"🔇 CRF keresés zajszűrt mesterdarabon: {crf_search_source.name}")
                            else:
                                print(t('log_crf_file_check').format(path=video_path_abs_svt))
                            print(f"{t('log_reason').format(reason=reason)}")
                            
                            # Max encoded mode check (full video vs video track only)
                            # 'full' = user wants the FINAL file to be X% of original (correction needed)
                            # 'video' = user wants the VIDEO TRACK to be X% (no correction, ab-av1's default)
                            max_encoded_mode = task.get('max_encoded_mode', 'full')
                            
                            # Zajszűrés méret korrekció - KRITIKUS
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
                                        # SSD-k korában ez elhanyagolható idő (pár mp/perc), de kritikus a pontossághoz
                                        from .core_audio_video_ops import get_video_stream_size_bytes_exact
                                        
                                        with console_redirect(svt_logger):
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
                                            
                                            with console_redirect(svt_logger):
                                                print(f"   [OK] Videó stream méret: {exact_video_size/(1024**2):.1f} MB")
                                                print(f"   [OK] Korrigált egyéb adat (hang+overhead): {audio_size_mb:.1f} MB")
                                        else:
                                            # Fallback: ha a scan valamiért mégis falra futna, akkor használjuk a legjobb becslést
                                            # (Fájlméret - Becsült Audio)
                                            # De ha az audio is hibás, akkor a Fájlméret a fallback.
                                            est_video_size = orig_size - audio_size_bytes
                                            original_video_size = max(0, est_video_size)
                                            if original_video_size <= 0:
                                                with console_redirect(svt_logger):
                                                    print(f"   [WARN] Nem megbízható méretadatok, korrekció kihagyása")
                                                effective_max_encoded = max_encoded
                                                # Skip size correction entirely
                                                raise ValueError("Unreliable size data, skipping correction")
                                            
                                            with console_redirect(svt_logger):
                                                print(f"   [WARN] Pontos mérés sikertelen, becsült méret használata: {original_video_size/(1024**2):.1f} MB")
                                        
                                        original_video_size = max(0, original_video_size)
                                        
                                        if max_encoded_mode == 'video':
                                            # Videósáv mód: eredeti videó X%-a
                                            target_video_size = original_video_size * (max_encoded / 100.0)
                                            expected_final_size = target_video_size + audio_size_bytes
                                        else:
                                            # Teljes videó mód: végső fájl X%-a
                                            target_final_size = orig_size * (max_encoded / 100.0)
                                            target_video_size = target_final_size - audio_size_bytes
                                            target_video_size = max(0, target_video_size)
                                            expected_final_size = target_final_size
                                        
                                        # Calculate percentage relative to master file
                                        if master_size > 0:
                                            effective_max_encoded = (target_video_size / master_size) * 100.0
                                            effective_max_encoded = round(effective_max_encoded, 2)
                                            effective_max_encoded = max(0.01, effective_max_encoded)
                                        else:
                                            effective_max_encoded = max_encoded
                                        
                                        mode_label = t('max_encoded_mode_video') if max_encoded_mode == 'video' else t('max_encoded_mode_full')
                                        with console_redirect(svt_logger):
                                            print(f"⚖ Zajszűrés méret korrekció ({mode_label} mód):")
                                            print(f"   Eredeti forrásfájl: {orig_size/(1024**2):.1f} MB")
                                            print(f"   Eredeti videó (becsült): {original_video_size/(1024**2):.1f} MB")
                                            print(f"   Mesterfájl (FFV1): {master_size/(1024**2):.1f} MB")
                                            print(f"   Hang mérete (másolásra kerül): {audio_size_mb:.1f} MB")
                                            print(f"   Cél videó méret: {target_video_size/(1024**2):.1f} MB")
                                            print(f"   Várható végső fájlméret: {expected_final_size/(1024**2):.1f} MB ({(expected_final_size/orig_size)*100:.1f}%)")
                                            print(f"   -> ab-av1 max-encoded-percent: {effective_max_encoded:.2f}%")
                                except (OSError, IOError, AttributeError, KeyError, ValueError) as e:
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] Méret korrekció hiba: {e}")
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
                                        
                                        with console_redirect(svt_logger):
                                            print(f"⚖ Normál méret korrekció (Teljes videó mód):")
                                            print(f"   Eredeti forrásfájl: {orig_size/(1024**2):.1f} MB")
                                            print(f"   Eredeti videó (becsült): {original_video_size/(1024**2):.1f} MB")
                                            print(f"   Hang mérete (változatlan): {audio_size_mb:.1f} MB")
                                            print(f"   Cél végső méret ({max_encoded}%): {target_final_size/(1024**2):.1f} MB")
                                            print(f"   Cél videó méret: {target_video_size/(1024**2):.1f} MB")
                                            print(f"   -> ab-av1 max-encoded-percent: {effective_max_encoded:.2f}%")
                                except (OSError, IOError, AttributeError, KeyError, ValueError) as e:
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] Méret korrekció hiba: {e}")
                            else:
                                # Videósáv mód, normál átkódolás: nincs korrekció
                                with console_redirect(svt_logger):
                                    print(f"⚖ Videósáv mód: ab-av1 max-encoded-percent: {max_encoded}% (korrekció nélkül)")

                            # THREAD-SAFETY FIX: Use pre-cached preset from task dict
                            # instead of self.svt_preset.get() which is unsafe from worker threads.
                            crf_search_preset = svt_preset
                            # 8K+ resolution requires preset >= 5 (M5) for SVT-AV1
                            from .core_audio_video_ops import get_video_resolution
                            _w8k, _h8k = get_video_resolution(crf_search_source)
                            if _w8k and _h8k and (_w8k >= 7680 or _h8k >= 4320) and crf_search_preset < 5:
                                with console_redirect(svt_logger):
                                    print(f"[INFO] 8K videó ({_w8k}x{_h8k}), SVT-AV1 preset emelve: {crf_search_preset} -> 5 (M5 minimum)")
                                crf_search_preset = 5
                            # Check if denoise was enabled (denoised master exists)
                            is_denoised = denoised_master_path is not None and denoised_master_path.exists()
                            # THREAD-SAFETY FIX: Use pre-cached crf_increment from task dict
                            cq_result_svt = run_crf_search(crf_search_source, encoder='svt-av1', initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step, max_encoded_percent=effective_max_encoded, progress_callback=status_callback_svt, logger=svt_logger, stop_event=video_stop_event, svt_preset=crf_search_preset, crf_increment=crf_increment, denoise_enabled=is_denoised)
                            print(f"[OK] SVT-AV1 CRF keresés kész: {cq_result_svt}")
                    except FileNotFoundError as e:
                        # Ab-av1.exe nem található - végzetes hiba
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
                            logger=svt_logger,
                            console_redirect=console_redirect,
                            lang='hu'
                        )
                        return
                    except Exception as e:
                        # Újrakódolás (manuális vagy automata) esetén: ha bármilyen hiba van az ab-av1 művelet során, másoljuk át a fájlt
                        is_reencode = task.get('reason', '').startswith('manual_reencode') or task.get('reason', '').startswith('auto_reencode')
                        
                        # Check if it's NoSuitableCRFFound from core module
                        is_no_suitable_crf = isinstance(e, NoSuitableCRFFound) or (hasattr(e, '__class__') and e.__class__.__name__ == 'NoSuitableCRFFound')
                        
                        if is_reencode:
                            # Újrakódolás: ha bármilyen hiba van (NoSuitableCRFFound vagy más), másoljuk át a fájlt
                            reencode_type = (t('task_type_manual') if task.get('reason', '').startswith('manual_reencode') else t('task_type_auto')).capitalize()
                            error_type = "nem talált megfelelő értéket" if is_no_suitable_crf else f"hiba történt: {str(e)}"
                            
                            with console_redirect(svt_logger):
                                if is_no_suitable_crf:
                                    print(f"\n[WARN] CRF keresés nem talált megfelelő értéket (VMAF >= 85.0 ÉS fájl <= 75%)")
                                else:
                                    print(f"\n[WARN] CRF keresés során hiba történt: {str(e)}")
                                print(f"   -> {reencode_type} újrakódolás: videó másolása változatlanul\n")
                            
                            # Másolás (overwrite=True, mert újrakódolás esetén törölni kell a meglévő fájlt)
                            copy_success = copy_video_and_subtitles(video_path, output_file, overwrite=True)
                            
                            if copy_success:
                                # Sikeres másolás - kész státusz
                                orig_size_mb = video_path.stat().st_size / (1024**2)
                                new_size_mb = output_file.stat().st_size / (1024**2)
                                orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                                completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                self.video_to_output[video_path] = output_file
                                
                                # Becsült befejezési idő törlése
                                if item_id in self.estimated_end_dates:
                                    del self.estimated_end_dates[item_id]
                                
                                self.encoding_queue.put_nowait(("update", item_id, t('status_completed_copy'), "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                                self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                                self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                
                                # Adatbázis frissítése másolás befejezése után
                                if video_path:
                                    def update_db_after_copy():
                                        try:
                                            self.update_single_video_in_db(
                                                video_path, item_id, t('status_completed_copy'), 
                                                "-", "-", "-", orig_size_str, 
                                                new_size_mb, 0.0, completed_date
                                            )
                                            try:
                                                # Fallback test: Read temp file size
                                                if output_file.exists():
                                                    temp_size_mb = output_file.stat().st_size / (1024 * 1024)
                                                    self.encoding_queue.put_nowait(("update_new_size", item_id, f"{temp_size_mb:.2f} MB"))
                                            except (OSError, IOError, AttributeError):
                                                # File stat or queue error - non-critical
                                                pass
                                        except (OSError, IOError, AttributeError, KeyError) as e:
                                            if LOG_WRITER:
                                                try:
                                                    LOG_WRITER.write(f"[WARN] [copy] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                    LOG_WRITER.flush()
                                                except (OSError, IOError, AttributeError):
                                                    pass
                                    
                                    self._start_db_thread(update_db_after_copy, name="SvtCopyFallbackDB", daemon=True)
                                
                                with console_redirect(svt_logger):
                                    print(f"[OK] Videó sikeresen másolva: {output_file.name}\n")
                            else:
                                # Másolás sikertelen (pl. már létezik a célhelyen) - skip
                                completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # Becsült befejezési idő törlése
                                if item_id in self.estimated_end_dates:
                                    del self.estimated_end_dates[item_id]
                                
                                self.encoding_queue.put_nowait(("update", item_id, t('status_completed_exists'), "-", "-", "-", "100%", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put_nowait(("tag", item_id, "completed"))
                                self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                
                                # Adatbázis frissítése "már létezik" esetén
                                if video_path:
                                    # Próbáljuk meg meghatározni az output fájl méretét
                                    new_size_mb = None
                                    if output_file and output_file.exists():
                                        try:
                                            new_size_mb = output_file.stat().st_size / (1024**2)
                                        except (OSError, PermissionError):
                                            pass
                                    
                                    def update_db_after_exists():
                                        try:
                                            self.update_single_video_in_db(
                                                video_path, item_id, t('status_completed_exists'), 
                                                "-", "-", "-", orig_size_str, 
                                                new_size_mb, None, completed_date
                                            )
                                            try:
                                                # Read current output file size for "Új méret" column
                                                if output_file.exists():
                                                    current_size_mb_iter = output_file.stat().st_size / (1024 * 1024)
                                                    self.encoding_queue.put_nowait(("update_new_size", item_id, f"{current_size_mb_iter:.2f} MB"))
                                            except (OSError, IOError, AttributeError):
                                                # File stat error - non-critical
                                                pass
                                        except (OSError, IOError, AttributeError, KeyError) as e:
                                            if LOG_WRITER:
                                                try:
                                                    LOG_WRITER.write(f"[WARN] [exists] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                    LOG_WRITER.flush()
                                                except (OSError, IOError, AttributeError):
                                                    pass
                                    
                                    self._start_db_thread(update_db_after_exists, name="SvtExistsFallbackDB", daemon=True)
                                
                                with console_redirect(svt_logger):
                                    print(f"[WARN] Videó már létezik a célhelyen, átugrás\n")
                            
                            # SVT_QUEUE.task_done() removed - using list-based queue
                            return
                        else:
                            # Normál folyamat: csak NoSuitableCRFFound esetén másolunk
                            if is_no_suitable_crf:
                                # Normál folyamat: másolás
                                with console_redirect(svt_logger):
                                    print(f"\n[WARN] Nincs megfelelő CRF (VMAF >= 85.0 ÉS fájl <= 75%)")
                                    print(f"   -> Videó másolása feliratokkal együtt átkódolás nélkül\n")
                                
                                # Másolás (overwrite=True, mert újrakódolás esetén törölni kell a meglévő fájlt)
                                copy_success = copy_video_and_subtitles(video_path, output_file, overwrite=True)
                                
                                if copy_success:
                                    # Sikeres másolás - kész státusz
                                    orig_size_mb = video_path.stat().st_size / (1024**2)
                                    new_size_mb = output_file.stat().st_size / (1024**2)
                                    orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                    new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                                    completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    self.video_to_output[video_path] = output_file
                                    
                                    # Becsült befejezési idő törlése
                                    if item_id in self.estimated_end_dates:
                                        del self.estimated_end_dates[item_id]
                                    
                                    self.encoding_queue.put_nowait(("update", item_id, t('status_completed_copy'), "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                                    self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                                    self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                    
                                    # Adatbázis frissítése másolás befejezése után
                                    if video_path:
                                        def update_db_after_copy():
                                            try:
                                                self.update_single_video_in_db(
                                                    video_path, item_id, t('status_completed_copy'), 
                                                    "-", "-", "-", orig_size_str, 
                                                    new_size_mb, 0.0, completed_date
                                                )
                                            except (OSError, IOError, AttributeError, KeyError) as e:
                                                if LOG_WRITER:
                                                    try:
                                                        LOG_WRITER.write(f"[WARN] [copy] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                        LOG_WRITER.flush()
                                                    except (OSError, IOError, AttributeError):
                                                        pass
                                        
                                        self._start_db_thread(update_db_after_copy, name="SvtCopyFallbackDB", daemon=True)
                                    
                                    with console_redirect(svt_logger):
                                        print(f"[OK] Videó sikeresen másolva: {output_file.name}\n")
                                else:
                                    # Másolás sikertelen (pl. már létezik a célhelyen) - skip
                                    completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    
                                    # Becsült befejezési idő törlése
                                    if item_id in self.estimated_end_dates:
                                        del self.estimated_end_dates[item_id]
                                    
                                    self.encoding_queue.put_nowait(("update", item_id, t('status_completed_exists'), "-", "-", "-", "100%", orig_size_str, "-", "-", completed_date))
                                    self.encoding_queue.put_nowait(("tag", item_id, "completed"))
                                    self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                    
                                    # Adatbázis frissítése "már létezik" esetén
                                    if video_path:
                                        # Próbáljuk meg meghatározni az output fájl méretét
                                        new_size_mb = None
                                        if output_file and output_file.exists():
                                            try:
                                                new_size_mb = output_file.stat().st_size / (1024**2)
                                            except (OSError, PermissionError):
                                                pass
                                        
                                        def update_db_after_exists():
                                            try:
                                                self.update_single_video_in_db(
                                                    video_path, item_id, t('status_completed_exists'), 
                                                    "-", "-", "-", orig_size_str, 
                                                    new_size_mb, None, completed_date
                                                )
                                            except Exception as e:
                                                if LOG_WRITER:
                                                    try:
                                                        LOG_WRITER.write(f"[WARN] [exists] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                        LOG_WRITER.flush()
                                                    except (OSError, IOError, AttributeError):
                                                        # File stat error - non-critical
                                                        pass
                                        
                                        self._start_db_thread(update_db_after_exists, name="SvtExistsFallbackDB", daemon=True)
                                    
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] Videó már létezik a célhelyen, átugrás\n")
                                
                                # SVT_QUEUE.task_done() removed - using list-based queue
                                return
                            else:
                                # Más kivétel normál folyamatnál - dobjuk tovább
                                raise
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
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        with console_redirect(svt_logger):
                            print(f"\n{t('log_stop_request_svt')}\n")
                        # SVT_QUEUE.task_done() removed - using list-based queue
                        return
                    
                    # Leállítás ellenőrzés CRF keresés után
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
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        # SVT_QUEUE.task_done() removed - using list-based queue
                        return
                    
                    # Unpack 3-tuple: (crf_value, vmaf_value, predicted_size_mb)
                    cq_value_svt, vmaf_value_svt, predicted_size_mb = cq_result_svt
                    
                    # Apply max CQ limit if set
                    max_cq_limit = getattr(self, 'current_max_cq_limit', 0)
                    if max_cq_limit > 0 and cq_value_svt > max_cq_limit:
                        original_cq = cq_value_svt
                        cq_value_svt = float(max_cq_limit)
                        force_max_cq_post_vmaf = True
                        with console_redirect(svt_logger):
                            print(f"[INFO] CQ korlátozva: {format_localized_number(original_cq, decimals=1)} -> {max_cq_limit} (max CQ korlát)")
                        vmaf_value_svt = None
                        predicted_size_mb = None
                    
                # Display predicted size with ~ prefix if available
                # Display predicted size with ~ prefix if available
                # Add audio streams size for more accurate total file size estimate
                predicted_size_str = "-"
                if predicted_size_mb is not None and predicted_size_mb > 0:
                    # Calculate total estimated size
                    # Method priority:
                    # 1. Use audio_size_mb if defined in sizing logic blocks (zajszűrés/full mode)
                    # 2. Calculate accurately: Total File Size - Video Track Size = Audio + Subtitles + Container overhead
                    # 3. Fallback: FFprobe audio streams only
                    current_audio_and_extras_mb = 0.0
                    if audio_size_mb is not None:
                        # Use variable from sizing logic blocks (zajszűrés/full mode)
                        current_audio_and_extras_mb = audio_size_mb
                    else:
                        # Not defined - calculate from ORIGINAL source file
                        # Use original_video_path which correctly points to source (not denoised master)
                        try:
                            # More accurate method: Total file - Video track = Audio + extras
                            from .core_audio_video_ops import get_video_track_size_bytes

                            total_file_size = original_video_path.stat().st_size
                            video_track_size = get_video_track_size_bytes(original_video_path)

                            if video_track_size and video_track_size > 0:
                                # This includes audio + subtitles + container overhead + metadata
                                audio_and_extras_bytes = total_file_size - video_track_size
                                current_audio_and_extras_mb = max(0, audio_and_extras_bytes) / (1024 * 1024)
                            else:
                                # Fallback: FFprobe audio streams only (less accurate)
                                current_audio_and_extras_mb = get_audio_streams_total_size_mb(original_video_path)
                        except (OSError, IOError, ValueError, AttributeError):
                            # Audio size calculation failed - last resort fallback
                            try:
                                current_audio_and_extras_mb = get_audio_streams_total_size_mb(original_video_path)
                            except (OSError, IOError, ValueError, AttributeError):
                                current_audio_and_extras_mb = 0.0
                    
                    total_est_mb = predicted_size_mb + current_audio_and_extras_mb
                    
                    predicted_size_str = f"~{format_localized_number(total_est_mb, decimals=1)} MB"
                    with console_redirect(svt_logger):
                        print(f"[STATS] Becsült végső méret: {predicted_size_str} (Videó: {predicted_size_mb:.1f} MB + Hang/Egyéb: {current_audio_and_extras_mb:.1f} MB)")
                    
                # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                tree_data = self.request_tree_data_sync(item_id)
                current_values = tree_data.get('values', []) if tree_data else []
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=2)
                # Update GUI with predicted size
                
                # Use status callback for automatic worker index injection
                status_msg = t('status_svt_encoding')
                
                # Add manual encoding suffix if this is a manual task
                if manual_quality_check is not None:
                    manual_suffix = self._format_manual_status_suffix(cq_value_svt, manual_quality_check)
                    status_msg += manual_suffix
                
                # Use callback to add worker ID automatically and update status
                status_callback_svt(status_msg)
                
                # Update other columns (CQ, VMAF, size, etc.) via thread-safe queue
                # THREAD-SAFETY FIX: Use update_partial message instead of direct tree access
                self.encoding_queue.put_nowait(("update_partial", item_id, {
                    'cq': str(int(cq_value_svt)),
                    'vmaf': vmaf_display,
                    'psnr': "-",
                    'progress': "-",
                    'orig_size': orig_size_str,
                    'new_size': predicted_size_str,
                    'size_change': "-",
                    'completed_date': completed_date
                }))
                
                # Kódolás kezdési időpont frissítése (CRF keresés utáni pontos idő)
                self.encoding_start_times[item_id] = time.time()
                
                # SVT kódolás SVT konzolra irányítva
                # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a kódoláshoz, mint a CRF kereséshez
                # Determine encoding source: master if denoise enabled, else original
                if denoised_master_path and denoised_master_path.exists():
                     encoding_source = denoised_master_path
                else:
                     encoding_source = video_path
                video_path_abs_check_svt = encoding_source.absolute()
                
                if not skip_crf_search:
                    if video_path_abs_check_svt != video_path_abs_svt:
                        error_msg = f"VÉGZETES HIBA: A processzált fájl elérési útja nem egyezik a CRF keresésnél használttal!\n\nCRF keresés fájl: {video_path_abs_svt}\nKódolás fájl: {video_path_abs_check_svt}\n\nKülönbség oka lehet: Zajszűrés aktiválása/deaktiválása menet közben, vagy fájlrendszer változás.\n\nA program azonnal leáll."
                        with console_redirect(svt_logger):
                            print(f"\n{'='*80}")
                            print(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]")
                            print(f"{'='*80}")
                            print(error_msg)
                            print(f"{'='*80}\n")
                        # Log fájlba is írjuk
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"\n{'='*80}\n")
                                LOG_WRITER.write(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]\n")
                                LOG_WRITER.write(f"{'='*80}\n")
                                LOG_WRITER.write(f"{error_msg}\n")
                                LOG_WRITER.write(f"{'='*80}\n\n")
                                LOG_WRITER.flush()
                            except (OSError, IOError, AttributeError, ValueError):
                                # Log writer error - non-critical
                                pass
                        # Azonnali leállítás
                        STOP_EVENT.set()
                        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                        self.set_encoding_state(graceful_stop_requested=True)
                        # MessageBox hibaüzenet (GUI thread-ben)
                        # DEADLOCK FIX: Check if app is closing before calling root.after()
                        try:
                            if not is_app_closing() and self.root.winfo_exists():
                                self.root.after(0, lambda: messagebox.showerror(
                                    t('fatal_error_title'),
                                    error_msg
                                ))
                        except tk.TclError:
                            pass  # root already destroyed
                        raise ValueError(error_msg)
                additional_skipped_subtitles = []
                extracted_audio_tracks = []  # Track for cleanup
                video_only_temp_file = None  # Track for cleanup
                try:
                    with console_redirect(svt_logger):
                        
                        # ===== DENOISE WORKFLOW: SEPARATE ENCODE + MERGE =====
                        # To avoid interleave issues, we:
                        # 1. Encode video-only to temp file
                        # 2. Extract audio tracks from source to separate files
                        # 3. Merge video + audio + subtitles/metadata
                        
                        if denoised_master_path and denoised_master_path.exists():
                            print(f"\n{'='*80}")
                            print(f"🔇 ZAJSZŰRÉS WORKFLOW: Szétbontás-összerakás módszer")
                            print(f"{'='*80}")
                            
                            # Denoise flag for encode metadata
                            is_denoised = True
                            
                            # Determine CRF value
                            encode_crf = target_cq if (skip_crf_search and target_cq is not None) else cq_value_svt
                            
                            # Step 1: Encode video-only to temp file
                            video_only_temp_file = output_file.with_name(f"{output_file.stem}_video_only.mkv")
                            print(f"\n📽 1. LÉPÉS: Videó-only AV1 kódolás")
                            print(f"   Forrás: {encoding_source.name}")
                            print(f"   Temp fájl: {video_only_temp_file.name}")
                            
                            # DEBUG_MODE: Skip video encode if file already exists
                            from .core_preamble_and_imports import DEBUG_MODE
                            if DEBUG_MODE and video_only_temp_file.exists() and video_only_temp_file.stat().st_size > 0:
                                print(f"\n[STOP] DEBUG: Meglévő video-only fájl használata (gyors teszt mód)")
                                print(f"   Fájl: {video_only_temp_file.name}")
                                print(f"   Méret: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")
                                print(f"   ⚡ Video-only kódolás kihagyva!\n")
                                success_video = True
                            else:
                                # Size estimation callback setup (manual encoding only)
                                size_update_callback = None
                                if (skip_crf_search and target_cq is not None) or force_max_cq_post_vmaf:
                                    # This is manual encoding - setup size estimation
                                    # Use robust audio size calculation from core module (handles missing metadata)
                                    from .core_audio_video_ops import get_audio_streams_total_size_mb, get_video_info
                                    audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
                                    source_dur, _ = get_video_info(original_video_path)

                                    if source_dur and source_dur > 0 and audio_size_mb >= 0:
                                        # Create callback lambda
                                        def size_update_callback(current_video_mb, encoded_sec):
                                            try:
                                                if source_dur > 0 and encoded_sec > 0:
                                                    progress_ratio = min(encoded_sec / source_dur, 1.0)

                                                    # Estimated total video size
                                                    estimated_video_mb = current_video_mb / progress_ratio

                                                    # CRITICAL FIX: Audio is COPIED (not re-encoded), so use full size
                                                    # NOT proportional to encoding progress!
                                                    estimated_audio_mb = audio_size_mb

                                                    # Total
                                                    total_mb = estimated_video_mb + estimated_audio_mb

                                                    from .i18n import format_localized_number
                                                    size_str = f"~{format_localized_number(total_mb, decimals=1)} MB"

                                                    # GUI update
                                                    self.encoding_queue.put_nowait(("update_new_size", item_id, size_str))
                                            except (OSError, IOError, PermissionError):
                                                # File delete error - non-critical
                                                pass

                                # Temporarily disable audio for this encode
                                success_video, _ = encode_single_attempt(
                                    encoding_source,
                                    video_only_temp_file,
                                    encode_crf,
                                    [],  # No subtitle files for video-only
                                    'svt-av1',
                                    progress_callback_svt,
                                    stop_event=video_stop_event,
                                    vmaf_value=vmaf_value_svt,
                                    resize_enabled=resize_enabled,
                                    resize_height=resize_height,
                                    audio_compression_enabled=False,  # No audio compression for video-only
                                    audio_compression_method='fast',
                                    svt_preset=svt_preset,  # THREAD-SAFETY FIX: pre-cached from task dict
                                    logger=svt_logger,
                                    pre_invalid_subtitles=[],
                                    cached_video_metadata=original_video_metadata,
                                    original_input_path=None,  # No separate source - master is video-only already
                                    denoise_enabled=is_denoised,
                                    denoise_params=denoise_params_for_metadata,
                                    size_estimation_callback=size_update_callback,
                                    hard_rotate_degrees=hard_rotate_degrees
                                )
                            
                            if not success_video or not video_only_temp_file.exists():
                                print(f"[ERROR] Videó-only kódolás sikertelen!")
                                # Temp fájlok megőrzése hiba esetén
                                if video_only_temp_file and video_only_temp_file.exists():
                                    print(f"[STOP] HIBA: Video-only temp fájl megőrizve hibakereséshez: {video_only_temp_file.name}")
                                success_svt = False
                            else:
                                if not (DEBUG_MODE and video_only_temp_file.exists()):
                                    # Only print size if we just encoded (not skipped)
                                    print(f"[OK] Videó-only kódolás kész: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")
                                
                                # Step 2: Extract audio tracks from ORIGINAL source
                                print(f"\n🎵 2. LÉPÉS: Hangsávok kibontása a forrásból")
                                print(f"   Forrás: {original_video_path.name}")
                                
                                extracted_audio_tracks = extract_audio_tracks_with_metadata(
                                    original_video_path,
                                    output_file.parent,  # Same directory as output
                                    logger=svt_logger,
                                    stop_event=video_stop_event
                                )
                                
                                # Step 3: Merge video + audio + subtitles/metadata
                                print(f"\n[TOOL] 3. LÉPÉS: Összefűzés (videó + hang + feliratok)")
                                
                                success_merge = merge_video_audio_subtitles(
                                    video_only_temp_file,
                                    extracted_audio_tracks,
                                    original_video_path,  # For subtitles, chapters, metadata
                                    output_file,
                                    logger=svt_logger,
                                    stop_event=video_stop_event,
                                    external_subtitle_files=subtitle_files
                                )
                                
                                if success_merge:
                                    print(f"[OK] Összefűzés sikeres!")
                                    success_svt = True
                                    
                                    # CSAK sikeres merge után töröljük a temp fájlokat
                                    # Cleanup temp video file
                                    if video_only_temp_file and video_only_temp_file.exists():
                                        from .core_preamble_and_imports import DEBUG_MODE
                                        if DEBUG_MODE:
                                            print(f"[STOP] DEBUG: Video-only temp fájl megőrizve: {video_only_temp_file.name}")
                                        else:
                                            try:
                                                video_only_temp_file.unlink()
                                                print(f"[DEL] Video-only temp fájl törölve")
                                                if LOG_WRITER:
                                                    try:
                                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: sikeres összefűzés után, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                                        LOG_WRITER.flush()
                                                    except Exception:
                                                        pass
                                            except OSError as e:
                                                print(f"[WARN] Temp fájl törlés hiba: {e}")
                                    
                                    # Cleanup extracted audio files
                                    if extracted_audio_tracks:
                                        cleanup_extracted_audio_files(extracted_audio_tracks, logger=svt_logger)
                                else:
                                    print(f"[ERROR] Összefűzés sikertelen!")
                                    success_svt = False
                                    # HIBA ESETÉN NE TÖRÖLJÜK a temp fájlokat!
                                    print(f"\n[STOP] HIBA: Temp fájlok megőrizve újraindításhoz:")
                                    if video_only_temp_file and video_only_temp_file.exists():
                                        print(f"   - Video-only: {video_only_temp_file.name}")
                                    if extracted_audio_tracks:
                                        for track in extracted_audio_tracks:
                                            if track.get('path') and track['path'].exists():
                                                print(f"   - Audio: {track['path'].name}")
                                    print(f"   Kis javítással újrakezdhető a művelet!\n")
                            
                            print(f"{'='*80}\n")
                            
                        else:
                            # ===== NORMAL WORKFLOW (NO DENOISE) =====
                            # Using same separate encode + merge approach to avoid interleave issues
                            print(f"\n{'='*80}")
                            print(f"🎬 HAGYOMÁNYOS WORKFLOW: Szétbontás-összerakás módszer")
                            print(f"{'='*80}")
                            
                            # No denoise in this workflow
                            is_denoised = False
                            
                            # Determine CRF value
                            encode_crf = target_cq if (skip_crf_search and target_cq is not None) else cq_value_svt
                            
                            # Step 1: Encode video-only to temp file
                            video_only_temp_file = output_file.with_name(f"{output_file.stem}_video_only.mkv")
                            print(f"\n📽 1. LÉPÉS: Videó-only AV1 kódolás")
                            print(f"   Forrás: {encoding_source.name}")
                            print(f"   Temp fájl: {video_only_temp_file.name}")
                            
                            # DEBUG_MODE: Skip video encode if file already exists
                            from .core_preamble_and_imports import DEBUG_MODE
                            if DEBUG_MODE and video_only_temp_file.exists() and video_only_temp_file.stat().st_size > 0:
                                print(f"\n[STOP] DEBUG: Meglévő video-only fájl használata (gyors teszt mód)")
                                print(f"   Fájl: {video_only_temp_file.name}")
                                print(f"   Méret: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")
                                print(f"   ⚡ Video-only kódolás kihagyva!\n")
                                success_video = True
                            else:
                                # Size estimation callback setup (manual encoding only)
                                size_update_callback = None
                                if (skip_crf_search and target_cq is not None) or force_max_cq_post_vmaf:
                                    # This is manual encoding - setup size estimation
                                    # Use robust audio size calculation from core module (handles missing metadata)
                                    from .core_audio_video_ops import get_audio_streams_total_size_mb, get_video_info
                                    audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
                                    source_dur, _ = get_video_info(original_video_path)

                                    if source_dur and source_dur > 0 and audio_size_mb >= 0:
                                        # Create callback lambda
                                        def size_update_callback(current_video_mb, encoded_sec):
                                            try:
                                                if source_dur > 0 and encoded_sec > 0:
                                                    progress_ratio = min(encoded_sec / source_dur, 1.0)

                                                    # Estimated total video size
                                                    estimated_video_mb = current_video_mb / progress_ratio

                                                    # CRITICAL FIX: Audio is COPIED (not re-encoded), so use full size
                                                    # NOT proportional to encoding progress!
                                                    estimated_audio_mb = audio_size_mb

                                                    # Total
                                                    total_mb = estimated_video_mb + estimated_audio_mb

                                                    from .i18n import format_localized_number
                                                    size_str = f"~{format_localized_number(total_mb, decimals=1)} MB"

                                                    # GUI update
                                                    self.encoding_queue.put_nowait(("update_new_size", item_id, size_str))
                                            except (OSError, IOError, PermissionError):
                                                # File delete error - non-critical
                                                pass

                                # Use encode_single_attempt for video-only encoding
                                success_video, _ = encode_single_attempt(
                                    encoding_source,
                                    video_only_temp_file,
                                    encode_crf,
                                    [],  # No subtitle files for video-only
                                    'svt-av1',
                                    progress_callback_svt,
                                    stop_event=video_stop_event,
                                    vmaf_value=vmaf_value_svt,
                                    resize_enabled=resize_enabled,
                                    resize_height=resize_height,
                                    audio_compression_enabled=False,  # No audio for video-only
                                    audio_compression_method='fast',
                                    svt_preset=svt_preset,  # THREAD-SAFETY FIX: pre-cached from task dict
                                    logger=svt_logger,
                                    pre_invalid_subtitles=[],
                                    cached_video_metadata=original_video_metadata,
                                    original_input_path=None,  # Video-only encode
                                    denoise_enabled=is_denoised,
                                    denoise_params=denoise_params_for_metadata,
                                    include_audio=False,
                                    size_estimation_callback=size_update_callback,
                                    hard_rotate_degrees=hard_rotate_degrees
                                )
                            
                            if not success_video or not video_only_temp_file.exists():
                                print(f"[ERROR] Videó-only kódolás sikertelen!")
                                # Temp fájlok megőrzése hiba esetén
                                if video_only_temp_file and video_only_temp_file.exists():
                                    print(f"[STOP] HIBA: Video-only temp fájl megőrizve hibakereséshez: {video_only_temp_file.name}")
                                success_svt = False
                            else:
                                if not (DEBUG_MODE and video_only_temp_file.exists()):
                                    print(f"[OK] Videó-only kódolás kész: {video_only_temp_file.stat().st_size / (1024**2):.1f} MB")
                                
                                # Step 2: Extract audio tracks from source
                                print(f"\n🎵 2. LÉPÉS: Hangsávok kibontása a forrásból")
                                print(f"   Forrás: {original_video_path.name}")
                                
                                extracted_audio_tracks = extract_audio_tracks_with_metadata(
                                    original_video_path,
                                    output_file.parent,
                                    logger=svt_logger,
                                    stop_event=video_stop_event
                                )
                                
                                # Step 3: Merge video + audio + subtitles/metadata
                                print(f"\n[TOOL] 3. LÉPÉS: Összefűzés (videó + hang + feliratok)")
                                
                                success_merge = merge_video_audio_subtitles(
                                    video_only_temp_file,
                                    extracted_audio_tracks,
                                    original_video_path,
                                    output_file,
                                    logger=svt_logger,
                                    stop_event=video_stop_event,
                                    external_subtitle_files=subtitle_files
                                )
                                
                                if success_merge:
                                    print(f"[OK] Összefűzés sikeres!")
                                    success_svt = True
                                    
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
                                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: sikeres összefűzés után, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                                        LOG_WRITER.flush()
                                                    except Exception:
                                                        pass
                                            except OSError as e:
                                                print(f"[WARN] Temp fájl törlés hiba: {e}")
                                     
                                    # Cleanup extracted audio files
                                    if extracted_audio_tracks:
                                        cleanup_extracted_audio_files(extracted_audio_tracks, logger=svt_logger)
                                else:
                                    print(f"[ERROR] Összefűzés sikertelen!")
                                    success_svt = False
                                    # HIBA ESETÉN NE TÖRÖLJÜK a temp fájlokat!
                                    print(f"\n[STOP] HIBA: Temp fájlok megőrizve újraindításhoz:")
                                    if video_only_temp_file and video_only_temp_file.exists():
                                        print(f"   - Video-only: {video_only_temp_file.name}")
                                    if extracted_audio_tracks:
                                        for track in extracted_audio_tracks:
                                            if track.get('path') and track['path'].exists():
                                                print(f"   - Audio: {track['path'].name}")
                                    print(f"   Kis javítással újrakezdhető a művelet!\n")
                            
                            print(f"{'='*80}\n")

                except EncodingStopped:
                    # Cleanup temp files if denoise workflow was interrupted
                    if video_only_temp_file and video_only_temp_file.exists():
                        from .core_preamble_and_imports import DEBUG_MODE
                        if DEBUG_MODE:
                            with console_redirect(svt_logger):
                                print(f"[STOP] DEBUG: Video-only temp fájl megőrizve: {video_only_temp_file.name}")
                        else:
                            try:
                                video_only_temp_file.unlink()
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: EncodingStopped, temp fájl takarítás | fájl: {video_only_temp_file.name}\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except OSError:
                                pass
                    if extracted_audio_tracks:
                        cleanup_extracted_audio_files(extracted_audio_tracks, logger=svt_logger)
                    
                    # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                    tree_data = self.request_tree_data_sync(item_id)
                    if tree_data:
                        current_values = tree_data.get('values', [])
                        tags = tree_data.get('tags', ())
                    else:
                        current_values = []
                        tags = ()
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    with console_redirect(svt_logger):
                        print(f"\n[STOP] Leállítás kérés -> SVT-AV1 worker megszakítva\n")
                    return
                
                # FIX #5: Cleanup orphaned denoised master when encoding fails.
                # Normally cleanup happens in the VMAF worker, but if we never
                # reach VMAF (encoding error / EncodingStopped / etc.) the huge
                # lossless master file would leak on disk indefinitely.
                # J5: IO hiba (pl. lemez tele) esetén is MEGŐRIZZÜK a mastert,
                # mert drága előállítani és újrapróbálkozáskor újrahasználható.
                if not success_svt and denoised_master_path and denoised_master_path.exists():
                    if video_stop_event.is_set():
                        # Stop event miatt sikertelen - master megőrzése újrafelhasználásra
                        should_delete_master = False
                        task['denoised_master_path_for_cleanup'] = None
                        with console_redirect(svt_logger):
                            print(f"♻ Zajszűrt master megőrizve (stop/event miatti hiba): {denoised_master_path.name}")
                    else:
                        should_delete_master = True
                        task['denoised_master_path_for_cleanup'] = None
                        from .core_preamble_and_imports import DEBUG_MODE as _DEBUG_SVT
                        if not _DEBUG_SVT:
                            try:
                                _dm_size = denoised_master_path.stat().st_size / (1024 * 1024)
                                denoised_master_path.unlink()
                                denoised_master_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                                denoised_master_path.with_suffix('.denoise_params').unlink(missing_ok=True)
                                with console_redirect(svt_logger):
                                    print(f"[DEL] Zajszűrt mesterdarab törölve (kódolási hiba): {denoised_master_path.name} ({_dm_size:.1f} MB felszabadítva)")
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: kódolási hiba, zajszűrt master takarítás | fájl: {denoised_master_path.name} ({_dm_size:.1f} MB)\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except (OSError, IOError):
                                pass
                        else:
                            with console_redirect(svt_logger):
                                print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve (kódolási hiba): {denoised_master_path.name}")

                # Leállítás ellenőrzés kódolás után
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
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                        self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    return
                
                if success_svt:
                    # KRITIKUS: Ellenőrizzük, hogy a videó már "Kész" állapotban van-e (pl. VMAF/PSNR számítás után)
                    # Ha igen, ne indítsuk újra a validálást!
                    # FONTOS: Újrakódolásnál (manual_reencode vagy auto_reencode) MINDIG validálunk, figyelmen kívül hagyva a GUI (esetleg elavult) státuszát
                    is_reencode = task.get('reason', '').startswith('manual_reencode') or task.get('reason', '').startswith('auto_reencode')
                    
                    # FIX #7: Initialize current_values/tags before the is_reencode branch
                    # so they are always defined for the completed_date read at line ~1967
                    is_already_completed = False
                    current_values = []
                    tags_before_validation = ()
                    if not is_reencode:
                        try:
                            # FIGYELEM: GUI olvasása thread-ből nem biztonságos, de a jelenlegi architektúrában így működik
                            # A try-except blokk véd a versenyhelyzetek ellen
                            # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                            tree_data = self.request_tree_data_sync(item_id)
                            if tree_data:
                                current_values = tree_data.get('values', [])
                                tags_before_validation = tree_data.get('tags', ())
                            else:
                                current_values = []
                                tags_before_validation = ()
                            
                            status_before_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            is_already_completed = (
                                is_status_completed(status_before_validation) or 
                                "completed" in tags_before_validation
                            )
                        except (tk.TclError, KeyError, IndexError, AttributeError):
                            # Ha hiba van az olvasásnál, feltételezzük, hogy nem kész
                            is_already_completed = False
                    
                    if is_already_completed:
                        # A videó már kész (pl. VMAF/PSNR számítás után), ne indítsuk újra a validálást!
                        with console_redirect(svt_logger):
                            print(f"[INFO] A videó már kész státuszban van, validálás kihagyása.")
                        return
                    
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=2)
                    
                    # Use status callback for automatic worker index injection
                    status_callback_svt(t('status_svt_validation'))
                    
                    # Update other columns (CQ, VMAF, progress) via thread-safe queue
                    # THREAD-SAFETY FIX: Use update_partial message instead of direct tree access
                    self.encoding_queue.put_nowait(("update_partial", item_id, {
                        'cq': str(int(cq_value_svt)),
                        'vmaf': vmaf_display,
                        'psnr': "-",
                        'progress': "100%",
                        'orig_size': orig_size_str
                    }))
                    
                    # Leállítás ellenőrzés validálás előtt
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
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        return
                    
                    # Validálás SVT konzolra irányítva
                    try:
                        with console_redirect(svt_logger):
                            # Check if VirtualDub validation is disabled (read from task snapshot)
                            vdub_disabled = task.get('vdub_validation_disabled', False)
                            if vdub_disabled:
                                # Skip VirtualDub validation
                                is_valid = True
                                print("  [INFO] VirtualDub2 validation disabled by user setting")
                            else:
                                is_valid = validate_encoded_video_vlc(output_file, encoder='svt-av1', stop_event=video_stop_event, source_path=video_path)
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
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        with console_redirect(svt_logger):
                            print(f"\n{t('log_stop_request_svt')}\n")
                        return
                    
                    # KRITIKUS: Újraellenőrizzük a validálás után is, hogy a videó már "Kész" állapotban van-e
                    # (lehet, hogy közben VMAF/PSNR számítás befejeződött)
                    is_now_completed = False
                    if not is_reencode:
                        try:
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
                        except (OSError, IOError, PermissionError):
                            # File delete error - non-critical
                            is_now_completed = False
                    
                    if is_now_completed:
                        # A videó közben kész lett (pl. VMAF/PSNR számítás befejeződött), ne írjuk felül!
                        with console_redirect(svt_logger):
                            print(f"[INFO] A videó közben kész státuszba került, eredmény felülírásának mellőzése.")
                        return
                    
                    # Leállítás ellenőrzés validálás után
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
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(status) and "completed" not in tags and not is_status_needs_check(status) and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put_nowait(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        return
                    
                    if is_valid:

                        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(original_video_path, output_file)
                        vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=2)
                        # Denoise params are already in file metadata, don't add to VMAF column
                        # if denoise_params_for_metadata:
                        #     vmaf_display += f" [{denoise_params_for_metadata}]"
                        orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"

                        # Extract paths for VMAF (if denoised)
                        vmaf_reference = task.get('vmaf_reference_path', None)  # Original source
                        denoised_master_for_cleanup = task.get('denoised_master_path_for_cleanup', None)  # Master to delete after VMAF

                        # Ha a zajszűrés részleges sikerrel járt (pl. lsmas utolsó frame hiba),
                        # a videó "ellenőrizendő" státuszt kap "kész" helyett
                        if task.get('denoise_partial'):
                            final_status = t('status_needs_check_svt')
                        else:
                            final_status = t('status_completed_svt')

                        completion_manual_quality_check = (
                            'vmaf' if force_max_cq_post_vmaf and not manual_quality_check else manual_quality_check
                        )
                        completion_manual_cq_value = (
                            int(cq_value_svt) if force_max_cq_post_vmaf and manual_cq_value is None else manual_cq_value
                        )

                        post_encode_quality_check_queued = bool(self.mark_encoding_completed(
                            item_id,
                            final_status,
                            str(int(cq_value_svt)),
                            vmaf_display,
                            "-",
                            orig_size_display,
                            new_size_mb,
                            change_percent,
                            manual_quality_check=completion_manual_quality_check,
                            manual_cq_value=completion_manual_cq_value,
                            vmaf_reference_path=vmaf_reference,  # NEW: Original source for VMAF
                            denoised_master_path=denoised_master_for_cleanup  # NEW: Master to cleanup after VMAF
                        ))
                        # Copy invalid subtitles from task + any additional ones found during encoding
                        all_invalid_subtitles = list(invalid_subtitles) + additional_skipped_subtitles
                        self._copy_invalid_subtitles(all_invalid_subtitles, output_file)
                        
                        # Safety-net: denoised master törlése, ha VMAF NEM fog futni
                        if denoised_master_path and denoised_master_path.exists() and not DEBUG_MODE:
                            vmaf_will_run = post_encode_quality_check_queued
                            if not vmaf_will_run:
                                try:
                                    master_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                                    denoised_master_path.unlink()
                                    denoised_master_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                                    denoised_master_path.with_suffix('.denoise_params').unlink(missing_ok=True)
                                    with console_redirect(svt_logger):
                                        print(f"[DEL] Zajszűrt master törölve (SVT befejezve, VMAF nem fut): {denoised_master_path.name} ({master_size_mb:.1f} MB)")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: SVT kész, VMAF nem fut, zajszűrt master takarítás | fájl: {denoised_master_path.name} ({master_size_mb:.1f} MB)\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except (OSError, IOError, PermissionError):
                                    pass
                        
                        with console_redirect(svt_logger):
                            orig_size_str_log = format_localized_number(orig_size_mb, decimals=1)
                            new_size_str_log = format_localized_number(new_size_mb, decimals=1)
                            change_percent_str_log = format_localized_number(change_percent, decimals=2, show_sign=True)
                            print(f"\n[OK] SVT-AV1 kódolás sikeres: {original_video_path.name}")
                            print(f"  {orig_size_str_log}MB -> {new_size_str_log}MB ({change_percent_str_log}%)\n")

                    elif not is_valid and output_file.exists():
                        orig_size_mb = original_video_path.stat().st_size / (1024**2)
                        new_size_mb = output_file.stat().st_size / (1024**2)
                        change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100 if orig_size_mb > 0 else 0
                        
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=2)
                        # Denoise params are already in file metadata, don't add to VMAF column
                        # if denoise_params_for_metadata:
                        #     vmaf_display += f" [{denoise_params_for_metadata}]"
                        new_size_str_check = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        change_percent_str_check = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                        self.encoding_queue.put_nowait(("update", item_id, t('status_needs_check_svt'), str(int(cq_value_svt)), vmaf_display, "-", "100%", orig_size_str, new_size_str_check, change_percent_str_check, completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "needs_check"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                        
                        with console_redirect(svt_logger):
                            print(f"\n[WARN] SVT-AV1 validáció sikertelen, ellenőrizendő: {video_path.name}\n")
                    else:
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        # Becsült befejezési idő törlése
                        if item_id in self.estimated_end_dates:
                            del self.estimated_end_dates[item_id]
                        
                        self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))  # Az érték dinamikusan számolódik
                        
                        if output_file.exists() and not DEBUG_MODE:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: SVT-AV1 kódolás sikertelen | fájl: {output_file.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            output_file.unlink()
                else:
                    # ========================================================================
                    # COPY FALLBACK: Encoding sikertelen -> Változatlan másolás
                    # ========================================================================
                    # Újrakódolás (manuális vagy automata) esetén NE másoljon, hanem hibát jelezzen
                    is_reencode = task.get('reason', '').startswith('manual_reencode') or task.get('reason', '').startswith('auto_reencode')
                    if is_reencode:
                        # Újrakódolás: kódolás sikertelen - hiba
                        reencode_type = (t('task_type_manual') if task.get('reason', '').startswith('manual_reencode') else t('task_type_auto')).capitalize()
                        with console_redirect(svt_logger):
                            print(f"\n[ERROR] SVT-AV1 kódolás sikertelen: {video_path.name}")
                            print(f"   -> {reencode_type} újrakódolás sikertelen\n")
                        
                        # THREAD-SAFETY FIX: Request tree data via queue instead of direct access
                        tree_data = self.request_tree_data_sync(item_id)
                        current_values = tree_data.get('values', []) if tree_data else []
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        # Becsült befejezési idő törlése
                        if item_id in self.estimated_end_dates:
                            del self.estimated_end_dates[item_id]
                        
                        self.encoding_queue.put_nowait(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        
                        if output_file.exists() and not DEBUG_MODE:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF ellenőrzés sikertelen | fájl: {output_file.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            output_file.unlink()
                    
                    # Normál folyamat: másolás
                    with console_redirect(svt_logger):
                        print(f"\n[WARN] SVT-AV1 kódolás sikertelen: {video_path.name}")
                        print(f"   -> Változatlan másolás (eredeti kiterjesztés megtartva)...\n")
                    
                    # Generate copy destination with ORIGINAL extension
                    copy_dest = get_copy_filename(video_path, self.source_path, self.dest_path)
                    
                    # Perform copy with validated subtitles
                    copy_success = copy_video_fallback(
                        video_path,
                        copy_dest,
                        subtitle_files,  # Valid subtitles only
                        logger=svt_logger,
                        invalid_subtitles=invalid_subtitles
                    )
                    
                    if copy_success:
                        # Calculate sizes
                        try:
                            orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(
                                video_path, copy_dest
                            )
                            orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                            new_size_display = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            change_display = "0%"  # No change
                        except Exception as e:
                            with console_redirect(svt_logger):
                                print(f"[WARN] Méretszámítás hiba: {e}")
                            orig_size_mb = video_path.stat().st_size / (1024**2)
                            new_size_mb = orig_size_mb
                            change_percent = 0
                            orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                            new_size_display = orig_size_display
                            change_display = "0%"
                        
                        # Mark as completed (copied) in tree
                        completed_date_copy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.encoding_queue.put_nowait((
                            "update", 
                            item_id, 
                            t('status_completed_copy'),  # "[OK] Kész (másolva)"
                            "-",  # No CQ
                            "-",  # No VMAF
                            "-",  # No PSNR
                            "100%",  # Progress
                            orig_size_display,
                            new_size_display,
                            change_display,
                            completed_date_copy
                        ))
                        self.encoding_queue.put_nowait(("tag", item_id, "completed_copy"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        
                        # Update video_to_output mapping (CRITICAL!)
                        self.video_to_output[video_path] = copy_dest
                        
                        # Copy invalid subtitles too
                        try:
                            self._copy_invalid_subtitles(invalid_subtitles, copy_dest)
                        except Exception as e:
                            with console_redirect(svt_logger):
                                print(f"[WARN] Érvénytelen feliratok másolása hiba: {e}")
                        
                        # Database update in background thread
                        def update_db_after_copy():
                            try:
                                self.update_single_video_in_db(
                                    video_path, item_id, t('status_completed_copy'),
                                    "-", "-", "-",
                                    orig_size_display, new_size_mb, change_percent, completed_date_copy
                                )
                            except Exception as e:
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[WARN] [copy] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                        LOG_WRITER.flush()
                                    except (OSError, IOError, AttributeError, ValueError):
                                        # Log writer error - non-critical
                                        pass
                        
                        self._start_db_thread(update_db_after_copy, name="SvtCopyNoChangeDB", daemon=True)
                        
                        with console_redirect(svt_logger):
                            orig_mb_str = format_localized_number(orig_size_mb, decimals=1)
                            print(f"\n[OK] Videó változatlan másolva: {video_path.name}")
                            print(f"  {orig_mb_str} MB (nincs méretváltozás)\n")
                    else:
                        # Copy also failed - mark as failed
                        completed_date = ""
                        self.encoding_queue.put_nowait((
                            "update",
                            item_id,
                            t('status_error_copy_failed'),
                            "-", "-", "-", "-",
                            orig_size_str, "-", "-", completed_date
                        ))
                        self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                        self.encoding_queue.put_nowait(("progress_bar", 0))
                        
                        with console_redirect(svt_logger):
                            print(f"\n[ERROR] Másolás is sikertelen: {video_path.name}\n")
                
                with console_redirect(svt_logger):
                    print(f"[OK] SVT-AV1 slot felszabadítva\n")
        finally:
            # Clean up per-video stop event
            try:
                with self.video_stop_events_lock:
                    self.video_stop_events.pop(video_path, None)
            except (KeyError, RuntimeError, AttributeError):
                pass

            # STOP EVENT CHECK: Ha a worker stop event miatt állt le (pl. manuális CQ váltás),
            # a mastert MEGŐRIZZÜK újrafelhasználásra — a denoise szint nem változott,
            # az új feladat közvetlenül használhatja a meglévő mastert.
            if video_stop_event.is_set():
                should_delete_master = False
                if denoised_master_path and denoised_master_path.exists():
                    with console_redirect(svt_logger):
                        print(f"♻ Zajszűrt master megőrizve (stop event - újrafelhasználásra): {denoised_master_path.name}")

            # Clean up denoised master - BUT ONLY if no VMAF/PSNR check is pending!
            if denoised_master_path and denoised_master_path.exists() and should_delete_master:
                # CRITICAL CHECK: Don't delete if mark_encoding_completed already
                # queued a VMAF/PSNR task that uses this master as reference.
                will_need_for_vmaf = post_encode_quality_check_queued
                manual_qc = task.get('manual_quality_check')
                
                # Only delete if VMAF won't need it
                if not will_need_for_vmaf:
                    from .core_preamble_and_imports import DEBUG_MODE
                    if DEBUG_MODE:
                        with console_redirect(svt_logger):
                            print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve: {denoised_master_path.name}")
                    else:
                        try:
                            master_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                            denoised_master_path.unlink()
                            # Sidecar fájl törlése is
                            denoised_master_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                            denoised_master_path.with_suffix('.denoise_params').unlink(missing_ok=True)
                            with console_redirect(svt_logger):
                                print(f"[DEL] Zajszűrt mesterdarab törölve (nincs VMAF): {denoised_master_path.name} ({master_size_mb:.1f} MB felszabadítva)")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: nincs VMAF, zajszűrt master takarítás | fájl: {denoised_master_path.name} ({master_size_mb:.1f} MB)\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except (OSError, IOError, PermissionError) as e:
                            with console_redirect(svt_logger):
                                print(f"[WARN] Mesterdarab törlés hiba: {e}")
                else:
                    # VMAF will need it - don't delete, log the reason
                    with console_redirect(svt_logger):
                        cleanup_reason = "manuális VMAF" if manual_qc else "auto VMAF/PSNR"
                        print(f"💾 Zajszűrt mesterdarab MEGŐRIZVE {cleanup_reason} számításhoz: {denoised_master_path.name}")
                        print(f"   (Törlés a VMAF befejezésekor történik)")


            # Remove from processing set
            if hasattr(self, 'svt_processing_videos'):
                self.svt_processing_videos.discard(video_path)
            
            # Clear current video path in logger (use svt_logger, not self.svt_logger - they differ in multi-console mode)
            if hasattr(svt_logger, 'set_current_video_path'):
                svt_logger.set_current_video_path(None)
            
            # KRITIKUS: console_redirect stack teljes törlése a task befejezésekor
            # Ez biztosítja, hogy ne maradjon beakadt logger a stack-ben
            try:
                from .core_paths_tools_logging import STDOUT_ROUTER
                STDOUT_ROUTER.clear_all_loggers()
            except (ImportError, AttributeError):
                # Import error - non-critical
                pass  # Csendes hiba - ne zavarjuk meg a folyamatot

            # PER-VIDEO STOP RACE CONDITION FIX:
            # Ha per-video stop event miatt állt le (pl. manuális CQ override), megvárjuk,
            # amíg a GUI szál hozzáadja a replacement manuális taskot a queue-hoz.
            # A manual_override_ready_events[video_path] event-et a GUI szál set-eli
            # a task queue-ba helyezése UTÁN. Timeout 5s (safety net, ha a GUI nem jelez).
            if raw_video_stop_event.is_set() and not STOP_EVENT.is_set():
                override_ready = self.manual_override_ready_events.pop(video_path, None)
                if override_ready:
                    override_ready.wait(timeout=5.0)
                    with console_redirect(svt_logger):
                        print(f"♻ Override task kész - worker folytathat")

    def reencode_with_svt_av1(self, video_path, item_id, prompt=True, reason='manual_reencode'):
        """Initiate SVT-AV1 re-encoding for a video.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            prompt: If True, asks for confirmation before starting.
            reason: Reason for re-encoding. 'manual_reencode' for manual, 'auto_reencode' for automatic.
        """
    
        current_values = self.tree.item(item_id, 'values')
        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
        active_override_requested = False
    
        # ============ ÚJ LOGIKA: Folyamatban lévő átkódolás ellenőrzése és leállítása ============
        if prompt:  # Csak manuális újrakódolásnál (prompt=True)
            is_encoding, task_info, queue_type = self._is_video_actively_encoding(video_path)
            
            if is_encoding:
                active_override_requested = True
                # Megerősítés kérése a felhasználótól
                task_type_str = t('task_type_auto') if not task_info.get('is_manual') else t('task_type_manual')
                
                confirm_msg = t('msg_active_reencode_svt_confirm').format(
                    task_type=task_type_str,
                    video_label=t('label_video'),
                    filename=video_path.name,
                    queue=queue_type.upper(),
                    overwrite_note=t('msg_overwrite_existing_file'),
                    keep_master_note=t('msg_keep_denoised_master_note')
                )
                
                result = messagebox.askyesno(
                    t('menu_reencode_svt'),
                    confirm_msg
                )
                
                if not result:
                    return False  # Felhasználó visszavonta

                # Override ready event létrehozása STOP ELŐTT
                override_ready = threading.Event()
                self.manual_override_ready_events[video_path] = override_ready

                # Leállítjuk a folyamatban lévő átkódolást
                self.log_status(f"[WARN] Manuális felülírás: {video_path.name}")
                success = self.stop_encoding_for_video(video_path, cleanup_denoised_master=False)

                if not success:
                    self.manual_override_ready_events.pop(video_path, None)
                    messagebox.showerror(
                        t('msg_error'),
                        t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                    )
                    return False

                if not self._wait_for_video_stop_completion(video_path):
                    self.manual_override_ready_events.pop(video_path, None)
                    messagebox.showerror(
                        t('msg_error'),
                        t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                    )
                    return False
            else:
                # Nincs folyamatban lévő átkódolás - normál megerősítés
                if "SVT-AV1" in status and "queue" in status.lower():
                    messagebox.showwarning(t('msg_warning'), t('msg_svt_already_processing'))
                    return False
                
                result = messagebox.askyesno(
                    t('menu_reencode_svt'),
                    f"{t('msg_svt_reencode_confirm')}\n\n{video_path.name}\n\n{t('msg_overwrite_existing_file')}"
                )
                if not result:
                    return False
        
        # ============ EREDETI LOGIKA FOLYTATÓDIK ============
    
        # Beállítjuk az encoding állapotot az elején, hogy a worker ne álljon le
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        is_enc, _, _ = self.get_encoding_state()
        if not is_enc:
            # ThreadSafeEvent provides built-in atomicity, no external lock needed
            STOP_EVENT.clear()
            self.set_encoding_state(is_encoding=True)
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
            self.root.after(100, self.check_encoding_queue)

            # CRITICAL FIX: Start encoding_worker thread to process pending auto tasks
            # Without this, pending videos won't be queued when manual re-encode is started
            if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                self.set_encoding_state(encoding_worker_running=True)
                if hasattr(self, '_refresh_encoding_worker_snapshot'):
                    self._refresh_encoding_worker_snapshot()
                self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                self.encoding_worker_thread.start()

        # CRITICAL: Reset graceful stop flag for manual override (ALWAYS, not just when is_encoding=False)
        # This ensures manual re-encode works even during graceful stop
        self.set_encoding_state(graceful_stop_requested=False)

        # Újrakódolás során mindig .av1.mkv kiterjesztésű output fájlt használunk
        # (nem az eredeti kiterjesztésű másolt fájlt)
        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
        # Frissítjük a mapping-et is
        self.video_to_output[video_path] = output_file

        # GUI FREEZE FIX: A fájltörlés és feliratkeresés átkerült az SVT workerbe
        # (_reencode_pre_cleanup), mert hálózati megosztáson ezek percekig blokkolhatják a GUI szálat.
        # A worker háttérszálban fut, így nem fagyasztja be a GUI-t.
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

        # LIST-BASED QUEUE: Add manual re-encode task
        # subtitle_files=None jelzi a workernek, hogy a feliratkeresés még nem történt meg
        tree_order_index = None
        try:
            tree_order_index = int(self.tree.index(item_id))
        except Exception:
            tree_order_index = None
        self.add_to_svt_queue(
            video_path=video_path, item_id=item_id, task_type='encode', is_manual=True,
            output_file=output_file, subtitle_files=None,
            invalid_subtitles=None, orig_size_str=orig_size_str,
            initial_min_vmaf=self.min_vmaf.get(), vmaf_step=self.vmaf_step.get(),
            max_encoded=self.max_encoded_percent.get(),
            resize_enabled=self.resize_enabled.get(), resize_height=self.resize_height.get(),
            audio_compression_enabled=self.audio_compression_enabled.get(),
            audio_compression_method=self.audio_compression_method.get(),
            pre_cached_tree_index=tree_order_index,
            queue_front=active_override_requested,
            reason=reason,
            needs_pre_cleanup=True
        )

        # Ensure workers are started for this task
        self._ensure_svt_workers_running()

        # Jelezzük a worker-nek, hogy a replacement task hozzáadása megtörtént
        override_ready = self.manual_override_ready_events.pop(video_path, None)
        if override_ready:
            override_ready.set()

        current_values = self.tree.item(item_id, 'values')
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        status_text = t('status_svt_queue')
        # Azonnali frissítés a tree-ben (UI thread)
        new_values = list(current_values)
        if len(new_values) < len(self.COLUMN_INDEX):
            new_values.extend([''] * (len(self.COLUMN_INDEX) - len(new_values)))
        new_values[self.COLUMN_INDEX['status']] = status_text
        new_values[self.COLUMN_INDEX['cq']] = "-"
        new_values[self.COLUMN_INDEX['vmaf']] = "-"
        new_values[self.COLUMN_INDEX['psnr']] = "-"
        new_values[self.COLUMN_INDEX['progress']] = "-"
        new_values[self.COLUMN_INDEX['new_size']] = "-"
        new_values[self.COLUMN_INDEX['size_change']] = "-"
        # Megtartjuk a duration és frames értékeket
        new_values[self.COLUMN_INDEX['completed_date']] = completed_date
        self.tree.item(item_id, values=tuple(new_values))

        self.encoding_queue.put_nowait(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
        self.encoding_queue.put_nowait(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik  # JSON mentés SVT queue-ba kerülés után

        # Start gomb állapotának frissítése
        self.update_start_button_state()
    
        if prompt:
            messagebox.showinfo(t('msg_started'), f"{t('msg_svt_added')}\n{video_path.name}")
        else:
            self.log_status(f"[OK] SVT-AV1 sorba állítva (batch): {video_path.name}")
        
        self.update_summary_row()
        
        # CRITICAL: Save state to DB immediately (debounced)
        if hasattr(self, '_save_settings_debounced'):
            self._save_settings_debounced()

        def clear_reencode_output_state_in_db():
            try:
                self.update_single_video_in_db(
                    video_path=video_path,
                    item_id=item_id,
                    status_text=status_text,
                    cq_str="-",
                    vmaf_str="-",
                    psnr_str="-",
                    orig_size_str=orig_size_str,
                    new_size_mb=None,
                    change_percent=None,
                    completed_date="",
                    denoise_enabled=self.get_tree_item_meta(item_id, 'denoise_enabled', 0),
                    hard_rotate_degrees=self.get_tree_item_meta(item_id, 'hard_rotate_degrees', 0),
                    clear_output_state=True
                )
            except Exception:
                pass

        if hasattr(self, '_start_db_thread'):
            self._start_db_thread(clear_reencode_output_state_in_db, name="ClearReencodeOutputState", daemon=True)
        
        return True

    def reencode_with_cq(self, video_path, item_id, target_cq, encoder_type):
        """Initiate re-encoding with a specific CQ/CRF value.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            target_cq: Target CQ/CRF value.
            encoder_type: 'nvenc' or 'svt-av1'.
        """
    
        # ============ ÚJ LOGIKA: Folyamatban lévő átkódolás ellenőrzése és leállítása ============
        active_override_requested = False
        is_encoding, task_info, queue_type = self._is_video_actively_encoding(video_path)
        
        if is_encoding:
            active_override_requested = True
            # Megerősítés kérése a felhasználótól
            task_type_str = t('task_type_auto') if not task_info.get('is_manual') else t('task_type_manual')
            encoder_display = "NVENC" if encoder_type == "NVENC" else "SVT-AV1"
            
            confirm_msg = t('msg_active_reencode_config_confirm').format(
                task_type=task_type_str,
                video_label=t('label_video'),
                filename=video_path.name,
                queue=queue_type.upper(),
                encoder_label=t('label_encoder'),
                encoder=encoder_display,
                cq=target_cq,
                overwrite_note=t('msg_overwrite_existing_file'),
                keep_master_note=t('msg_keep_denoised_master_note')
            )
            
            result = messagebox.askyesno(
                t('menu_reencode'),
                confirm_msg
            )
            
            if not result:
                return  # Felhasználó visszavonta

            # Override ready event létrehozása STOP ELŐTT
            override_ready = threading.Event()
            self.manual_override_ready_events[video_path] = override_ready

            # Leállítjuk a folyamatban lévő átkódolást
            self.log_status(f"[WARN] Manuális felülírás: {video_path.name}")
            success = self.stop_encoding_for_video(video_path, cleanup_denoised_master=False)

            if not success:
                self.manual_override_ready_events.pop(video_path, None)
                messagebox.showerror(
                    t('msg_error'),
                    t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                )
                return

            if not self._wait_for_video_stop_completion(video_path):
                self.manual_override_ready_events.pop(video_path, None)
                messagebox.showerror(
                    t('msg_error'),
                    t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                )
                return
        else:
            # Nincs folyamatban lévő átkódolás - normál megerősítés
            encoder_display = "NVENC" if encoder_type == "NVENC" else "SVT-AV1"
            result = messagebox.askyesno(
                t('menu_reencode'),
                f"{t('msg_reencode_confirm')}\n\n"
                f"{t('label_video')}: {video_path.name}\n"
                f"{t('label_encoder')}: {encoder_display}\n"
                f"CQ/CRF: {target_cq}\n\n"
                f"{t('msg_overwrite_existing_file')}"
            )
            
            if not result:
                return
        
        # ============ EREDETI LOGIKA FOLYTATÓDIK ============
        
        # Újrakódolás során mindig .av1.mkv kiterjesztésű output fájlt használunk
        # (nem az eredeti kiterjesztésű másolt fájlt)
        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
        # Frissítjük a mapping-et is
        self.video_to_output[video_path] = output_file
        
        current_values = self.tree.item(item_id, 'values')
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else None
        tree_order_index = None
        try:
            tree_order_index = int(self.tree.index(item_id))
        except Exception:
            tree_order_index = None

        # GUI FREEZE FIX: A fájltörlés és feliratkeresés átkerült a workerbe (_reencode_pre_cleanup)

        # VMAF érték konvertálása ha elérhető
        vmaf_value = None
        if vmaf_str and vmaf_str != "-":
            try:
                vmaf_value = float(vmaf_str)
            except (ValueError, TypeError):
                vmaf_value = None
        
        if encoder_type == "NVENC":
            # NVENC encoding queue-ba - manuális task-ként
            task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': None,  # GUI FREEZE FIX: worker-ben keresés
                'invalid_subtitles': None,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'target_cq': target_cq,
                'vmaf_value': vmaf_value,
                'resize_enabled': self.resize_enabled.get(),
                'resize_height': self.resize_height.get(),
                'skip_crf_search': True,
                'reason': 'manual_reencode_cq',
                'needs_pre_cleanup': True,
            }
            # Hozzáadás a manuális task listához és worker indítása
            if active_override_requested:
                self.manual_nvenc_tasks.insert(0, task)
            else:
                self.manual_nvenc_tasks.append(task)
            # Státusz frissítés a manuális CQ értékkel
            status_suffix = self._format_manual_status_suffix(target_cq, None)
            self.encoding_queue.put_nowait(("update", item_id, t('status_nvenc_queue') + status_suffix, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            is_enc, _, _ = self.get_encoding_state()
            if not is_enc:
                # ThreadSafeEvent provides built-in atomicity, no external lock needed
                STOP_EVENT.clear()
                self.set_encoding_state(is_encoding=True)
                self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.NORMAL)
                self.load_videos_btn.config(state=tk.DISABLED)
                self.root.after(100, self.check_encoding_queue)

                # CRITICAL FIX: Start encoding_worker thread to process pending auto tasks
                if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                    self.set_encoding_state(encoding_worker_running=True)
                    if hasattr(self, '_refresh_encoding_worker_snapshot'):
                        self._refresh_encoding_worker_snapshot()
                    self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                    self.encoding_worker_thread.start()

            # CRITICAL: Reset graceful stop flag for manual override (ALWAYS)
            self.set_encoding_state(graceful_stop_requested=False)
            # Worker thread indítása, ha nincs futó
            if not hasattr(self, 'manual_nvenc_worker') or not self.manual_nvenc_worker.is_alive():
                self.manual_nvenc_worker = threading.Thread(target=self.process_manual_nvenc_tasks_worker, daemon=True)
                self.manual_nvenc_worker.start()
        else:
            # SVT-AV1 encoding queue-ba
            # LIST-BASED QUEUE: Add manual SVT re-encode task
            self.add_to_svt_queue(
                video_path=video_path, item_id=item_id, task_type='encode', is_manual=True,
                output_file=output_file, subtitle_files=None,  # GUI FREEZE FIX: worker-ben keresés
                invalid_subtitles=None, orig_size_str=orig_size_str,
                initial_min_vmaf=self.min_vmaf.get(), vmaf_step=self.vmaf_step.get(),
                max_encoded=self.max_encoded_percent.get(),
                resize_enabled=self.resize_enabled.get(), resize_height=self.resize_height.get(),
                target_cq=target_cq, vmaf_value=vmaf_value, skip_crf_search=True,
                pre_cached_tree_index=tree_order_index,
                queue_front=active_override_requested,
                reason='manual_reencode_cq',
                needs_pre_cleanup=True
            )

            # Ensure workers are started for this task
            self._ensure_svt_workers_running()

            # Státusz frissítés a manuális CQ értékkel
            status_suffix = self._format_manual_status_suffix(target_cq, None)
            status_text = t('status_svt_queue') + status_suffix
            self.encoding_queue.put_nowait(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
            # Beállítjuk az encoding állapotot és indítjuk a queue ellenőrzést
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            is_enc, _, _ = self.get_encoding_state()
            if not is_enc:
                # ThreadSafeEvent provides built-in atomicity, no external lock needed
                STOP_EVENT.clear()
                self.set_encoding_state(is_encoding=True)
                self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.NORMAL)
                self.load_videos_btn.config(state=tk.DISABLED)
                self.root.after(100, self.check_encoding_queue)

                # CRITICAL FIX: Start encoding_worker thread to process pending auto tasks
                if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                    self.set_encoding_state(encoding_worker_running=True)
                    if hasattr(self, '_refresh_encoding_worker_snapshot'):
                        self._refresh_encoding_worker_snapshot()
                    self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                    self.encoding_worker_thread.start()

            # CRITICAL: Reset graceful stop flag for manual override (ALWAYS)
            self.set_encoding_state(graceful_stop_requested=False)

        # Jelezzük a worker-nek, hogy a replacement task hozzáadása megtörtént
        override_ready = self.manual_override_ready_events.pop(video_path, None)
        if override_ready:
            override_ready.set()

        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
        self.update_summary_row()

        # CRITICAL: Save manual CQ parameters to DB IMMEDIATELY (no debounce!)
        # This ensures that if app crashes or loses power during encoding,
        # the manual CQ request is preserved and can be resumed on next startup
        status_suffix = self._format_manual_status_suffix(target_cq, None)
        if encoder_type == "NVENC":
            final_status_text = t('status_nvenc_queue') + status_suffix
        else:
            final_status_text = t('status_svt_queue') + status_suffix

        self._save_video_manual_params_immediately(
            video_path=video_path,
            item_id=item_id,
            status_text=final_status_text,
            orig_size_str=orig_size_str,
            manual_cq_range=None,  # reencode_with_cq doesn't use CQ range
            manual_cq_value=target_cq,
            manual_quality_check=None  # reencode_with_cq doesn't use quality check
        )

        # Removed messagebox - user already confirmed, no need for additional notification

    def reencode_with_manual_config(self, video_path, item_id, target_cq, encoder_type, quality_check, cq_range, skip_confirmation=False):
        """Initiate re-encoding with manual configuration (CQ value, encoder, quality check).

        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            target_cq: Target CQ/CRF value.
            encoder_type: 'NVENC' or 'SVT-AV1'.
            quality_check: 'vmaf', 'psnr', 'both', 'none'.
            cq_range: CQ range string (e.g., "10-24").
            skip_confirmation: If True, skip confirmation dialogs (for bulk operations).
        """
        
        # ============ ÚJ LOGIKA: Folyamatban lévő átkódolás ellenőrzése és leállítása ============
        active_override_requested = False
        is_encoding, task_info, queue_type = self._is_video_actively_encoding(video_path)
        
        if is_encoding:
            active_override_requested = True

            # Megerősítés kérése a felhasználótól (csak ha nem bulk művelet)
            if not skip_confirmation:
                task_type_str = t('task_type_auto') if not task_info.get('is_manual') else t('task_type_manual')
                encoder_display = encoder_type  # Már "NVENC" vagy "SVT-AV1"
                quality_check_display = {
                    'vmaf': t('manual_quality_vmaf'),
                    'psnr': t('manual_quality_psnr'),
                    'both': t('manual_quality_both'),
                    'none': t('manual_quality_none')
                }.get(quality_check, quality_check)

                confirm_msg = t('msg_active_reencode_manual_confirm').format(
                    task_type=task_type_str,
                    video_label=t('label_video'),
                    filename=video_path.name,
                    queue=queue_type.upper(),
                    encoder_label=t('label_encoder'),
                    encoder=encoder_display,
                    cq=target_cq,
                    cq_range=cq_range,
                    quality_label=t('label_quality_check'),
                    quality_check=quality_check_display,
                    keep_master_note=t('msg_keep_denoised_master_note')
                )

                result = messagebox.askyesno(
                    t('menu_reencode_manual'),
                    confirm_msg
                )

                if not result:
                    return  # Felhasználó visszavonta

            # Override ready event létrehozása STOP ELŐTT — a worker erre vár a finally-ban
            override_ready = threading.Event()
            self.manual_override_ready_events[video_path] = override_ready

            # Leállítjuk a folyamatban lévő átkódolást
            self.log_status(f"[WARN] Manuális felülírás: {video_path.name}")
            success = self.stop_encoding_for_video(video_path, cleanup_denoised_master=False)

            if not success:
                self.manual_override_ready_events.pop(video_path, None)
                if not skip_confirmation:
                    messagebox.showerror(
                        t('msg_error'),
                        t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                    )
                return

            if not self._wait_for_video_stop_completion(video_path):
                self.manual_override_ready_events.pop(video_path, None)
                if not skip_confirmation:
                    messagebox.showerror(
                        t('msg_error'),
                        t('msg_stop_active_encoding_failed').format(filename=video_path.name)
                    )
                return
        else:
            # Nincs folyamatban lévő átkódolás - normál megerősítés (csak ha nem bulk művelet)
            if not skip_confirmation:
                encoder_display = encoder_type  # Már "NVENC" vagy "SVT-AV1"
                quality_check_display = {
                    'vmaf': t('manual_quality_vmaf'),
                    'psnr': t('manual_quality_psnr'),
                    'both': t('manual_quality_both'),
                    'none': t('manual_quality_none')
                }.get(quality_check, quality_check)

                result = messagebox.askyesno(
                    t('menu_reencode_manual'),
                    f"{t('msg_reencode_confirm')}\n\n"
                    f"{t('label_video')}: {video_path.name}\n"
                    f"{t('label_encoder')}: {encoder_display}\n"
                    f"CQ/CRF: {target_cq} ({cq_range})\n"
                    f"{t('label_quality_check')}: {quality_check_display}\n\n"
                    f"{t('msg_overwrite_existing_file')}"
                )

                if not result:
                    return
            # Ha skip_confirmation=True, folytatjuk megerősítés nélkül
        
        # ============ EREDETI LOGIKA FOLYTATÓDIK ============
        
        # Újrakódolás során mindig .av1.mkv kiterjesztésű output fájlt használunk
        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
        self.video_to_output[video_path] = output_file
        
        current_values = self.tree.item(item_id, 'values')
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""

        # ============ DENOISE SZINT MEGHATÁROZÁSA A FLAG FÁJLBÓL ============
        # Ha a denoised master + .denoise_level flag fájl létezik a lemezen,
        # és az aktuális denoise beállítás nem változott, akkor a worker
        # újrafelhasználja a meglévő mesterfájlt ahelyett, hogy újrakezdené.
        denoise_level_for_task = 0
        try:
            denoise_idx = self.COLUMN_INDEX.get('denoise', -1)
            if denoise_idx >= 0 and len(current_values) > denoise_idx:
                denoise_level_for_task = display_to_denoise_level(current_values[denoise_idx])
        except (KeyError, IndexError, ValueError):
            pass

        if denoise_level_for_task > 0:
            master_filename = f"{video_path.stem}_denoised_master.mkv"
            denoised_master_check = output_file.parent / master_filename
            level_file_check = denoised_master_check.with_suffix('.denoise_level')
            if denoised_master_check.exists() and denoised_master_check.stat().st_size > 0 and level_file_check.exists():
                stored_level = read_denoise_level_file(level_file_check)
                if stored_level == denoise_level_for_task:
                    self.log_status(f"[INFO] Meglévő zajszűrt master újrafelhasználható: {master_filename} (szint: {stored_level})")
                else:
                    self.log_status(f"[WARN] Denoise szint változott ({stored_level} → {denoise_level_for_task}), master újragenerálás szükséges")

        tree_order_index = None
        try:
            tree_order_index = int(self.tree.index(item_id))
        except Exception:
            tree_order_index = None
        
        # Korábbi output fájl törlése (.av1.mkv)
        if output_file.exists():
            try:
                if hasattr(self, '_delete_output_file_with_probe_guard'):
                    self._delete_output_file_with_probe_guard(
                        output_file,
                        video_path=video_path,
                        item_id=item_id,
                        log_context="svt_manual_reencode"
                    )
                else:
                    output_file.unlink()
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: manuális újrakódolás előkészítés | fájl: {output_file.name}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            except Exception as e:
                messagebox.showerror(t('msg_error'), f"{t('msg_delete_failed')}\n{e}")
                return

        # CRITICAL: Töröljük a _video_only.mkv temp fájlt is (ha létezik)
        # Ez kritikus, mert ha a korábbi kódolás közben más CQ-val újraindítjuk,
        # a régi video_only összeépülne az új audioval, ami hibás eredményt adna
        video_only_temp = output_file.with_name(f"{output_file.stem}_video_only.mkv")
        if video_only_temp.exists():
            try:
                video_only_temp.unlink()
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: manuális újrakódolás előkészítés, temp fájl | fájl: {video_only_temp.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            except (KeyError, AttributeError):
                pass

        # Eredeti kiterjesztésű másolt fájl törlése (ha létezik)
        try:
            from .core_audio_video_ops import get_copy_filename
            copy_file = get_copy_filename(video_path, self.source_path, self.dest_path)
            if copy_file.exists() and copy_file != output_file:
                try:
                    copy_file.unlink()
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: manuális újrakódolás előkészítés, régi másolat | fájl: {copy_file.name}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                except (OSError, IOError, PermissionError):
                    pass
        except (ImportError, AttributeError, OSError, IOError):
            # Import or file error - non-critical
            pass

        # Feliratok törlése (mert újrakódolásba bekerülnek)
        try:
            from .core_subtitles_and_metadata import find_subtitle_files
            if self.dest_path and self.source_path:
                relative_path = video_path.relative_to(self.source_path)
                dest_sub_dir = self.dest_path / relative_path.parent
                video_stem = video_path.stem
                
                subtitle_files_list = find_subtitle_files(video_path)
                for sub_path, lang_part in subtitle_files_list:
                    if lang_part:
                        dest_sub_name = f"{video_stem}.{lang_part}{sub_path.suffix}"
                    else:
                        dest_sub_name = f"{video_stem}{sub_path.suffix}"
                    dest_sub = dest_sub_dir / dest_sub_name
                    
                    if dest_sub.exists():
                        try:
                            dest_sub.unlink()
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: manuális újrakódolás előkészítés, régi felirat | fájl: {dest_sub.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except (OSError, IOError, PermissionError):
                            pass
        except (OSError, IOError, PermissionError, AttributeError):
            pass
        
        valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
        subtitle_files = valid_subtitles
        
        # Save encoding state BEFORE task setup to detect first-start scenario
        _was_encoding_before, _, _ = self.get_encoding_state()

        # Task létrehozása encoder típustól függően
        if encoder_type == "NVENC":
            # NVENC manuális task
            task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': subtitle_files,
                'invalid_subtitles': invalid_subtitles,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'target_cq': target_cq,
                'vmaf_value': None,
                'resize_enabled': self.resize_enabled.get(),
                'resize_height': self.resize_height.get(),
                'skip_crf_search': True,
                'reason': 'manual_reencode_cq',
                'manual_cq_range': cq_range,
                'manual_cq_value': target_cq,
                'manual_quality_check': quality_check,
                'denoise_enabled': denoise_level_for_task
            }
            if active_override_requested:
                self.manual_nvenc_tasks.insert(0, task)
            else:
                self.manual_nvenc_tasks.append(task)
            
            # Státusz frissítés a manuális paraméterekkel
            status_suffix = self._format_manual_status_suffix(target_cq, quality_check)
            status_text = t('status_nvenc_queue') + status_suffix
            self.encoding_queue.put_nowait(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "pending"))

            # Encoding állapot beállítása
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            is_enc, _, _ = self.get_encoding_state()
            if not is_enc:
                # ThreadSafeEvent provides built-in atomicity, no external lock needed
                STOP_EVENT.clear()
                self.set_encoding_state(is_encoding=True)
                self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.NORMAL)
                self.load_videos_btn.config(state=tk.DISABLED)
                self.root.after(100, self.check_encoding_queue)

            # CRITICAL FIX: Start encoding_worker thread ALWAYS if needed (even if encoding already running)
            # This ensures that manual override tasks are processed immediately
            if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                self.set_encoding_state(encoding_worker_running=True)
                if hasattr(self, '_refresh_encoding_worker_snapshot'):
                    self._refresh_encoding_worker_snapshot()
                self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                self.encoding_worker_thread.start()

            # CRITICAL: Reset graceful stop flag for manual override (ALWAYS)
            self.set_encoding_state(graceful_stop_requested=False)

            # Worker indítása
            if not hasattr(self, 'manual_nvenc_worker') or not self.manual_nvenc_worker.is_alive():
                self.manual_nvenc_worker = threading.Thread(target=self.process_manual_nvenc_tasks_worker, daemon=True)
                self.manual_nvenc_worker.start()
        else:
            # SVT-AV1 task
            # LIST-BASED QUEUE: Add manual SVT re-encode task
            self.add_to_svt_queue(
                video_path=video_path, item_id=item_id, task_type='encode', is_manual=True,
                output_file=output_file, subtitle_files=subtitle_files,
                invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                initial_min_vmaf=self.min_vmaf.get(), vmaf_step=self.vmaf_step.get(),
                max_encoded=self.max_encoded_percent.get(),
                resize_enabled=self.resize_enabled.get(), resize_height=self.resize_height.get(),
                target_cq=target_cq, vmaf_value=None, skip_crf_search=True,
                manual_cq_range=cq_range, manual_cq_value=target_cq,
                manual_quality_check=quality_check,
                denoise_enabled=denoise_level_for_task,
                pre_cached_tree_index=tree_order_index,
                queue_front=active_override_requested,
                reason='manual_reencode_cq'
            )

            # Ensure workers are started for this task
            self._ensure_svt_workers_running()

            # Státusz frissítés a manuális paraméterekkel
            status_suffix = self._format_manual_status_suffix(target_cq, quality_check)
            status_text = t('status_svt_queue') + status_suffix
            self.encoding_queue.put_nowait(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "pending"))

            # Encoding állapot beállítása
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            is_enc, _, _ = self.get_encoding_state()
            if not is_enc:
                # ThreadSafeEvent provides built-in atomicity, no external lock needed
                STOP_EVENT.clear()
                self.set_encoding_state(is_encoding=True)
                self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.NORMAL)
                self.load_videos_btn.config(state=tk.DISABLED)
                self.root.after(100, self.check_encoding_queue)

            # CRITICAL FIX: Start encoding_worker thread ALWAYS if needed (even if encoding already running)
            # This ensures that manual override tasks are processed immediately
            if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                self.set_encoding_state(encoding_worker_running=True)
                if hasattr(self, '_refresh_encoding_worker_snapshot'):
                    self._refresh_encoding_worker_snapshot()
                self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                self.encoding_worker_thread.start()

            # CRITICAL: Reset graceful stop flag for manual override (ALWAYS)
            self.set_encoding_state(graceful_stop_requested=False)

        # Jelezzük a worker-nek, hogy a replacement task hozzáadása megtörtént
        override_ready = self.manual_override_ready_events.pop(video_path, None)
        if override_ready:
            override_ready.set()

        self.update_summary_row()

        # If encoding was NOT running before this manual re-encode, also load
        # all remaining pending files into queues (equivalent to pressing Start).
        if not _was_encoding_before:
            self.root.after(200, self._start_remaining_pending_files)

        # CRITICAL: Save manual parameters to DB IMMEDIATELY (no debounce!)
        # This ensures that if app crashes or loses power during encoding,
        # the manual encoding request is preserved and can be resumed on next startup
        status_suffix = self._format_manual_status_suffix(target_cq, quality_check)
        if encoder_type == "NVENC":
            final_status_text = t('status_nvenc_queue') + status_suffix
        else:
            final_status_text = t('status_svt_queue') + status_suffix

        self._save_video_manual_params_immediately(
            video_path=video_path,
            item_id=item_id,
            status_text=final_status_text,
            orig_size_str=orig_size_str,
            manual_cq_range=cq_range,
            manual_cq_value=target_cq,
            manual_quality_check=quality_check
        )

        # Removed messagebox - user already confirmed, no need for additional notification

    # NOTE: _format_manual_status_suffix removed - defined in NvencWorkerMixin (takes precedence in MRO)

    def _save_video_manual_params_immediately(self, video_path, item_id, status_text, orig_size_str,
                                               manual_cq_range, manual_cq_value, manual_quality_check,
                                               denoise_enabled=None):
        """Save manual encoding parameters to database IMMEDIATELY (no debounce).

        This ensures that if the app crashes or loses power during encoding,
        the manual request is preserved and can be resumed on next startup.

        Args:
            video_path: Path to the video file
            item_id: Tree item ID
            status_text: Current status text (with manual suffix)
            orig_size_str: Original size string
            manual_cq_range: CQ range (e.g., "10-20")
            manual_cq_value: Target CQ value
            manual_quality_check: Quality check type ('vmaf', 'psnr', 'both', or None)
            denoise_enabled: Denoise level (0, 1, 2, or 3)
        """
        try:
            denoise_val_for_cache = denoise_enabled
            if denoise_val_for_cache is None:
                denoise_val_for_cache = self.get_tree_item_meta(item_id, 'denoise_enabled', 0)
            self.set_tree_item_meta(
                item_id,
                status_code=normalize_status_to_code(status_text) or self.get_tree_item_meta(item_id, 'status_code', 'svt_queue'),
                status_display=status_text,
                manual_cq_range=manual_cq_range,
                manual_cq_value=manual_cq_value,
                manual_quality_check=manual_quality_check,
                denoise_enabled=normalize_denoise_level(denoise_val_for_cache)
            )
        except Exception:
            pass

        def save_in_thread():
            try:
                # Get denoise from tree_item_data if not provided
                denoise_val = denoise_enabled
                if denoise_val is None:
                    try:
                        denoise_val = self.get_tree_item_meta(item_id, 'denoise_enabled', 0)
                    except (KeyError, AttributeError, TypeError):
                        denoise_val = 0

                # Call update_single_video_in_db with manual parameters
                # We pass "-" for fields we don't have yet (CQ, VMAF, etc.)
                self.update_single_video_in_db(
                    video_path=video_path,
                    item_id=item_id,
                    status_text=status_text,
                    cq_str="-",
                    vmaf_str="-",
                    psnr_str="-",
                    orig_size_str=orig_size_str,
                    new_size_mb=None,
                    change_percent=None,
                    completed_date="",
                    manual_cq_range=manual_cq_range,
                    manual_cq_value=manual_cq_value,
                    manual_quality_check=manual_quality_check,
                    denoise_enabled=denoise_val,
                    clear_output_state=True
                )
            except Exception as e:
                # Silent fail - don't crash the UI thread
                try:
                    from .core_paths_tools_logging import LOG_WRITER
                    if LOG_WRITER:
                        LOG_WRITER.write(f"[WARN] _save_video_manual_params_immediately error: {e}\n")
                        LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    # File or DB error - non-critical
                    pass

        # Run in background thread to not block UI
        self._start_db_thread(save_in_thread, name="SaveManualParamsDB", daemon=True)

    # NOTE: update_svt_preset_label moved to SettingsControlMixin (takes precedence in MRO)
    # The implementation in SettingsControlMixin provides centralized settings control
    # and automatic debounced saving. See gui_settings_control.py for details.
    def _update_svt_preset_label_moved_to_settings_control(self, value):
        """DEPRECATED: Moved to SettingsControlMixin."""
        pass


    def _process_vmaf_task(self, task, svt_logger, worker_index=0):
        """Process VMAF/PSNR calculation task in SVT worker thread.
        
        This method handles VMAF/PSNR calculation requests that are now queued
        in the same SVT_QUEUE as encoding tasks. This ensures that VMAF calculations
        wait for an available SVT worker and don't run simultaneously with encoding.
        
        Args:
            task: VMAF task dictionary
            svt_logger: Logger instance for console output
            worker_index: Worker thread index
        """
        video_path = task['video_path']
        # Use vmaf_reference_path if provided (e.g. denoised master), otherwise original video path
        # CRITICAL: Use 'or' to handle None values - dict.get() returns None if key exists with None value!
        calculation_reference_path = task.get('vmaf_reference_path') or video_path
        output_file = task['output_file']
        item_id = task['item_id']
        orig_size_str = task.get('orig_size_str', "-")
        
        # Tree values from task (fetched in GUI thread for thread-safety)
        original_cq_str = task.get('current_cq_str', "-")
        original_vmaf_str = task.get('current_vmaf_str', "-")
        original_psnr_str = task.get('current_psnr_str', "-")
        original_new_size_str = task.get('current_new_size_str', "-")
        original_size_change = task.get('current_size_change', "-")
        original_completed_date = task.get('current_completed_date', "")
        original_status = task.get('current_status', t('status_completed'))
        check_vmaf = bool(task.get('check_vmaf', True))
        check_psnr = bool(task.get('check_psnr', True))
        
        if not check_vmaf and not check_psnr:
            check_vmaf = True  # Safety: at least one must be checked
        
        pending_vmaf = bool(check_vmaf)
        pending_psnr = bool(check_psnr)
        
        # Set current video path in logger
        if hasattr(svt_logger, 'set_current_video_path'):
            svt_logger.set_current_video_path(video_path)
        
        # Add to VMAF processing set
        if hasattr(self, 'vmaf_processing_videos'):
            with self.nvenc_selection_lock:
                self.vmaf_processing_videos.add(video_path)
        
        try:
            # Debug log
            with console_redirect(svt_logger):
                print(f"\n{t('log_vmaf_task_received').format(filename=video_path.name)}")
                print(f"   Output: {output_file.name}")
                print(f"   VMAF: {check_vmaf}, PSNR: {check_psnr}")
            
            # Check if stop requested
            if STOP_EVENT.is_set():
                waiting_status = self._get_vmaf_waiting_status_text(check_vmaf, check_psnr)
                self.encoding_queue.put_nowait((
                    "update", item_id, waiting_status,
                    original_cq_str, original_vmaf_str, original_psnr_str, "-",
                    orig_size_str, original_new_size_str, original_size_change,
                    original_completed_date
                ))
                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                if hasattr(self, 'vmaf_processing_videos'):
                    with self.nvenc_selection_lock:
                        self.vmaf_processing_videos.discard(video_path)
                return
            
            # Check if files exist
            if not video_path.exists() or not output_file.exists():
                self.encoding_queue.put_nowait(("update", item_id, t('status_file_missing'), "-", "-", "-", "-", orig_size_str, "-", "-", original_completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                if hasattr(self, 'vmaf_processing_videos'):
                    with self.nvenc_selection_lock:
                        self.vmaf_processing_videos.discard(video_path)
                return
            
            # Reset logger state before VMAF calculation
            if hasattr(svt_logger, 'set_current_video_path'):
                svt_logger.set_current_video_path(video_path)
            
            # Clear logger stack
            try:
                from .core_paths_tools_logging import STDOUT_ROUTER
                STDOUT_ROUTER.clear_all_loggers()
            except (KeyError, AttributeError):
                # Denoise dict cleanup error - non-critical
                pass
            
            # Calculate missing file sizes if needed
            if (original_new_size_str == "-" or original_size_change == "-") and output_file.exists():
                try:
                    orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                    if original_new_size_str == "-" and new_size_mb > 0:
                        original_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                    if original_size_change == "-" and orig_size_mb > 0:
                        original_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                except (OSError, IOError, AttributeError, ValueError):
                    # Log writer error - non-critical
                    pass
            
            # Update status to "VMAF calculation in progress..." with worker index
            with console_redirect(svt_logger):
                print(t('log_status_update_vmaf'))
            vmaf_status_with_worker = f"SVT-AV1 #{worker_index + 1} {t('status_vmaf_calculating')}"
            self.encoding_queue.put_nowait(("update", item_id, vmaf_status_with_worker, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, "-"))
            self.encoding_queue.put_nowait(("tag", item_id, "encoding"))
            
            # Store start time
            self.encoding_start_times[item_id] = time.time()
            
            # Variables for progress tracking
            callback_cq_str = original_cq_str
            callback_vmaf_str = original_vmaf_str
            callback_psnr_str = original_psnr_str
            callback_orig_size_str = orig_size_str
            callback_new_size_str = original_new_size_str
            callback_size_change = original_size_change
            video_duration_seconds = None
            total_duration_str = None
            metric_results = {'vmaf': None, 'psnr': None}
            metadata_updated_once = False
            current_progress_message = "-"
            # Add worker index to VMAF calculation status
            current_status_display = f"SVT-AV1 #{worker_index + 1} {t('status_vmaf_calculating')}"
            
            def update_metadata_partial():
                nonlocal metadata_updated_once
                if metric_results['vmaf'] is None:
                    return
                with console_redirect(svt_logger):
                    update_video_metadata_vmaf(output_file, metric_results['vmaf'], psnr_value=metric_results['psnr'], logger=svt_logger)
                metadata_updated_once = True
            
            def push_partial_update():
                completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                self.encoding_queue.put_nowait(("update", item_id, current_status_display, callback_cq_str, callback_vmaf_str, callback_psnr_str, current_progress_message, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
            
            def metric_done_callback(metric_name, value):
                nonlocal callback_vmaf_str, callback_psnr_str, current_status_display, pending_vmaf, pending_psnr
                metric_upper = (metric_name or "").upper()
                if metric_upper == 'VMAF':
                    metric_results['vmaf'] = value
                    callback_vmaf_str = format_metric_value(value)
                    pending_vmaf = False
                    update_metadata_partial()
                    if check_psnr:
                        current_status_display = t('status_psnr_only')
                    push_partial_update()
                elif metric_upper in ('PSNR', 'XPSNR'):
                    metric_results['psnr'] = value
                    callback_psnr_str = format_metric_value(value)
                    pending_psnr = False
                    if metric_results['vmaf'] is None:
                        try:
                            _, output_vmaf_meta, _, _, _, _, _, _, _, _ = get_output_file_info(output_file)
                            if output_vmaf_meta is not None:
                                metric_results['vmaf'] = output_vmaf_meta
                                callback_vmaf_str = format_metric_value(output_vmaf_meta)
                        except (OSError, IOError, PermissionError):
                            # File delete error - non-critical
                            pass
                    update_metadata_partial()
                    push_partial_update()
            
            def progress_callback(msg):
                nonlocal current_status_display, current_progress_message
                if STOP_EVENT.is_set():
                    return

                # Add worker index to VMAF calculation status
                status = f"SVT-AV1 #{worker_index + 1} {t('status_vmaf_calculating')}"
                progress_display = "-"
                completed_date_to_use = self.estimated_end_dates.get(item_id, "-")

                if isinstance(msg, dict) and msg.get('type') == 'abav1_progress':
                    metric_name = msg.get('metric')
                    if metric_name == 'VMAF':
                        status = f"SVT-AV1 #{worker_index + 1} {t('status_vmaf_only')}"
                    elif metric_name in ('XPSNR', 'PSNR'):
                        status = f"SVT-AV1 #{worker_index + 1} {t('status_psnr_only')}"
                    percent = msg.get('percent')
                    eta_seconds = msg.get('eta_seconds')
                    duration_for_calc = video_duration_seconds if video_duration_seconds else msg.get('duration_seconds')
                    elapsed_seconds = msg.get('elapsed_seconds')
                    if elapsed_seconds is None and percent is not None and duration_for_calc:
                        elapsed_seconds = max(0.0, min(duration_for_calc, duration_for_calc * (percent / 100.0)))
                    elapsed_str = format_seconds_hms(elapsed_seconds) if elapsed_seconds is not None else None
                    total_str = total_duration_str or format_seconds_hms(duration_for_calc)
                    if elapsed_str and total_str:
                        progress_display = f"{elapsed_str} / {total_str}"
                    elif percent is not None:
                        progress_display = f"{format_localized_number(percent, decimals=1)}%"
                    else:
                        progress_display = msg.get('text', "-")
                    
                    if eta_seconds is not None:
                        try:
                            eta_seconds = float(max(0.0, eta_seconds))
                            estimated_end_datetime = datetime.fromtimestamp(time.time() + eta_seconds)
                            estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                            self.estimated_end_dates[item_id] = estimated_end_str
                            completed_date_to_use = estimated_end_str
                        except (ValueError, OverflowError):
                            completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                    else:
                        completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                else:
                    progress_display = msg if isinstance(msg, str) else str(msg)
                    completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                
                current_status_display = status
                current_progress_message = progress_display
                self.encoding_queue.put_nowait(("update", item_id, status, callback_cq_str, callback_vmaf_str, callback_psnr_str, progress_display, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
            
            # Run VMAF calculation
            try:
                with console_redirect(svt_logger):
                    print(f"\n{'='*80}")
                    print(f"{t('log_vmaf_test').format(filename=video_path.name)}")
                    print(f"{'='*80}")
                    print(t('log_calling_vmaf'))
                    print(f"   Reference: {calculation_reference_path}")
                    print(f"   Encoded: {output_file}")
                    print(f"   Check VMAF: {check_vmaf}, Check PSNR: {check_psnr}")
                    print(f"   {t('log_files_exist').format(ref=calculation_reference_path.exists(), enc=output_file.exists())}")
                
                if not calculation_reference_path.exists() or not output_file.exists():
                    with console_redirect(svt_logger):
                        print(t('log_error_files_missing'))
                    raise FileNotFoundError(t('log_files_not_exist').format(ref=calculation_reference_path.exists(), enc=output_file.exists()))
                
                with console_redirect(svt_logger):
                    print(t('log_files_verified'))
                    vmaf_result = calculate_full_vmaf(
                        calculation_reference_path,
                        output_file,
                        progress_callback,
                        STOP_EVENT,
                        logger=svt_logger,
                        check_vmaf=check_vmaf,
                        check_psnr=check_psnr,
                        metric_done_callback=metric_done_callback
                    )
                    print(t('log_vmaf_returned').format(result=vmaf_result))
                    if vmaf_result:
                        vmaf_value, psnr_value = vmaf_result
                    else:
                        vmaf_value, psnr_value = (None, None)
                
                final_vmaf_value = metric_results['vmaf'] if metric_results['vmaf'] is not None else vmaf_value
                final_psnr_value = metric_results['psnr'] if metric_results['psnr'] is not None else psnr_value
                metrics_ok = True
                if check_vmaf and final_vmaf_value is None:
                    metrics_ok = False
                if check_psnr and final_psnr_value is None:
                    metrics_ok = False
                
                if metrics_ok:
                    with console_redirect(svt_logger):
                        print(f"[OK] VMAF/PSNR kalkuláció sikeres")
                        print(f"   Final VMAF: {final_vmaf_value}, Final PSNR: {final_psnr_value}")
                    
                    # Update metadata if not already done
                    if not metadata_updated_once and final_vmaf_value is not None:
                        with console_redirect(svt_logger):
                            print(f"🔄 Metaadat frissítés...")
                            try:
                                update_video_metadata_vmaf(output_file,final_vmaf_value, psnr_value=final_psnr_value, logger=svt_logger)
                                metadata_updated_once = True
                                print(f"[OK] Metaadat frissítés sikeres")
                            except Exception as meta_err:
                                print(f"[ERROR] METAADAT FRISSÍTÉS HIBA: {meta_err}")
                    
                    # Read output file info
                    try:
                        output_cq_crf, output_vmaf_meta, output_psnr_meta, output_frame_count, output_file_size, output_modified_date, output_encoder_type, _, _, _ = get_output_file_info(output_file)
                    except (OSError, IOError, ValueError, AttributeError):
                        # File or dict error - use fallback
                        pass
                        output_cq_crf = None
                        output_vmaf_meta = None
                        output_psnr_meta = None
                        output_frame_count = None
                        output_file_size = None
                        output_modified_date = None
                        output_encoder_type = None
                    
                    # Determine final values for display
                    final_cq_str = original_cq_str
                    if output_cq_crf is not None:
                        final_cq_str = str(output_cq_crf)
                    
                    final_new_size_str = original_new_size_str
                    final_size_change = original_size_change
                    if output_file_size is not None:
                        new_size_mb = output_file_size / (1024**2)
                        final_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        
                        if orig_size_str and orig_size_str != "-" and 'MB' in orig_size_str:
                            try:
                                orig_size_val = float(orig_size_str.replace(' MB', ''))
                                change_percent = ((new_size_mb - orig_size_val) / orig_size_val) * 100 if orig_size_val > 0 else 0
                                final_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                            except (ValueError, TypeError, ZeroDivisionError, AttributeError):
                                pass
                    
                    # Determine status based on encoder type
                    final_status_code_hint = task.get('final_status_code')
                    status_hint = None
                    if final_status_code_hint:
                        localized_hint = status_code_to_localized(final_status_code_hint)
                        if localized_hint and normalize_status_to_code(localized_hint) == final_status_code_hint:
                            status_hint = localized_hint
                    
                    status_code = normalize_status_to_code(original_status)
                    if status_hint:
                        status = status_hint
                    elif status_code in ('vmaf_waiting', 'vmaf_psnr_waiting', 'psnr_waiting'):
                        if output_encoder_type:
                            if output_encoder_type == 'nvenc':
                                status = t('status_completed_nvenc')
                            elif output_encoder_type == 'svt-av1':
                                status = t('status_completed_svt')
                            else:
                                status = t('status_completed')
                        else:
                            status = t('status_completed')
                    else:
                        status = original_status
                    
                    final_completed_date = output_modified_date if output_modified_date else original_completed_date
                    
                    # Display VMAF and PSNR
                    if final_psnr_value is not None:
                        vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                        psnr_display = format_metric_value(final_psnr_value)
                    elif output_psnr_meta is not None:
                        if output_vmaf_meta is not None:
                            vmaf_display = format_metric_value(output_vmaf_meta)
                        elif final_vmaf_value is not None:
                            vmaf_display = format_metric_value(final_vmaf_value)
                        else:
                            vmaf_display = callback_vmaf_str
                        psnr_display = format_metric_value(output_psnr_meta)
                    else:
                        vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                        psnr_display = callback_psnr_str if callback_psnr_str not in ("", None, "-") else "-"
                    
                    final_status_code = normalize_status_to_code(status)
                    if is_status_needs_check(status):
                        final_tag = "needs_check"
                    elif final_status_code == 'completed_copy':
                        final_tag = "completed_copy"
                    elif is_status_failed(status):
                        final_tag = "failed"
                    elif is_status_completed(status):
                        final_tag = "completed"
                    else:
                        final_tag = "pending"

                    self.encoding_queue.put_nowait(("update", item_id, status, final_cq_str, vmaf_display, psnr_display, "100%", orig_size_str, final_new_size_str, final_size_change, final_completed_date))
                    self.encoding_queue.put_nowait(("tag", item_id, final_tag))
                    if final_tag in ("completed", "completed_copy"):
                        self.encoding_queue.put_nowait(("schedule_hide_completed", item_id))
                    self.encoding_queue.put_nowait(("progress_bar", 0))  # Force progress bar update
                    
                    # Update database
                    new_size_mb = None
                    change_percent = None
                    if output_file_size is not None:
                        new_size_mb = output_file_size / (1024**2)
                    elif final_new_size_str and final_new_size_str != "-" and 'MB' in final_new_size_str:
                        try:
                            new_size_mb = float(final_new_size_str.replace(' MB', '').replace(',', '.'))
                        except (ValueError, TypeError):
                            pass
                    if final_size_change and final_size_change != "-" and '%' in final_size_change:
                        try:
                            change_percent = float(final_size_change.replace('%', '').replace('+', '').replace(',', '.'))
                        except (ValueError, TypeError):
                            pass
                    
                    if video_path:
                        def update_db_after_vmaf():
                            try:
                                self.update_single_video_in_db(
                                    video_path, item_id, status, final_cq_str,
                                    vmaf_display, psnr_display, orig_size_str,
                                    new_size_mb, change_percent, final_completed_date
                                )
                            except (OSError, IOError, PermissionError):
                                # File delete error - non-critical
                                pass
                        self._start_db_thread(update_db_after_vmaf, name="SvtVmafResultDB", daemon=True)
                    
                    # Clear encoding times
                    self.clear_encoding_times(item_id)
                    
                    with console_redirect(svt_logger):
                        if check_psnr and final_psnr_value is not None:
                            print(f"\n{t('log_vmaf_psnr_done').format(filename=video_path.name, vmaf=format_metric_value(final_vmaf_value) if final_vmaf_value is not None else '-', psnr=format_metric_value(final_psnr_value))}\n")
                        elif final_vmaf_value is not None and check_vmaf:
                            print(f"\n{t('log_vmaf_done').format(filename=video_path.name, vmaf=format_metric_value(final_vmaf_value))}\n")
                        elif check_psnr and final_psnr_value is not None:
                            print(f"\n{t('log_psnr_done').format(filename=video_path.name, psnr=format_metric_value(final_psnr_value))}\n")
                else:
                    # VMAF/PSNR calculation error - restore original values
                    self.encoding_queue.put_nowait(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                    self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                    self.encoding_queue.put_nowait(("progress_bar", 0))  # Force progress bar update
                    self.clear_encoding_times(item_id)
                    with console_redirect(svt_logger):
                        print(f"\n{t('log_vmaf_calc_error').format(filename=video_path.name)}\n")
            
            except EncodingStopped:
                pending_vmaf_flag = bool(pending_vmaf)
                pending_psnr_flag = bool(pending_psnr)
                if pending_vmaf_flag or pending_psnr_flag:
                    waiting_status = self._get_vmaf_waiting_status_text(pending_vmaf_flag, pending_psnr_flag)
                else:
                    waiting_status = original_status
                display_cq = callback_cq_str if callback_cq_str not in ("", None) else original_cq_str
                display_vmaf = callback_vmaf_str if callback_vmaf_str not in ("", None) else original_vmaf_str
                display_psnr = callback_psnr_str if callback_psnr_str not in ("", None) else original_psnr_str
                self.encoding_queue.put_nowait((
                    "update", item_id, waiting_status,
                    display_cq, display_vmaf, display_psnr, "-",
                    orig_size_str, callback_new_size_str, callback_size_change,
                    original_completed_date
                ))
                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                stop_msg = (
                    f"\n{t('log_immediate_stop_vmaf_interrupted').format(filename=video_path.name)}\n"
                    if STOP_EVENT.is_set()
                    else f"\n{t('log_vmaf_interrupted').format(filename=video_path.name)}\n"
                )
                with console_redirect(svt_logger):
                    print(stop_msg)
                if pending_vmaf_flag or pending_psnr_flag:
                    # LIST-BASED QUEUE: Re-add VMAF task to queue
                    self.add_to_svt_queue(
                        video_path=task.get('video_path'),
                        item_id=task.get('item_id'),
                        task_type='vmaf',
                        is_manual=True,
                        output_file=task.get('output_file'),
                        orig_size_str=task.get('orig_size_str'),
                        check_vmaf=pending_vmaf_flag,
                        check_psnr=pending_psnr_flag,
                        current_cq_str=task.get('current_cq_str'),
                        current_vmaf_str=task.get('current_vmaf_str'),
                        current_psnr_str=task.get('current_psnr_str'),
                        current_new_size_str=task.get('current_new_size_str'),
                        current_size_change=task.get('current_size_change'),
                        current_completed_date=task.get('current_completed_date'),
                        current_status=task.get('current_status'),
                        vmaf_reference_path=task.get('vmaf_reference_path'),
                        denoised_master_path=task.get('denoised_master_path'),
                        queue_front=True,
                        reason='vmaf_retry_after_interrupt'
                    )
                    try:
                        if LOG_WRITER:
                            LOG_WRITER.write(
                                f"[INFO] Interrupted VMAF task re-queued to front: {Path(video_path).name}\n"
                            )
                            LOG_WRITER.flush()
                    except Exception:
                        pass
                self.clear_encoding_times(item_id)
            
            except Exception as e:
                # Error - restore original values
                self.encoding_queue.put_nowait(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "failed"))
                self.clear_encoding_times(item_id)
                with console_redirect(svt_logger):
                    print(f"\n{t('log_vmaf_test_error').format(error=e)}\n")
        
        finally:
            # Remove from VMAF processing set
            if hasattr(self, 'vmaf_processing_videos'):
                with self.nvenc_selection_lock:
                    self.vmaf_processing_videos.discard(video_path)
            
            # AZONNALI LEÁLLÍTÁS: Zajszűrt master megőrzése későbbi újrafelhasználásra
            # Ha STOP_EVENT aktív (azonnali leállítás), NEM töröljük a mastert,
            # mert drága előállítani és újra felhasználható a következő átkódolásnál.
            denoised_master_to_cleanup = task.get('denoised_master_path')
            if STOP_EVENT.is_set():
                # Azonnali leállítás - master megőrzése
                if denoised_master_to_cleanup and hasattr(denoised_master_to_cleanup, 'exists') and denoised_master_to_cleanup.exists():
                    with console_redirect(svt_logger):
                        print(f"♻ Zajszűrt master megőrizve (azonnali leállítás): {denoised_master_to_cleanup.name}")
            else:
                # Normál befejezés - master törlése a VMAF után
                if denoised_master_to_cleanup and hasattr(denoised_master_to_cleanup, 'exists') and denoised_master_to_cleanup.exists():
                    from .core_preamble_and_imports import DEBUG_MODE
                    if DEBUG_MODE:
                        with console_redirect(svt_logger):
                            print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve (VMAF task kész): {denoised_master_to_cleanup.name}")
                    else:
                        try:
                            master_size_mb = denoised_master_to_cleanup.stat().st_size / (1024 * 1024)
                            denoised_master_to_cleanup.unlink()
                            denoised_master_to_cleanup.with_suffix('.denoise_level').unlink(missing_ok=True)
                            denoised_master_to_cleanup.with_suffix('.denoise_params').unlink(missing_ok=True)
                            with console_redirect(svt_logger):
                                print(f"[DEL] Zajszűrt mesterdarab törölve (VMAF kész): {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB felszabadítva)")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF kész, zajszűrt master takarítás | fájl: {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB)\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except (OSError, IOError, PermissionError) as e:
                            with console_redirect(svt_logger):
                                print(f"[WARN] Mesterdarab törlés hiba (VMAF task után): {e}")
            
            # Clean up video_stop_events to prevent memory leak
            with self.video_stop_events_lock:
                self.video_stop_events.pop(video_path, None)

            # Cleanup flag sync for svt_worker finally block
            # Mivel a cleanupot már fent kezeljük, a flag False marad
            task['_should_delete_master'] = False

            # Clean up logger state
            if hasattr(svt_logger, 'set_current_video_path'):
                svt_logger.set_current_video_path(None)

    # NOTE: _find_original_source_for_master removed - defined in NvencWorkerMixin (takes precedence in MRO)

    def _calculate_audio_size(self, video_path):
        """
        Kiszámítja az összes hangsáv méretét FFprobe segítségével.
        
        Args:
            video_path: Forrás videó elérési útja
        
        Returns:
            float: Hangsávok mérete MB-ban, vagy 0.0 ha nem sikerült
        """
        import subprocess
        import json
        
        try:
            result = subprocess.run(
                [
                    FFPROBE_PATH, '-v', 'error',
                    '-select_streams', 'a',
                    '-show_entries', 'stream=bit_rate,duration',
                    '-of', 'json',
                    str(video_path)
                ],
                capture_output=True,
                text=True,
                timeout=10,
                startupinfo=get_startup_info()
            )
            
            if result.returncode != 0:
                return 0.0
            
            data = json.loads(result.stdout)
            total_audio_mb = 0.0
            
            for stream in data.get('streams', []):
                bit_rate = stream.get('bit_rate')
                duration = stream.get('duration')
                
                if bit_rate and duration:
                    try:
                        # Méret = (bit_rate * duration) / 8 / 1024 / 1024
                        stream_mb = (float(bit_rate) * float(duration)) / (8 * 1024 * 1024)
                        total_audio_mb += stream_mb
                    except (ValueError, TypeError):
                        continue
            
            return total_audio_mb
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Hangsáv méret számítás hiba: {e}\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    # File or calculation error - non-critical
                    pass
            return 0.0
