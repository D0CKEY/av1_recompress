from .gui_imports import *
from .gui_shared import *

class VideoLoadingMixin:
    def _build_saved_manual_queue_status(self, status_code, saved_status="", manual_cq_value=None, manual_quality_check=None):
        """Rebuild queue status text while preserving manual CQ suffix after reload."""
        base_status = status_code_to_localized(status_code) if status_code else (saved_status or "")
        saved_status = str(saved_status or "")
        if not base_status:
            return saved_status
        if " (M " in saved_status:
            return saved_status
        if manual_cq_value is None:
            return base_status
        try:
            cq_value = int(manual_cq_value)
        except (TypeError, ValueError):
            return base_status

        if hasattr(self, '_format_manual_status_suffix'):
            try:
                return base_status + self._format_manual_status_suffix(cq_value, manual_quality_check)
            except Exception:
                pass

        quality_check = str(manual_quality_check or '').strip().upper()
        return f"{base_status} (M CQ:{cq_value}{(' ' + quality_check) if quality_check else ''})"

    def _refresh_loading_ui(self):
        """Flush pending layout/paint work without entering a nested event loop."""
        try:
            self.root.update_idletasks()
        except (tk.TclError, AttributeError):
            pass

    def format_relative_name(self, video_path):
        """Format video path relative to source directory for display.

        Args:
            video_path: Path to the video file.

        Returns:
            str: Relative path string, or absolute path if relative path cannot be computed.
        """
        try:
            if hasattr(self, 'source_path') and self.source_path:
                rel = video_path.resolve().relative_to(self.source_path.resolve())
                return str(rel)
            return str(video_path)
        except Exception as e:
            if LOG_WRITER:
                try:
                    if not LOG_WRITER.closed:
                        LOG_WRITER.write(f"[ERROR] Relative path error: {video_path} -> {e}\n")
                        LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    pass
            return str(video_path)

    def _format_frame_count_display(self, frame_count):
        """Format frame count for table display."""
        if frame_count is None:
            return "-"
        try:
            value = int(float(frame_count))
            return str(value) if value > 0 else "-"
        except (ValueError, TypeError):
            return "-"

    def _build_duration_frames_display(
        self,
        source_duration_seconds=None,
        source_frame_count=None,
        target_duration_seconds=None,
        target_frame_count=None,
        show_target=False
    ):
        """Build Duration/Frames cell strings, optionally as 'source / target'."""
        source_duration_str = format_seconds_hms(source_duration_seconds) if source_duration_seconds else "-"
        source_frames_str = self._format_frame_count_display(source_frame_count)

        if not show_target:
            return source_duration_str, source_frames_str

        target_duration_str = format_seconds_hms(target_duration_seconds) if target_duration_seconds else "-"
        target_frames_str = self._format_frame_count_display(target_frame_count)

        duration_display = (
            f"{source_duration_str} / {target_duration_str}"
            if target_duration_str != "-"
            else source_duration_str
        )
        frames_display = (
            f"{source_frames_str} / {target_frames_str}"
            if target_frames_str != "-"
            else source_frames_str
        )
        return duration_display, frames_display

    def load_videos(self):
        """Load videos from the source directory.

        Scans the source directory for video files, checks their status in the database,
        and populates the GUI treeview. Handles both cold start (no DB) and warm start.
        """
        # If Stop button is visible instead of Start, the load videos button should be inactive
        # (Button state is handled elsewhere, this is just a safety check)
        if self.is_encoding:
            # If encoding is running, loading is not allowed
            return
        if getattr(self, 'is_loading_videos', False):
            # Prevent re-entrant calls (e.g. from root.update() processing events during scan)
            return

        # Wait for any running background DB save to finish before clearing caches.
        # Without this, reloading right after a first load would find an empty/incomplete DB
        # and re-probe every single file (potentially hours of wasted ffprobe work).
        with self.db_thread_lock:
            has_active_db_threads = any(t.is_alive() for t in self.active_db_threads)
        if has_active_db_threads:
            self.status_label.config(text='Adatbázis mentés befejezésére várakozás...')
            self._refresh_loading_ui()
            wait_start = time.time()
            while True:
                with self.db_thread_lock:
                    alive = [t for t in self.active_db_threads if t.is_alive()]
                    self.active_db_threads = alive
                if not alive:
                    break
                elapsed = time.time() - wait_start
                if elapsed > 120.0:
                    # Safety timeout - don't wait forever
                    break
                self.status_label.config(text=f'Adatbázis mentés befejezésére várakozás... ({elapsed:.0f}s)')
                self._refresh_loading_ui()
                time.sleep(0.1)

        def log_file_check(msg):
            if LOG_WRITER:
                try:
                    if not LOG_WRITER.closed:
                        LOG_WRITER.write(msg + "\n")
                        LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    pass

        def build_load_error_result(video_path, error_message):
            """Helper function: return a displayable row even in case of error."""
            if video_path:
                try:
                    video_name = self.format_relative_name(video_path)
                except Exception:
                    video_name = str(video_path)
                order_num = self.video_order.get(video_path, 0) if hasattr(self, 'video_order') else 0
            else:
                video_name = "Unknown video"
                order_num = 0
            
            error_text = str(error_message) if error_message else ""
            if len(error_text) > 120:
                error_text = error_text[:117] + "..."
            status_text = f"{t('status_load_error')}: {error_text}" if error_text else t('status_load_error')
            load_debug_log(f"Error row created: {video_name} -> {status_text}")
            result = {
                'video_path': video_path,
                'order_num': order_num,
                'video_name': video_name,
                'values': ("", "0", video_name, status_text, "-", "-", "-", "-", "-", "-", "-", "-", "-", ""),
                'tag': 'failed',
                'error': error_text
            }
            if hasattr(self, 'last_load_errors'):
                try:
                    self.last_load_errors.append((video_name, status_text))
                except Exception:
                    pass
            return result
        
        source = self.source_entry.get()
        if not source or not os.path.exists(source):
            messagebox.showerror(t('msg_error'), t('msg_invalid_source'))
            return
        
        self.source_path = Path(source)
        dest = self.dest_entry.get()
        self.dest_path = Path(dest) if dest else None
        destination_is_empty = False
        if self.dest_path:
            video_loading_log(f"STEP 1.5: Checking destination dir list")
            dest_dir_start = time.time()
            destination_is_empty = is_directory_completely_empty(self.dest_path)
            dest_dir_time = (time.time() - dest_dir_start) * 1000
            video_loading_log(f"STEP 1.5 DONE: {dest_dir_time:.2f}ms - destination_is_empty={destination_is_empty}")
            if destination_is_empty:
                log_file_check("* Destination folder empty -> skipping check of existing outputs.")
                video_loading_log(f"  Destination folder empty -> skipping check of existing outputs")
        
        # Check and offer database state (only if not startup load)
        video_loading_log(f"STEP 2: DB query (load_state_from_db)")
        db_query_start = time.time()
        saved_state = self.load_state_from_db()
        db_query_time = (time.time() - db_query_start) * 1000
        video_loading_log(f"STEP 2 DONE: {db_query_time:.2f}ms - saved_state={'YES' if saved_state else 'NO'}")
        
        # DB content in JSON format to log file
        if saved_state:
            # Summary statistics
            db_summary = {
                'db_path': str(self.db_path),
                'settings': {
                    'source_path': saved_state.get('source_path'),
                    'dest_path': saved_state.get('dest_path'),
                    'min_vmaf': saved_state.get('min_vmaf'),
                    'vmaf_step': saved_state.get('vmaf_step'),
                    'max_encoded_percent': saved_state.get('max_encoded_percent'),
                    'resize_enabled': saved_state.get('resize_enabled'),
                    'resize_height': saved_state.get('resize_height'),
                    'deband_enabled': saved_state.get('deband_enabled'),
                    'force_8bit_denoised_master': saved_state.get('force_8bit_denoised_master'),
                    'audio_compression_enabled': saved_state.get('audio_compression_enabled'),
                    'audio_compression_method': saved_state.get('audio_compression_method'),
                    'auto_vmaf_psnr': saved_state.get('auto_vmaf_psnr'),
                    'svt_preset': saved_state.get('svt_preset'),
                    'nvenc_worker_count': saved_state.get('nvenc_worker_count')
                },
                'videos_count': len(saved_state.get('videos', [])),
                'videos_sample': saved_state.get('videos', [])[:10] if len(saved_state.get('videos', [])) > 10 else saved_state.get('videos', []),
                'videos_full': saved_state.get('videos', [])  # Full list
            }
            video_loading_log_json(db_summary, "Database Content (Full)")
            
            # Shorter summary too (statistics)
            stats = {
                'total_videos': len(saved_state.get('videos', [])),
                'videos_with_source_size': sum(1 for v in saved_state.get('videos', []) if v.get('orig_size_bytes')),
                'videos_with_source_timestamp': sum(1 for v in saved_state.get('videos', []) if v.get('source_modified_timestamp')),
                'videos_with_output': sum(1 for v in saved_state.get('videos', []) if v.get('output_path')),
                'videos_by_status': {}
            }
            for video in saved_state.get('videos', []):
                status = video.get('status', 'unknown')
                stats['videos_by_status'][status] = stats['videos_by_status'].get(status, 0) + 1
            video_loading_log_json(stats, "Database Statistics")
        else:
            video_loading_log_json({'db_path': str(self.db_path), 'status': 'no_data'}, "Database Content")
        
        load_saved = False
        
        if saved_state:
            # Check if source/destination paths are the same
            saved_source = saved_state.get('source_path')
            saved_dest = saved_state.get('dest_path')
            
            video_loading_log(f"  DB saved_source: {saved_source}")
            video_loading_log(f"  Current source_path: {self.source_path}")
            video_loading_log(f"  DB saved_dest: {saved_dest}")
            video_loading_log(f"  Current dest_path: {self.dest_path}")
            
            # Path comparison normalized (resolve() to make them absolute paths)
            try:
                saved_source_path = Path(saved_source).resolve() if saved_source else None
                current_source_path = self.source_path.resolve() if self.source_path else None
                source_match = saved_source_path and current_source_path and saved_source_path == current_source_path
            except Exception as e:
                video_loading_log(f"  ERROR source path comparison: {e}")
                source_match = False
            
            try:
                saved_dest_path = Path(saved_dest).resolve() if saved_dest else None
                current_dest_path = self.dest_path.resolve() if self.dest_path else None
                dest_match = (not saved_dest_path and not current_dest_path) or (saved_dest_path and current_dest_path and saved_dest_path == current_dest_path)
            except Exception as e:
                video_loading_log(f"  ERROR dest path comparison: {e}")
                dest_match = False
            
            video_loading_log(f"  source_match: {source_match} (saved={saved_source}, current={self.source_path})")
            video_loading_log(f"  dest_match: {dest_match} (saved={saved_dest}, current={self.dest_path})")
            
            # OPTIMIZATION: Even if paths don't match, load video data!
            # Because they might be the same files, just on a different path (e.g. different drive)
            # When loading video data, we compare with normalized paths, so we find them
            if source_match and dest_match:
                # If paths also match, we definitely load
                load_saved = True
                video_loading_log(f"  [OK] load_saved = True (paths match)")
            elif saved_state.get('videos'):
                # If paths don't match, BUT there are video data in DB, we still load
                # (because they might be the same files, just on a different path)
                load_saved = True
                video_loading_log(f"  [OK] load_saved = True (paths don't match, but videos exist in DB - will try to match by normalized paths)")
            else:
                video_loading_log(f"  [ERROR] load_saved = False (no videos in DB)")
        else:
            video_loading_log(f"  [ERROR] load_saved = False (no saved_state)")
        
        log_file_check(f"\n=== LOADING VIDEOS ===")
        log_file_check(f"Source: {source}")
        log_file_check(f"Destination: {self.dest_entry.get() or 'N/A'}")
        
        video_loading_log(f"=== LOADING VIDEOS START ===")
        video_loading_log(f"Source: {source}")
        video_loading_log(f"Destination: {self.dest_entry.get() or 'N/A'}")
    
        self.is_loading_videos = True
        self.last_load_errors = []
        self.update_start_button_state()
        
        # Initialize loading progress tracking for HTTP API
        self.loading_progress = {
            'phase': 'initializing',  # 'initializing', 'scanning', 'comparing', 'processing', 'finishing'
            'phase_text': t('status_initializing'),
            'total_files': 0,
            'processed_files': 0,
            'current_file': '',
            'start_time': time.time(),
            'phase_start_time': time.time(),
            'estimated_remaining_seconds': None,
            'percent': 0
        }
    
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.video_items.clear()
        self.subtitle_items.clear()
        self.video_to_output.clear()
        self.tree_item_data.clear()  # Clear data behind tree items
        self.video_stat_cache.clear()  # Clear stat cache
        
        # Cleanup stop events for removed videos
        with self.video_stop_events_lock:
            self.video_stop_events.clear()
        
        # If skip_av1_files checkbox is checked, handle .av1 files separately
        skip_av1 = self.skip_av1_files.get()
        # Capture nvenc_enabled for threads
        nvenc_enabled_val = self.nvenc_enabled.get()
        
        # Load normal video files (.av1 outputs are always skipped from encode queue)
        video_loading_log(f"STEP 1: Source dir list (batch_scan_directory)")
        dir_list_start = time.time()
        
        # Update loading progress - scanning phase
        self.loading_progress['phase'] = 'scanning'
        self.loading_progress['phase_text'] = 'Mappa keresése...'
        self.loading_progress['phase_start_time'] = time.time()
        self.status_label.config(text='Mappa keresése...')
        self.root.update_idletasks()  # Non-blocking update
        
        # Progress callback for scanning - uses after() for thread-safe GUI update
        scan_result_holder = [None]
        scan_complete_flag = [False]
        last_scan_update = [0]
        
        def scan_progress_callback(scanned_count):
            """Called from batch_scan_directory every 500 files."""
            # Store for later GUI update (called from thread)
            last_scan_update[0] = scanned_count
        
        def do_scan():
            """Run scanning in background thread."""
            try:
                result = batch_scan_directory(self.source_path, progress_callback=scan_progress_callback)
                scan_result_holder[0] = result
            except Exception as e:
                print(t('log_scan_error').format(error=e))
                scan_result_holder[0] = {}
            finally:
                scan_complete_flag[0] = True
        
        # Start scanning thread
        scan_thread = threading.Thread(target=do_scan, daemon=True)
        scan_thread.start()
        
        # Wait for scan to complete without re-entering the full Tk event loop.
        scan_wait_start = time.time()
        while not scan_complete_flag[0]:
            count = last_scan_update[0]
            if count > 0:
                self.loading_progress['phase_text'] = f'Keresés: {count:,} fájl...'
                self.status_label.config(text=f'Keresés: {count:,} fájl...')
            self._refresh_loading_ui()
            time.sleep(0.05)
            if time.time() - scan_wait_start > 300.0:
                video_loading_log(f"SCAN TIMEOUT after 300s")
                break
        
        all_files_with_stats = scan_result_holder[0] or {}
        
        def _normalize_scan_key(path_obj):
            """Normalize paths for case-insensitive cache lookups on Windows."""
            if not path_obj:
                return ""
            try:
                path_str = os.fspath(path_obj)
            except (TypeError, ValueError):
                path_str = str(path_obj)
            return path_str.replace('\\', '/').lower()
        
        source_scan_by_norm = {
            _normalize_scan_key(path_obj): stat_info
            for path_obj, stat_info in all_files_with_stats.items()
        }
        
        # Update loading progress - filtering phase
        self.loading_progress['phase'] = 'filtering'
        self.loading_progress['phase_text'] = f'Szűrés: 0/{len(all_files_with_stats):,} fájl...'
        self.status_label.config(text=f'Szűrés: 0/{len(all_files_with_stats):,} fájl...')
        self.root.update_idletasks()
        
        self.video_files = []
        total_files = len(all_files_with_stats)
        processed_filter = 0
        batch_size = 1000  # Process 1000 files per batch
        last_update = time.time()
        
        for file_path, stat_info in all_files_with_stats.items():
            processed_filter += 1
            
            # Filter for video extensions
            if file_path.suffix.lower() in VIDEO_EXTENSIONS:
                # Skip files in .ab-av1-* subfolders (ab-av1 temp files)
                path_parts = file_path.parts
                if any('.ab-av1-' in part for part in path_parts):
                    continue
                
                # Skip .av1 files unless explicitly included (default False)
                if not file_path.stem.endswith('.av1'):
                    self.video_files.append(file_path)
                    
                    # Cache stat info immediately!
                    self.video_stat_cache[file_path] = {
                        'source_size_bytes': stat_info['size'],
                        'source_modified_timestamp': stat_info['mtime']
                    }
            
            # Update GUI every batch_size files or every 200ms
            if processed_filter % batch_size == 0 or (time.time() - last_update) > 0.2:
                self.loading_progress['phase_text'] = f'Szűrés: {processed_filter:,}/{total_files:,} ({len(self.video_files):,} videó)...'
                self.status_label.config(text=f'Szűrés: {processed_filter:,}/{total_files:,} ({len(self.video_files):,} videó)...')
                self._refresh_loading_ui()
                last_update = time.time()

        # Final progress update to show 100% completion
        self.loading_progress['phase_text'] = f'Szűrés: {total_files:,}/{total_files:,} ({len(self.video_files):,} videó)...'
        self.status_label.config(text=f'Szűrés: {total_files:,}/{total_files:,} ({len(self.video_files):,} videó)...')
        self.root.update_idletasks()

        dir_list_time = (time.time() - dir_list_start) * 1000
        video_loading_log(f"STEP 1 DONE: {dir_list_time:.2f}ms - {len(self.video_files)} videos found (scanned {len(all_files_with_stats)} files)")
        
        # Destination-side batch scan (single directory walk) for fast output checks.
        # This avoids per-row exists()/stat() calls on network paths during warm load.
        dest_scan_by_norm = {}
        if self.dest_path and self.dest_path.exists():
            video_loading_log("STEP 1B: Destination dir list (batch_scan_directory)")
            dest_scan_start = time.time()
            try:
                dest_files_with_stats = batch_scan_directory(self.dest_path)
            except Exception:
                dest_files_with_stats = {}
            dest_scan_by_norm = {
                _normalize_scan_key(path_obj): stat_info
                for path_obj, stat_info in dest_files_with_stats.items()
            }
            dest_scan_time = (time.time() - dest_scan_start) * 1000
            video_loading_log(
                f"STEP 1B DONE: {dest_scan_time:.2f}ms - "
                f"{len(dest_files_with_stats)} destination files indexed"
            )
        else:
            video_loading_log("STEP 1B SKIP: Destination path unavailable")

        def _get_source_scan_info(path_obj):
            return source_scan_by_norm.get(_normalize_scan_key(path_obj))

        def _get_dest_scan_info(path_obj):
            return dest_scan_by_norm.get(_normalize_scan_key(path_obj))

        # Source-side subtitle index (single pass, directory-list based).
        # This removes per-video directory scans in find_subtitle_files() on startup.
        video_loading_log("STEP 1C: Source subtitle index build")
        subtitle_index_start = time.time()
        subtitle_index_by_dir_stem = {}
        subtitle_extensions = SUBTITLE_EXTENSIONS if 'SUBTITLE_EXTENSIONS' in globals() else {'.srt', '.vtt', '.sub', '.ass', '.ssa'}
        for src_path in all_files_with_stats.keys():
            try:
                if src_path.suffix.lower() not in subtitle_extensions:
                    continue
                base_name, lang_part = extract_language_from_filename(src_path.name)
                stem_key = (base_name or src_path.stem).strip().lower()
                index_key = (_normalize_scan_key(src_path.parent), stem_key)
                subtitle_index_by_dir_stem.setdefault(index_key, []).append((src_path, lang_part))
            except Exception:
                continue
        subtitle_index_time = (time.time() - subtitle_index_start) * 1000
        video_loading_log(
            f"STEP 1C DONE: {subtitle_index_time:.2f}ms - "
            f"{sum(len(v) for v in subtitle_index_by_dir_stem.values())} subtitles indexed "
            f"({len(subtitle_index_by_dir_stem)} groups)"
        )

        def _get_validated_subtitles_from_index(video_path):
            try:
                idx_key = (_normalize_scan_key(video_path.parent), video_path.stem.strip().lower())
            except Exception:
                return [], []
            candidates = subtitle_index_by_dir_stem.get(idx_key, [])
            if not candidates:
                return [], []
            valid_subtitles, invalid_subtitles = split_valid_invalid_subtitles(candidates)
            if invalid_subtitles:
                try:
                    self._log_invalid_subtitles(video_path, invalid_subtitles)
                except Exception:
                    pass
            return valid_subtitles, invalid_subtitles
        
        # Update loading progress - scanning complete
        self.loading_progress['total_files'] = len(self.video_files)
        self.loading_progress['phase_text'] = f'{len(self.video_files):,} videó találva'
        self.status_label.config(text=f'{len(self.video_files):,} videó találva - adatbázis ellenőrzése...')
        self.root.update_idletasks()  # Non-blocking update
        
        # If skip_av1 is checked, copy .av1 files (if destination folder exists)
        if skip_av1 and self.dest_path:
            av1_files = [
                fp for fp in all_files_with_stats.keys()
                if fp.suffix.lower() in VIDEO_EXTENSIONS
                and fp.stem.endswith('.av1')
                and not any('.ab-av1-' in part for part in fp.parts)
            ]
            
            # Copy .av1 files
            for av1_file in av1_files:
                relative_path = av1_file.relative_to(self.source_path)
                dest_file = self.dest_path / relative_path
                
                # If already exists, skip
                if dest_file.exists():
                    continue
                
                try:
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(av1_file, dest_file)
                    # Copy subtitles too
                    subtitle_files = find_subtitle_files(av1_file)
                    for sub_path, lang_part in subtitle_files:
                        dest_sub_name = dest_file.stem
                        if lang_part:
                            dest_sub_name += f".{lang_part}"
                        dest_sub_name += sub_path.suffix
                        dest_sub_path = dest_file.parent / dest_sub_name
                        if not dest_sub_path.exists():
                            shutil.copy2(sub_path, dest_sub_path)
                except Exception as e:
                    log_file_check(f"[ERROR] Error copying .av1 file ({av1_file.name}): {e}")
                    print(t('log_copy_av1_error').format(filename=av1_file.name, error=e))
        
        log_file_check(f"Talált videófájlok száma: {len(self.video_files)} (skip_av1={self.skip_av1_files.get()})")
    
        if not self.video_files:
            log_file_check("[ERROR] No loadable videos in source folder.")
            messagebox.showinfo(t('msg_info'), t('msg_no_video'))
            self.is_loading_videos = False
            self.update_start_button_state()
            return
        
        # Sort alphabetically by relative path (constant order)
        # Update progress for sorting phase
        self.loading_progress['phase'] = 'sorting'
        self.loading_progress['phase_text'] = f'{len(self.video_files):,} videó rendezése...'
        self.status_label.config(text=f'{len(self.video_files):,} videó rendezése...')
        self._refresh_loading_ui()
        
        # FAST SORTING: Use string operations instead of slow Path.resolve()
        # Pre-compute source path string for fast prefix removal
        source_str = str(self.source_path).lower().rstrip('/\\')
        source_len = len(source_str)
        
        def fast_sort_key(video_path):
            """Fast sort key using string operations (no I/O!)"""
            path_str = str(video_path).lower()
            # Remove source prefix to get relative path
            if path_str.startswith(source_str):
                return path_str[source_len:].lstrip('/\\')
            return path_str
        
        # Sort directly - no background thread needed, this is now FAST!
        sort_start = time.time()
        self.video_files.sort(key=fast_sort_key)
        sort_time = time.time() - sort_start
        
        # Log sort time
        if sort_time > 0.5:
            video_loading_log(f"SORT: {len(self.video_files):,} videos sorted in {sort_time:.2f}s")
        
        # Set order numbers in alphabetical order (constant, does not change)
        # This is fast now - just dict assignment
        self.loading_progress['phase_text'] = f'Sorszámozás...'
        self.status_label.config(text=f'Sorszámozás...')
        self._refresh_loading_ui()
        
        self.video_order = {}
        total_videos_for_order = len(self.video_files)
        last_order_update = time.time()
        
        for idx, video_path in enumerate(self.video_files, 1):
            self.video_order[video_path] = idx
            # Update GUI every 10000 items or every 200ms
            if idx % 10000 == 0 or (time.time() - last_order_update) > 0.2:
                self.loading_progress['phase_text'] = f'Sorszámozás: {idx:,}/{total_videos_for_order:,}...'
                self.status_label.config(text=f'Sorszámozás: {idx:,}/{total_videos_for_order:,}...')
                self._refresh_loading_ui()
                last_order_update = time.time()
        
        # Preparing probing (always necessary)
        video_loading_log(f"STEP 3: Comparing data and preparing processing")
        video_loading_log(f"  load_saved={load_saved}, saved_state={'YES' if saved_state else 'NO'}")
        
        # Update loading progress - comparing phase
        self.loading_progress['phase'] = 'comparing'
        self.loading_progress['phase_text'] = 'Adatbázis összehasonlítása...'
        self.loading_progress['phase_start_time'] = time.time()
        self.status_label.config(text=f'{len(self.video_files):,} videó - adatbázis összehasonlítása...')
        self.root.update_idletasks()  # Non-blocking update
        
        comparison_start = time.time()
        saved_videos = {}
        saved_videos_by_name = {}  # Fallback: can also search by name
        saved_videos_by_path_normalized = {}  # NEW: normalized (lowercase) path string keys for Windows compatibility
        if load_saved and saved_state:
            videos_list = saved_state.get('videos', [])
            video_loading_log(f"  Received {len(videos_list)} video data from DB")
            
            # Build saved_videos dict in background thread
            db_result_holder = [None, None]  # [saved_videos, saved_videos_by_name]
            db_complete_flag = [False]
            db_progress = [0]
            total_db_videos = len(videos_list)
            
            def build_saved_videos_dict():
                """Build saved_videos dict in background thread."""
                sv = {}
                sv_by_name = {}
                sv_by_path_normalized = {}  # NEW: normalized (lowercase) path string keys
                for idx, v in enumerate(videos_list, 1):
                    try:
                        video_path_str = v.get('video_path')
                        if video_path_str:
                            # Use Path directly - NO resolve()! (resolve is slow on network drives)
                            video_path_db = Path(video_path_str)
                            sv[video_path_db] = v
                            # NEW: Also index by normalized path string (lowercase, forward slashes)
                            # This helps with Windows case-insensitive path matching
                            normalized_path_str = str(video_path_db).lower().replace('\\', '/')
                            sv_by_path_normalized[normalized_path_str] = v
                            # Fallback: index by name too (relative path or filename) - also normalized
                            try:
                                video_name_db = v.get('video_name') or Path(video_path_str).name
                                if video_name_db:
                                    sv_by_name[video_name_db.lower()] = v  # Lowercase for case-insensitive matching
                            except Exception:
                                pass
                    except Exception as e:
                        video_loading_log(f"  ERROR converting video_path to Path: {video_path_str} -> {e}")
                    
                    db_progress[0] = idx
                
                db_result_holder[0] = sv
                db_result_holder[1] = sv_by_name
                db_result_holder.append(sv_by_path_normalized)  # NEW: index 2 = normalized path dict
                db_complete_flag[0] = True
            
            # Start background thread
            self._start_db_thread(build_saved_videos_dict, name="BuildSavedVideosDictDB", daemon=True)
            
            # Update GUI while waiting without nested event processing
            last_db_update = time.time()
            db_wait_start = time.time()
            while not db_complete_flag[0]:
                current_progress = db_progress[0]
                if current_progress > 0 and (time.time() - last_db_update) > 0.15:
                    self.loading_progress['phase_text'] = f'Adatbázis: {current_progress:,}/{total_db_videos:,}...'
                    self.status_label.config(text=f'Adatbázis: {current_progress:,}/{total_db_videos:,}...')
                    last_db_update = time.time()
                self._refresh_loading_ui()
                time.sleep(0.05)
                if time.time() - db_wait_start > 120.0:
                    video_loading_log(f"DB dict build timeout after 120s")
                    break
            
            saved_videos = db_result_holder[0] or {}
            saved_videos_by_name = db_result_holder[1] or {}
            saved_videos_by_path_normalized = db_result_holder[2] if len(db_result_holder) > 2 else {}  # NEW: normalized path lookup
            
            video_loading_log(f"  {len(saved_videos)} video data converted to Path objects")
            video_loading_log(f"  {len(saved_videos_by_name)} video data indexed by name (fallback, lowercase)")
            video_loading_log(f"  {len(saved_videos_by_path_normalized)} video data indexed by normalized path (fallback)")
            
            # Test: check if first few videos are found
            if self.video_files and saved_videos:
                test_count = min(5, len(self.video_files))
                video_loading_log(f"  Test: checking video match for first {test_count} videos:")
                for i, video_path in enumerate(list(self.video_files)[:test_count]):
                    try:
                        normalized_video_path = video_path.resolve()
                        found = normalized_video_path in saved_videos
                        video_loading_log(f"    [{i+1}] {video_path.name[:50]}... -> {'[OK] FOUND' if found else '[ERROR] NOT FOUND'}")
                        if not found:
                            # Try to compare with paths in DB too
                            for db_path in list(saved_videos.keys())[:10]:  # Check only first 10
                                try:
                                    if db_path.resolve() == normalized_video_path:
                                        video_loading_log(f"      -> [OK] FOUND (normalized path match with {db_path})")
                                        found = True
                                        break
                                except Exception:
                                    pass
                    except Exception as e:
                        video_loading_log(f"    [{i+1}] ERROR: {e}")
        else:
            video_loading_log(f"  [ERROR] Nincs saved_videos (load_saved={load_saved}, saved_state={'YES' if saved_state else 'NO'})")
        comparison_time = (time.time() - comparison_start) * 1000
        video_loading_log(f"STEP 3 DONE: {comparison_time:.2f}ms - {len(saved_videos)} video data ready for use")
        
        # A globális beállítások visszaállítását itt kihagyjuk, mert felülírná a felhasználó
        # futás közben esetleg módosított beállításait.
        # A beállítások visszaállítása csak programindításkor (check_and_offer_state_load)
        # történik, felhasználói megerősítés után.
        
            
            # Do not use order numbers loaded from JSON, because alphabetical order is constant
            # video_files is already in alphabetical order, and video_order is also set in alphabetical order
            # This ensures that the same file always has the same order number
        
        # Parallel video data collection helper function
        def process_video_data(video_path):
            """
            Collect all data for a video in parallel
            """
            if LOAD_DEBUG:
                log_file_check(f"DEBUG: process_video_data started for {video_path}")
            def _safe_int(value):
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return None

            def _safe_float(value):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            def _has_valid_source_probe_data(frames, duration):
                frames_int = _safe_int(frames)
                duration_float = _safe_float(duration)
                return (
                    frames_int is not None and frames_int > 0 and
                    duration_float is not None and duration_float > 0
                )

            def _has_failed_source_probe_marker(frames, duration, fps):
                frames_int = _safe_int(frames)
                duration_float = _safe_float(duration)
                fps_float = _safe_float(fps)
                return (
                    frames_int == 0 and
                    (
                        (duration_float is not None and duration_float == 0.0) or
                        (fps_float is not None and fps_float == 0.0)
                    )
                )

            def _missing_output_probe_cache(saved_video_row, status_code):
                output_frames = _safe_int(saved_video_row.get('output_frame_count'))
                output_duration = _safe_float(saved_video_row.get('output_duration_seconds'))
                if output_frames is not None and output_frames > 0 and output_duration is not None and output_duration > 0:
                    return False

                # For completed_copy we can fall back to source probe data.
                if status_code == 'completed_copy':
                    source_frames = _safe_int(saved_video_row.get('source_frame_count'))
                    source_duration = _safe_float(saved_video_row.get('source_duration_seconds'))
                    if source_frames is not None and source_frames > 0 and source_duration is not None and source_duration > 0:
                        return False

                return True

            def _infer_saved_status_code(saved_video_row):
                status_code = saved_video_row.get('status_code')
                if not status_code:
                    status_code = normalize_status_to_code(saved_video_row.get('status', ''))
                if status_code:
                    return status_code

                # Recovery path for legacy/corrupted rows where status/status_code is missing
                # but completed output metadata exists.
                has_output_meta = (
                    saved_video_row.get('output_file_size_bytes') is not None and
                    saved_video_row.get('output_modified_timestamp') is not None
                )
                if has_output_meta:
                    enc = str(saved_video_row.get('output_encoder_type') or '').lower()
                    if enc == 'nvenc':
                        return 'completed_nvenc'
                    if enc in ('svt-av1', 'svt'):
                        return 'completed_svt'
                    return 'completed_exists'
                return status_code

            def _format_completed_date_from_timestamp(raw_ts):
                if raw_ts in (None, "", "-"):
                    return ""
                try:
                    ts = float(raw_ts)
                    if ts <= 0:
                        return ""
                    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                except (TypeError, ValueError, OSError):
                    return ""

            def _denoise_level_to_display(denoise_level):
                return denoise_level_to_display(denoise_level)

            def _get_copy_output_path(src_video_path):
                if not self.source_path or not self.dest_path:
                    return None
                try:
                    return get_copy_filename(src_video_path, self.source_path, self.dest_path)
                except Exception:
                    return None

            def _find_existing_output_with_fallback(src_video_path, preferred_output_file):
                candidates = []

                def _add_candidate(path_obj):
                    if not path_obj:
                        return
                    try:
                        candidate = Path(path_obj)
                    except (TypeError, ValueError, OSError):
                        return
                    if candidate in candidates:
                        return
                    candidates.append(candidate)

                _add_candidate(preferred_output_file)
                if self.dest_path:
                    try:
                        _add_candidate(get_output_filename(src_video_path, self.source_path, self.dest_path))
                    except Exception:
                        pass
                    _add_candidate(_get_copy_output_path(src_video_path))

                for candidate in candidates:
                    scan_info = _get_dest_scan_info(candidate)
                    if scan_info:
                        return candidate, scan_info
                return preferred_output_file, None

            result = {
                'video_path': video_path,
                'order_num': self.video_order.get(video_path, 0),
                'video_name': None,
                'exists': False,
                'source_size_bytes': None,
                'source_frame_count': None,
                'source_duration_seconds': None,
                'output_file': None,
                'output_exists': False,
                'output_info': None,
                'saved_video': None,  # Find later normalized
                'subtitle_files': [],
                'values': None,
                'tag': None,
                'error': None,
                'source_modified_timestamp': None,
                'denoise_enabled': None,
                'hard_rotate_degrees': 0
            }
            
            # Pre-fill from cache if available (optimization)
            cached_stat = self.video_stat_cache.get(video_path)
            if cached_stat:
                result['source_size_bytes'] = cached_stat.get('source_size_bytes')
                result['source_modified_timestamp'] = cached_stat.get('source_modified_timestamp')
            
            try:
                # Video name - use fast string operations instead of resolve()
                try:
                    # Fast relative path calculation using strings
                    source_str = str(self.source_path).lower().rstrip('/\\')
                    video_str = str(video_path)
                    if video_str.lower().startswith(source_str):
                        rel_path = video_str[len(source_str):].lstrip('/\\')
                        result['video_name'] = rel_path
                    else:
                        result['video_name'] = str(video_path)
                except Exception:
                    result['video_name'] = str(video_path)
                video_loading_log(f"  video_name: {result['video_name']}")
                
                # Search for saved video - use multiple fallback methods for Windows compatibility
                try:
                    # Prepare normalized path string for fallback lookups
                    normalized_video_path_str = str(video_path).lower().replace('\\', '/')
                    
                    # Try direct lookup first (most common case)
                    result['saved_video'] = saved_videos.get(video_path)
                    if not result['saved_video']:
                        # Fallback 1: try by normalized path string (lowercase, forward slashes)
                        # This handles Windows case-insensitive path matching and backslash/forward slash differences
                        result['saved_video'] = saved_videos_by_path_normalized.get(normalized_video_path_str)
                        if result['saved_video']:
                            video_loading_log(f"  Found saved_video by normalized path (fallback 1)")
                            if LOAD_DEBUG:
                                print(f"  [DB MATCH] {video_path.name}: Megtalálva normalizált útvonallal")
                    if not result['saved_video']:
                        # Fallback 2: try by video_name (lowercase)
                        video_name_lower = result['video_name'].lower() if result['video_name'] else ''
                        result['saved_video'] = saved_videos_by_name.get(video_name_lower)
                        if result['saved_video']:
                            video_loading_log(f"  Found saved_video by name lowercase (fallback 2)")
                            if LOAD_DEBUG:
                                print(f"  [DB MATCH] {video_path.name}: Megtalálva név alapján (lowercase)")
                    if result['saved_video']:
                        video_loading_log(f"  Found saved_video in DB")
                        # Print saved video status for debugging
                        sv_status = result['saved_video'].get('status', result['saved_video'].get('status_code', 'unknown'))
                        if LOAD_DEBUG:
                            print(f"  [DB] {video_path.name}: DB státusz = '{sv_status}'")
                    else:
                        video_loading_log(f"  NOT FOUND in DB: path={video_path}, normalized={normalized_video_path_str}, name={result['video_name']}")
                        if LOAD_DEBUG:
                            print(f"  [DB MISS] {video_path.name}: NINCS az adatbázisban -> cold start ellenőrzés...")
                except Exception as e:
                    video_loading_log(f"  ERROR looking up saved_video: {e}")
                    if LOAD_DEBUG:
                        print(f"  [DB ERROR] {video_path.name}: {e}")
                    # Last resort fallback
                    video_name_lower = result['video_name'].lower() if result['video_name'] else ''
                    result['saved_video'] = saved_videos_by_name.get(video_name_lower)
                
                # Exists check - optimized: only check if no saved video (fast load)
                saved_video_check = result['saved_video']
                source_scan_info = _get_source_scan_info(video_path)
                if saved_video_check:
                    # If saved video exists, assume file exists (fast)
                    result['exists'] = True
                else:
                    # Source existence check from directory snapshot (no per-file stat/exists).
                    if not source_scan_info:
                        result['values'] = ("", "0", result['video_name'], t('status_source_missing'), "-", "-", "-", "-", "-", "-", "-", "-", "-", "")
                        result['tag'] = "failed"
                        return result
                    result['exists'] = True
                
                # Cold start detection: if completed output already exists in destination
                # This handles two cases:
                # 1. Same size as source = copied file (completed_copy)
                # 2. Different size (smaller) = AV1 encoded file (completed_exists)
                if self.dest_path and not result['saved_video']:
                    try:
                        # Get source size and mtime (if not yet available)
                        if result['source_size_bytes'] is None or result['source_modified_timestamp'] is None:
                            if source_scan_info:
                                result['source_size_bytes'] = source_scan_info.get('size')
                                result['source_modified_timestamp'] = source_scan_info.get('mtime')
                            else:
                                stat_res = video_path.stat()
                                result['source_size_bytes'] = stat_res.st_size
                                result['source_modified_timestamp'] = stat_res.st_mtime
                            
                        # Search for potential .av1.mkv file in destination
                        # Based on relative path, but with .av1.mkv extension
                        rel_path = video_path.relative_to(self.source_path)
                        dest_av1_path = self.dest_path / rel_path.parent / (video_path.stem + ".av1.mkv")
                        dest_copy_path = _get_copy_output_path(video_path)

                        if dest_copy_path:
                            dest_copy_scan_info = _get_dest_scan_info(dest_copy_path)
                            if dest_copy_scan_info:
                                dest_copy_size = dest_copy_scan_info.get('size')
                                dest_copy_mtime = dest_copy_scan_info.get('mtime')
                                if dest_copy_size == result['source_size_bytes']:
                                    fake_saved_video = {
                                        'video_path': str(video_path),
                                        'output_path': str(dest_copy_path),
                                        'status': 'completed_copy',
                                        'status_code': 'completed_copy',
                                        'orig_size_bytes': result['source_size_bytes'],
                                        'source_modified_timestamp': result['source_modified_timestamp'],
                                        'output_file_size_bytes': dest_copy_size,
                                        'output_modified_timestamp': dest_copy_mtime,
                                        'size_change': '0.0%',
                                        'new_size': format_size_mb(dest_copy_size),
                                        'new_size_bytes': dest_copy_size,
                                        'denoise_enabled': 0,
                                        'hard_rotate_degrees': 0
                                    }
                                    result['saved_video'] = fake_saved_video
                                    result['output_file'] = dest_copy_path
                                    result['exists'] = True
                                    video_loading_log(f"  Cold start: found copied output with original extension -> {dest_copy_path.name}")
                        
                        if LOAD_DEBUG:
                            print(f"  [COLD START] Ellenőrzés: {dest_av1_path}")
                        
                        dest_av1_scan_info = _get_dest_scan_info(dest_av1_path) if not result['saved_video'] else None
                        if dest_av1_scan_info:
                            dest_size = dest_av1_scan_info.get('size')
                            dest_mtime = dest_av1_scan_info.get('mtime')
                            
                            if LOAD_DEBUG:
                                print(f"  [COLD START] [OK] LÉTEZIK: {dest_av1_path.name} ({dest_size/1024/1024:.1f} MB) - forrás: {result['source_size_bytes']/1024/1024:.1f} MB")
                            video_loading_log(f"  Cold start: Found {dest_av1_path.name} ({dest_size} bytes) - source was {result['source_size_bytes']} bytes")
                            
                            if dest_size == result['source_size_bytes']:
                                # CASE 1: Size matches source = copied file (not encoded, just renamed/copied)
                                video_loading_log(f"  Cold start match: same size as source -> treating as completed_copy")
                                
                                # 1. Rename to original name (with extension) in destination
                                # Original name in destination:
                                dest_original_name_path = self.dest_path / rel_path
                                dest_original_scan_info = _get_dest_scan_info(dest_original_name_path)
                                original_name_exists = dest_original_scan_info is not None
                                
                                # If target file (with original name) does not exist yet, rename it
                                if not original_name_exists:
                                    try:
                                        dest_av1_path.rename(dest_original_name_path)
                                        video_loading_log(f"    Renamed {dest_av1_path.name} -> {dest_original_name_path.name}")
                                        
                                        # 2. Copy subtitles too
                                        subs_copied = copy_external_subtitles(video_path, dest_original_name_path)
                                        video_loading_log(f"    Copied {subs_copied} subtitle files")
                                        
                                        # 3. Set status to "Completed (copied)"
                                        # Create a fake saved_video object for this
                                        fake_saved_video = {
                                            'video_path': str(video_path),
                                            'output_path': str(dest_original_name_path),
                                            'status': 'completed_copy', # Completed (copied)
                                            'orig_size_bytes': result['source_size_bytes'],
                                            'source_modified_timestamp': result['source_modified_timestamp'],
                                            'output_file_size_bytes': dest_size,
                                            'output_modified_timestamp': dest_original_name_path.stat().st_mtime,
                                            'size_change': '0.0%',
                                            'new_size': format_size_mb(dest_size),
                                            'denoise_enabled': 0,
                                            'hard_rotate_degrees': 0
                                        }
                                        result['saved_video'] = fake_saved_video
                                        result['exists'] = True
                                        video_loading_log(f"    Created fake saved_video entry with status 'completed_copy'")
                                        
                                    except OSError as e:
                                        video_loading_log(f"    Error renaming/copying: {e}")
                                else:
                                    video_loading_log(f"    Target file {dest_original_name_path.name} already exists, skipping rename")
                                    # If original named file already exists, still mark as completed if size matches
                                    original_size = dest_original_scan_info.get('size') if dest_original_scan_info else None
                                    if original_size is None:
                                        try:
                                            original_size = dest_original_name_path.stat().st_size
                                        except (OSError, PermissionError):
                                            original_size = None
                                    if original_size == result['source_size_bytes']:
                                         fake_saved_video = {
                                            'video_path': str(video_path),
                                            'output_path': str(dest_original_name_path),
                                            'status': 'completed_copy',
                                            'orig_size_bytes': result['source_size_bytes'],
                                            'source_modified_timestamp': result['source_modified_timestamp'],
                                            'output_file_size_bytes': dest_size,
                                            'output_modified_timestamp': dest_original_name_path.stat().st_mtime,
                                            'size_change': '0.0%',
                                            'new_size': format_size_mb(dest_size),
                                            'denoise_enabled': 0,
                                            'hard_rotate_degrees': 0
                                        }
                                         result['saved_video'] = fake_saved_video
                                         video_loading_log(f"    Target exists and matches size, treating as completed_copy")
                            
                            elif dest_size > 1024 * 1024:  # At least 1MB = likely a valid encoded video
                                # CASE 2: Size differs and file is reasonably large = likely AV1 encoded
                                # This is the NEW cold start detection for already-encoded videos!
                                video_loading_log(f"  Cold start: Different size ({dest_size} bytes vs {result['source_size_bytes']} bytes) -> treating as completed_exists")
                                
                                # Calculate size change percentage
                                try:
                                    change_percent = ((dest_size - result['source_size_bytes']) / result['source_size_bytes']) * 100
                                    change_percent_str = f"{change_percent:+.2f}%"
                                except (ZeroDivisionError, TypeError):
                                    change_percent_str = "-"
                                
                                # Create fake saved_video for the already-encoded file
                                fake_saved_video = {
                                    'video_path': str(video_path),
                                    'output_path': str(dest_av1_path),
                                    'status': 'completed_exists',  # Completed (already exists)
                                    'status_code': 'completed_exists',
                                    'orig_size_bytes': result['source_size_bytes'],
                                    'source_modified_timestamp': result['source_modified_timestamp'],
                                    'output_file_size_bytes': dest_size,
                                    'output_modified_timestamp': dest_mtime,
                                    'size_change': change_percent_str,
                                    'new_size': format_size_mb(dest_size),
                                    'new_size_bytes': dest_size,
                                    'denoise_enabled': 0,
                                    'hard_rotate_degrees': 0
                                }
                                result['saved_video'] = fake_saved_video
                                result['exists'] = True
                                result['output_file'] = dest_av1_path  # Set output file to the existing one
                                video_loading_log(f"    Created fake saved_video entry with status 'completed_exists'")
                            else:
                                video_loading_log(f"  Cold start: File too small ({dest_size} bytes), likely incomplete - ignoring")
                                
                    except Exception as e:
                        video_loading_log(f"  Error in cold start detection: {e}")
                
                # Output file path
                result['output_file'] = get_output_filename(video_path, self.source_path, self.dest_path)
                
                # Saved video handling
                saved_video = result['saved_video']
                if saved_video:
                    video_loading_log(f"  HAS saved_video in DB")
                    hard_rotate_val = normalize_hard_rotate_degrees(saved_video.get('hard_rotate_degrees', 0))
                    result['hard_rotate_degrees'] = hard_rotate_val
                    saved_video['hard_rotate_degrees'] = hard_rotate_val
                    # Custom output path
                    saved_output_path = saved_video.get('output_path')
                    if saved_output_path:
                        try:
                            result['output_file'] = Path(saved_output_path)
                            video_loading_log(f"  saved output_path: {saved_output_path}")
                        except (OSError, ValueError):
                            pass
                    
                    # Source size and date check - optimized: single stat() call
                    saved_source_size_bytes = saved_video.get('orig_size_bytes')
                    saved_source_modified_timestamp = saved_video.get('source_modified_timestamp')
                    saved_status_code_for_source = saved_video.get('status_code')
                    if not saved_status_code_for_source:
                        saved_status_for_source = saved_video.get('status', '')
                        saved_status_code_for_source = normalize_status_to_code(saved_status_for_source)
                    
                    video_loading_log(f"  saved: size={saved_source_size_bytes}, date={saved_source_modified_timestamp}, status={saved_status_code_for_source}")
                    
                    # Fast check: if saved data exists (size AND date), check with single stat() call
                    source_size_current = None
                    source_modified_current = None
                    file_size_matches = False
                    file_date_matches = False
                    should_skip_probe = False
                    
                    if saved_source_size_bytes is not None and saved_source_modified_timestamp is not None:
                        # Saved size AND date exist: fast stat() check (single call)
                        video_loading_log(f"  Checking: saved size AND date exist, doing stat()")
                        try:
                            # Use cached values
                            source_size_current = result['source_size_bytes']
                            source_modified_current = result['source_modified_timestamp']
                            
                            # Format dates for human-readable output
                            saved_date_str = datetime.fromtimestamp(saved_source_modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: {saved_source_size_bytes:,} bytes")
                            video_loading_log(f"  File date: {current_date_str} | DB date: {saved_date_str}")
                            
                            # Comparison: if both match, use saved data (fast load)
                            file_size_matches = (saved_source_size_bytes == source_size_current)
                            # Date comparison: exact timestamp match
                            file_date_matches = (saved_source_modified_timestamp == source_modified_current)
                            
                            # ONLY skip probe if BOTH (size AND date) match
                            if file_size_matches and file_date_matches:
                                # Check if we have valid probe data in DB (frame count and duration)
                                saved_frames = saved_video.get('source_frame_count')
                                saved_dur = saved_video.get('source_duration_seconds')
                                saved_fps = saved_video.get('source_fps')
                                has_valid_data = _has_valid_source_probe_data(saved_frames, saved_dur)
                                has_failed_marker = _has_failed_source_probe_marker(saved_frames, saved_dur, saved_fps)

                                if has_valid_data or has_failed_marker:
                                    # Both match and data is either valid or a cached failed probe marker.
                                    result['source_size_bytes'] = source_size_current
                                    should_skip_probe = True
                                    if has_failed_marker:
                                        video_loading_log('  SKIP PROBE: File size/date match and previous probe failure marker exists - skipping repeated failed ffprobe')
                                    else:
                                        video_loading_log(f'  SKIP PROBE: File size = DB size ({source_size_current:,} bytes) AND File date = DB date ({current_date_str}) - No probing needed')
                                else:
                                    # Cache miss: size/date match but source probe fields are missing.
                                    # Force one probe to populate cache (e.g. cold start after DB reset).
                                    result['source_size_bytes'] = source_size_current
                                    should_skip_probe = False
                                    video_loading_log('  NEED PROBE: File size/date match but source probe cache is missing')
                            else:
                                # Size or date mismatch: update values and probe needed
                                result['source_size_bytes'] = source_size_current
                                should_skip_probe = False
                                if not file_size_matches and not file_date_matches:
                                    video_loading_log(f"  [ERROR] NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes) AND File date changed ({current_date_str} != {saved_date_str})")
                                elif not file_size_matches:
                                    video_loading_log(f"  [ERROR] NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes)")
                                else:
                                    video_loading_log(f"  [ERROR] NEED PROBE: File date changed ({current_date_str} != {saved_date_str})")
                        except (OSError, PermissionError, TypeError, ValueError) as e:
                            # File not accessible: use saved values (fast)
                            result['source_size_bytes'] = saved_source_size_bytes
                            should_skip_probe = True
                            video_loading_log(f"  ERROR stat(): {e}, using saved data")
                    elif saved_source_size_bytes is not None:
                        # Only saved size exists (no date): strict size+date policy forces probe
                        video_loading_log(f"  Checking: saved size exists (no date in DB), doing stat()")
                        try:
                            # Use cached values
                            source_size_current = result['source_size_bytes']
                            source_modified_current = result['source_modified_timestamp']
                            
                            # Format date for human-readable output
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: {saved_source_size_bytes:,} bytes")
                            video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                            
                            file_size_matches = (saved_source_size_bytes == source_size_current)
                            result['source_size_bytes'] = source_size_current
                            result['source_modified_timestamp'] = source_modified_current  # For caching
                            should_skip_probe = False
                            if file_size_matches:
                                video_loading_log('  NEED PROBE: DB missing source date, strict size+date policy requires probe')
                            else:
                                video_loading_log(f'  NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes)')
                        except (OSError, PermissionError, TypeError, ValueError) as e:
                            result['source_size_bytes'] = saved_source_size_bytes
                            should_skip_probe = True
                            video_loading_log(f"  ERROR stat(): {e}, using saved data")
                    else:
                        # No saved data: read from file (rare case)
                        video_loading_log(f"  No saved data in DB, reading from file")
                        try:
                            # Use cached values
                            source_size_current = result['source_size_bytes']
                            source_modified_current = result['source_modified_timestamp']
                            result['source_size_bytes'] = source_size_current
                            result['source_modified_timestamp'] = source_modified_current  # For caching
                            should_skip_probe = False
                            
                            # Format date for human-readable output
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: (not available)")
                            video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                            video_loading_log(f"  [ERROR] NEED PROBE: New video (no data in DB)")
                        except (OSError, PermissionError, TypeError, ValueError) as e:
                            result['source_size_bytes'] = None
                            should_skip_probe = False
                            video_loading_log(f"  ERROR stat(): {e}")
                else:
                    video_loading_log(f"  NO saved_video in DB (new video)")
                    # No saved video, new video - probe needed
                    should_skip_probe = False
                    try:
                        # Use cached values
                        result['source_size_bytes'] = result['source_size_bytes'] # Already set
                        result['source_modified_timestamp'] = result['source_modified_timestamp'] # Already set
                        
                        # Format date for human-readable output
                        current_date_str = datetime.fromtimestamp(result['source_modified_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                        
                        video_loading_log(f"  File size: {result['source_size_bytes']:,} bytes | DB size: (not available)")
                        video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                        video_loading_log(f"  [ERROR] NEED PROBE: New video (no data in DB)")
                    except (OSError, PermissionError, TypeError, ValueError) as e:
                        result['source_size_bytes'] = None
                        video_loading_log(f"  ERROR stat(): {e}")

                # Probe results: use saved values if file not modified (fast load)
                if saved_video:
                    saved_frame_count = saved_video.get('source_frame_count')
                    saved_duration = saved_video.get('source_duration_seconds')
                    saved_fps = saved_video.get('source_fps')
                    
                    if should_skip_probe:
                        # File not modified: use saved values (fast) - NO probing
                        result['source_frame_count'] = saved_frame_count
                        result['source_duration_seconds'] = saved_duration
                        if saved_fps is not None:
                            result['source_fps'] = saved_fps
                        video_loading_log(f"  Using saved probe data: frames={saved_frame_count}, duration={saved_duration}, fps={saved_fps}")
                    else:
                        # File modified or no saved data: probe needed (rare case)
                        video_loading_log(f"  PROBING source video (file changed or no saved data)")
                        try:
                            probe_start = time.time()
                            duration, fps = get_video_info(video_path)
                            probe_time = (time.time() - probe_start) * 1000
                            if duration and fps:
                                result['source_frame_count'] = int(duration * fps)
                                result['source_duration_seconds'] = duration
                                result['source_fps'] = fps
                                video_loading_log(f"  Probe took {probe_time:.2f}ms: duration={duration}, fps={fps}, frames={result.get('source_frame_count')}")
                            else:
                                # Probe attempted but failed/incomplete: cache marker to avoid repeated probes.
                                result['source_frame_count'] = 0
                                result['source_duration_seconds'] = duration if (duration and duration > 0) else 0.0
                                result['source_fps'] = fps if (fps and fps > 0) else 0.0
                                video_loading_log(f"  Probe incomplete/failed ({probe_time:.2f}ms): duration={duration}, fps={fps}. Caching failure marker.")
                        except Exception as e:
                            # Fallback to saved values
                            result['source_frame_count'] = saved_frame_count
                            result['source_duration_seconds'] = saved_duration
                            if saved_fps is not None:
                                result['source_fps'] = saved_fps
                            # If no saved probe data at all, cache failure marker to avoid repeated failed probes
                            if saved_frame_count is None and saved_duration is None and saved_fps is None:
                                result['source_frame_count'] = 0
                                result['source_duration_seconds'] = 0.0
                                result['source_fps'] = 0.0
                            video_loading_log(f"  Probe ERROR: {e}, using saved data")
                    
                    # Process saved video data (similar to original code)
                    # First try to calculate from source_size_bytes
                    if result['source_size_bytes']:
                        orig_size_str = format_size_mb(result['source_size_bytes'])
                    else:
                        # If no source_size_bytes, try using saved orig_size string
                        saved_orig_size = saved_video.get('orig_size', '-')
                        if saved_orig_size != "-" and "MB" in saved_orig_size:
                            try:
                                size_num = float(saved_orig_size.replace("MB", "").strip())
                                orig_size_str = f"{format_localized_number(size_num, decimals=1)} MB"
                            except (ValueError, TypeError):
                                orig_size_str = saved_orig_size
                        else:
                            orig_size_str = saved_orig_size
                        
                        # If still no value, try using previously read source_size_current
                        if (orig_size_str == "-" or not orig_size_str) and source_size_current is not None:
                            result['source_size_bytes'] = source_size_current
                            orig_size_str = format_size_mb(source_size_current)
                        elif orig_size_str == "-" or not orig_size_str:
                            # Last chance: read from file (rare case if not yet read)
                            try:
                                file_size_bytes = result['source_size_bytes']
                                result['source_size_bytes'] = file_size_bytes
                                orig_size_str = format_size_mb(file_size_bytes)
                            except (OSError, PermissionError):
                                pass
                    
                    # Output file check - optimized: single stat() call if needed
                    if destination_is_empty:
                        result['output_exists'] = False
                        result['output_size_matches'] = False
                    else:
                        saved_status_code_check = _infer_saved_status_code(saved_video)
                        
                        saved_modified = saved_video.get('output_modified_timestamp')
                        saved_file_size = saved_video.get('output_file_size_bytes')
                        
                        # Optimization: if saved data exists (size AND date), check with single stat() call
                        if saved_status_code_check in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                            # Completed status: if saved data exists, check (fast stat())
                            if saved_modified is not None and saved_file_size is not None:
                                # Saved size AND date exist: fast stat() check (single call)
                                resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                                if output_scan_info:
                                    result['output_file'] = resolved_output_file
                                    output_size_current = output_scan_info.get('size')
                                    output_modified_current = output_scan_info.get('mtime')
                                    
                                    # Format dates for human-readable output
                                    saved_date_str = datetime.fromtimestamp(saved_modified).strftime('%Y-%m-%d %H:%M:%S')
                                    current_date_str = datetime.fromtimestamp(output_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                                    
                                    video_loading_log(f"  Output file check:")
                                    video_loading_log(f"    File size: {output_size_current:,} bytes | DB size: {saved_file_size:,} bytes")
                                    video_loading_log(f"    File date: {current_date_str} | DB date: {saved_date_str}")
                                    
                                    # Comparison: ONLY skip probe if BOTH (size AND date) match
                                    output_size_matches = (saved_file_size == output_size_current)
                                    # Date comparison: exact timestamp match
                                    output_date_matches = (saved_modified == output_modified_current)
                                    
                                    if output_size_matches and output_date_matches:
                                        # Both match: use saved data (fast) - NO probing
                                        result['output_exists'] = True
                                        result['output_size_matches'] = True
                                        result['output_file_size_current'] = output_size_current
                                        result['output_modified_current'] = output_modified_current
                                        video_loading_log(f"    [OK] SKIP OUTPUT PROBE: File size = DB size ({output_size_current:,} bytes) AND File date = DB date ({current_date_str}) - No probing needed")
                                    else:
                                        # Size or date mismatch: exists but modified - probe needed
                                        result['output_exists'] = True
                                        result['output_size_matches'] = False
                                        if not output_size_matches and not output_date_matches:
                                            video_loading_log(f"    [ERROR] NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes) AND File date changed ({current_date_str} != {saved_date_str})")
                                        elif not output_size_matches:
                                            video_loading_log(f"    [ERROR] NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes)")
                                        else:
                                            video_loading_log(f"    [ERROR] NEED OUTPUT PROBE: File date changed ({current_date_str} != {saved_date_str})")
                                else:
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                            elif saved_file_size is not None:
                                # Only saved file size exists (no date): strict size+date policy forces probe
                                resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                                if output_scan_info:
                                    result['output_file'] = resolved_output_file
                                    output_size_current = output_scan_info.get('size')
                                    output_modified_current = output_scan_info.get('mtime')
                                    
                                    # Format date for human-readable output
                                    current_date_str = datetime.fromtimestamp(output_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                                    
                                    video_loading_log(f"  Output file check:")
                                    video_loading_log(f"    File size: {output_size_current:,} bytes | DB size: {saved_file_size:,} bytes")
                                    video_loading_log(f"    File date: {current_date_str} | DB date: (not available)")
                                    
                                    # With missing saved date we must probe regardless of size match.
                                    result['output_exists'] = True
                                    result['output_size_matches'] = False
                                    if saved_file_size == output_size_current:
                                        video_loading_log('    NEED OUTPUT PROBE: DB missing output date, strict size+date policy requires probe')
                                    else:
                                        video_loading_log(f'    NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes)')
                                else:
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                            else:
                                # No saved data: check (rare case)
                                resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                                if output_scan_info:
                                    result['output_file'] = resolved_output_file
                                result['output_exists'] = output_scan_info is not None
                                result['output_size_matches'] = False
                        elif saved_status_code_check in ('pending', 'nvenc_queue', 'svt_queue', 'svt_encoding', 'svt_validation', 'svt_crf_search', 'encoding', 'nvenc_encoding'):
                            # Queue-only states can be huge in large libraries.
                            # Avoid expensive network output checks unless there is evidence that
                            # output may exist (active processing state / progress / cached output metadata).
                            active_processing_statuses = ('svt_encoding', 'svt_validation', 'svt_crf_search', 'encoding', 'nvenc_encoding')
                            has_cached_output_meta = (
                                saved_video.get('output_modified_timestamp') is not None or
                                saved_video.get('output_file_size_bytes') is not None or
                                saved_video.get('new_size_bytes') is not None
                            )
                            progress_raw = str(saved_video.get('progress', '')).strip()
                            has_progress = progress_raw not in ('', '-', '0', '0%')
                            # Fast fallback check (dictionary-based scan, no filesystem I/O).
                            # This also catches copied outputs with original extensions.
                            resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                            has_fallback_output = output_scan_info is not None
                            should_check_pending_output = (
                                saved_status_code_check in active_processing_statuses or
                                has_cached_output_meta or
                                has_progress or
                                has_fallback_output
                            )

                            if not should_check_pending_output:
                                # Strict cache-first startup: queued item with no output signals.
                                # Skip filesystem probing entirely; this removes hundreds of
                                # unnecessary network exists/stat calls on warm start.
                                result['output_exists'] = False
                                result['output_size_matches'] = False
                            else:
                                if LOAD_DEBUG:
                                    print(f"  [PENDING CHECK] {video_path.name}: status_code='{saved_status_code_check}', checking output: {result['output_file']}")
                                
                                # Check output path with fallback (.av1.mkv and original extension copy path)
                                if has_fallback_output:
                                    result['output_file'] = resolved_output_file
                                    output_size_current = output_scan_info.get('size')
                                    output_modified_current = output_scan_info.get('mtime')
                                    result['output_exists'] = True
                                    result['output_size_matches'] = False  # No saved data, so False
                                    if LOAD_DEBUG:
                                        print(f"  [PENDING CHECK] [OK] OUTPUT EXISTS: {result['output_file'].name} ({output_size_current/1024/1024:.1f} MB)")
                                    video_loading_log(f"  * Output file exists but status is pending/encoding - will update to completed")
                                else:
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                                    if LOAD_DEBUG:
                                        print(f"  [PENDING CHECK] [ERROR] OUTPUT NOT EXISTS (checked all fallback paths)")
                        else:
                            # Otherwise check (rare case)
                            resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                            if output_scan_info:
                                result['output_file'] = resolved_output_file
                            result['output_exists'] = output_scan_info is not None
                            result['output_size_matches'] = False
                    
                    output_cq_crf = output_vmaf = output_psnr = output_frame_count = output_file_size = None
                    output_modified_date = None
                    output_encoder_type = None
                    output_duration_seconds = None
                    output_denoise_info = None
                    should_delete_output = False
                    new_size_bytes = None
                    saved_file_size = None
                    saved_modified = None
                    
                    cq_str = saved_video.get('cq') or '-'
                    saved_vmaf = saved_video.get('vmaf') or '-'
                    if saved_vmaf != "-":
                        try:
                            vmaf_num = float(saved_vmaf)
                            vmaf_str = format_localized_number(vmaf_num, decimals=2)
                        except (ValueError, TypeError):
                            vmaf_str = saved_vmaf
                    else:
                        vmaf_str = saved_vmaf
                    
                    saved_psnr = saved_video.get('psnr') or '-'
                    if saved_psnr != "-":
                        try:
                            psnr_num = float(saved_psnr)
                            psnr_str = format_localized_number(psnr_num, decimals=2)
                        except (ValueError, TypeError):
                            psnr_str = saved_psnr
                    else:
                        psnr_str = saved_psnr
                    
                    progress_str = saved_video.get('progress', '-')
                    new_size_str = saved_video.get('new_size', '-')
                    change_percent_display = saved_video.get('size_change', '-')
                    
                    suspicious_reasons = []
                    # Optimization: if saved_video says completed and output exists,
                    # only call get_output_file_info if file modified (size or date)
                    saved_status_code = _infer_saved_status_code(saved_video)
                    
                    output_file_modified = None
                    output_file_size_current = None
                    should_probe_output = True
                    
                    # Optimization: if pending/queued status but output file exists, must probe
                    if saved_status_code in ('pending', 'nvenc_queue', 'svt_queue', 'svt_encoding', 'svt_validation', 'svt_crf_search', 'encoding', 'nvenc_encoding'):
                        # Pending/queued status: if output file exists, must probe (might be finished)
                        if result.get('output_exists', False):
                            should_probe_output = True  # Must probe to update status
                            video_loading_log(f"  * Status is pending/encoding but output file exists - probing to check if completed")
                        else:
                            should_probe_output = False  # No output file, truly pending
                    # Optimization: if completed status and date matches, skip probe
                    elif result['output_exists'] and saved_status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        saved_modified = saved_video.get('output_modified_timestamp')
                        saved_file_size = saved_video.get('output_file_size_bytes')
                        missing_output_cache = _missing_output_probe_cache(saved_video, saved_status_code)
                        
                        # ONLY skip probe if BOTH (size AND date) match (output_size_matches flag)
                        if result.get('output_size_matches', False):
                            if missing_output_cache:
                                should_probe_output = True
                                video_loading_log("  NEED OUTPUT PROBE: File size/date match but output probe cache is missing")
                            else:
                                # Size and date match: use saved data (fast) - DO NOT read from file
                                output_file_modified = result.get('output_modified_current', saved_modified)
                                output_file_size_current = result.get('output_file_size_current', saved_file_size)
                                should_probe_output = False  # Skip slow probe and filesystem operations
                        elif should_skip_probe and not missing_output_cache and saved_modified is not None and saved_file_size is not None:
                            # Source file verified unchanged (size+mtime match) AND completed status
                            # AND DB has complete output cache → output can't have changed independently.
                            # Trust DB values, skip expensive ffprobe calls.
                            # (dest scan may fail to find file due to path normalization issues,
                            #  but that doesn't mean the output changed)
                            output_file_modified = saved_modified
                            output_file_size_current = saved_file_size
                            should_probe_output = False
                            video_loading_log(f"  SKIP OUTPUT PROBE: Source unchanged + completed + DB output cache valid → trusting DB")
                        elif saved_modified is not None and saved_file_size is not None:
                            # Saved data exists but source also changed: must probe (file may have been re-encoded)
                            output_file_modified = result.get('output_modified_current', saved_modified)
                            output_file_size_current = result.get('output_file_size_current', saved_file_size)
                            should_probe_output = True  # Must probe because source changed
                        else:
                            # If nothing in JSON, full probe (rare case)
                            should_probe_output = True
                    
                    if result['output_exists']:
                        if should_probe_output:
                            # Full probe - only call if file modified
                            video_loading_log(f"  PROBING output file (file changed or no saved data)")
                            probe_start = time.time()
                            output_cq_crf, output_vmaf, output_psnr, output_frame_count, output_file_size, output_modified_date, output_encoder_type, should_delete_output, output_duration_seconds, output_denoise_info = get_output_file_info(result['output_file'])
                            probe_time = (time.time() - probe_start) * 1000
                            video_loading_log(f"  Output probe took {probe_time:.2f}ms: cq={output_cq_crf}, vmaf={output_vmaf}, size={output_file_size}")
                            manual_info = infer_manual_cq_from_settings(
                                extract_settings_from_file(result['output_file']),
                                output_cq_crf
                            )
                            if manual_info:
                                saved_video.update(manual_info)
                                result.update(manual_info)
                            inferred_denoise_val = normalize_denoise_level(infer_denoise_level_from_filter_info(output_denoise_info))
                            if inferred_denoise_val > 0:
                                saved_video['denoise_enabled'] = inferred_denoise_val
                                result['denoise_enabled'] = inferred_denoise_val
                            if output_file_modified is None:
                                try:
                                    output_file_modified = result['output_file'].stat().st_mtime
                                except (OSError, PermissionError):
                                    pass
                            if output_file_size_current is None:
                                try:
                                    output_file_size_current = result['output_file'].stat().st_size
                                except (OSError, PermissionError):
                                    pass
                            if not output_modified_date:
                                fallback_output_ts = output_file_modified
                                if fallback_output_ts is None:
                                    fallback_output_ts = result.get('output_modified_current')
                                if fallback_output_ts is None:
                                    fallback_output_ts = saved_video.get('output_modified_timestamp')
                                output_modified_date = _format_completed_date_from_timestamp(fallback_output_ts)
                        else:
                            # Use saved data (fast) - all metadata from JSON
                            video_loading_log(f"  [OK] SKIP output probe: using saved data (size and date match)")
                            output_cq_crf = saved_video.get('cq')
                            if output_cq_crf and output_cq_crf != '-':
                                try:
                                    output_cq_crf = int(float(output_cq_crf))
                                except (ValueError, TypeError):
                                    output_cq_crf = None
                            else:
                                output_cq_crf = None
                            output_vmaf = saved_video.get('vmaf')
                            if output_vmaf and output_vmaf != '-':
                                try:
                                    output_vmaf = float(output_vmaf)
                                except (ValueError, TypeError):
                                    output_vmaf = None
                            else:
                                output_vmaf = None
                            output_psnr = saved_video.get('psnr')
                            if output_psnr and output_psnr != '-':
                                try:
                                    output_psnr = float(output_psnr)
                                except (ValueError, TypeError):
                                    output_psnr = None
                            else:
                                output_psnr = None
                            # Recovery: if CQ/VMAF missing but status is completed, extract from file metadata
                            # BUT only if source changed (should_skip_probe=False) - when source is
                            # unchanged we trust DB fully, no ffprobe recovery needed on warm start
                            if output_cq_crf is None and not should_skip_probe and saved_status_code in ('completed', 'completed_nvenc', 'completed_svt') and result.get('output_file'):
                                try:
                                    settings_str = extract_settings_from_file(result['output_file'])
                                    if settings_str:
                                        cq_match = re.search(r'CQ:(\d+)', settings_str)
                                        crf_match = re.search(r'CRF:(\d+)', settings_str)
                                        if cq_match:
                                            output_cq_crf = int(cq_match.group(1))
                                        elif crf_match:
                                            output_cq_crf = int(crf_match.group(1))
                                        if output_vmaf is None:
                                            vmaf_match = re.search(r'(?:Actual|Planned)\s+VMAF:\s*([\d.,]+)', settings_str)
                                            if vmaf_match:
                                                output_vmaf = float(vmaf_match.group(1).replace(',', '.'))
                                        if output_psnr is None:
                                            psnr_match = re.search(r'PSNR:\s*([\d.,]+)', settings_str)
                                            if psnr_match:
                                                output_psnr = float(psnr_match.group(1).replace(',', '.'))
                                        if output_cq_crf is not None:
                                            video_loading_log(f"  [RECOVERY] CQ/VMAF recovered from file metadata: CQ={output_cq_crf}, VMAF={output_vmaf}")
                                except Exception:
                                    pass

                            output_frame_count = saved_video.get('output_frame_count')
                            if output_frame_count is None:
                                output_frame_count = saved_video.get('source_frame_count')
                            output_file_size = result.get('output_file_size_current', saved_video.get('output_file_size_bytes') or saved_video.get('new_size_bytes'))
                            output_modified_date = saved_video.get('completed_date', '')
                            if not output_modified_date:
                                cached_output_ts = result.get('output_modified_current')
                                if cached_output_ts is None:
                                    cached_output_ts = saved_video.get('output_modified_timestamp')
                                output_modified_date = _format_completed_date_from_timestamp(cached_output_ts)
                            should_delete_output = False
                            output_duration_seconds = saved_video.get('output_duration_seconds')
                            if output_duration_seconds is None:
                                output_duration_seconds = result.get('source_duration_seconds')
                            
                            # Encoder type from JSON (if missing, from status_code)
                            output_encoder_type = saved_video.get('output_encoder_type')
                            if output_encoder_type is None:
                                # Try to infer from status_code
                                if saved_status_code == 'completed_nvenc':
                                    output_encoder_type = 'nvenc'
                                elif saved_status_code == 'completed_svt':
                                    output_encoder_type = 'svt-av1'
                                elif saved_status_code in ('completed', 'completed_copy', 'completed_exists'):
                                    # Strict cache policy: do not probe on warm load when size/date match.
                                    # Keep inference filesystem-free; unknown encoder can remain None.
                                    saved_status_text = str(saved_video.get('status', '')).upper()
                                    if 'NVENC' in saved_status_text:
                                        output_encoder_type = 'nvenc'
                                    elif 'SVT-AV1' in saved_status_text or 'SVT' in saved_status_text:
                                        output_encoder_type = 'svt-av1'
                        
                        if should_probe_output:
                            if result['source_frame_count'] and output_frame_count and frames_significantly_different(result['source_frame_count'], output_frame_count):
                                # 1 másodperces tolerancia: ha a hossz eltérés 1mp-en belül van, nem tekintjük hibásnak
                                duration_mismatch = True
                                if result.get('source_duration_seconds') and output_duration_seconds:
                                    if abs(result['source_duration_seconds'] - output_duration_seconds) < 1.0:
                                        duration_mismatch = False
                                        video_loading_log(f"  * Képkockaszám eltérés detektálva, de a hossz különbség 1mp-en belül van ({abs(result['source_duration_seconds'] - output_duration_seconds):.2f}s) -> elfogadva.")
                                
                                if duration_mismatch:
                                    suspicious_reasons.append(f"frames: {output_frame_count}/{result['source_frame_count']}")
                        
                        if output_file_size:
                            new_size_bytes = output_file_size
                        else:
                            # Use saved value, do not read from file (fast)
                            new_size_bytes = saved_video.get('output_file_size_bytes') or saved_video.get('new_size_bytes')
                            # Only read if not in JSON (rare case)
                            if new_size_bytes is None:
                                try:
                                    new_size_bytes = result['output_file'].stat().st_size
                                except (OSError, PermissionError):
                                    new_size_bytes = None
                    
                    if not result['output_exists'] and new_size_bytes is None:
                        new_size_bytes = saved_video.get('new_size_bytes')
                    if new_size_bytes is not None:
                        new_size_mb = new_size_bytes / (1024**2)
                        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        if result['source_size_bytes']:
                            try:
                                change_percent = ((new_size_bytes - result['source_size_bytes']) / result['source_size_bytes']) * 100
                                change_percent_display = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                            except ZeroDivisionError:
                                change_percent_display = format_localized_number(0, decimals=2) + '%'
                    else:
                        saved_new_size = saved_video.get('new_size', '-')
                        if saved_new_size != "-" and "MB" in saved_new_size:
                            try:
                                size_num = float(saved_new_size.replace("MB", "").strip())
                                new_size_str = f"{format_localized_number(size_num, decimals=1)} MB"
                            except (ValueError, TypeError):
                                new_size_str = saved_new_size
                        else:
                            new_size_str = saved_new_size
                        
                        saved_change = saved_video.get('size_change', '-')
                        if saved_change != "-" and "%" in saved_change:
                            try:
                                has_plus = saved_change.strip().startswith('+')
                                clean_val = saved_change.replace("%", "").replace("+", "").strip()
                                change_num = float(clean_val)
                                change_percent_display = ("+" if has_plus else "") + format_localized_number(change_num, decimals=2, show_sign=False) + "%"
                            except (ValueError, TypeError):
                                change_percent_display = saved_change
                        elif isinstance(change_percent_display, (int, float)):
                            change_percent_display = f"{format_localized_number(change_percent_display, decimals=2, show_sign=True)}%"
                        else:
                            change_percent_display = saved_change
                    
                    size_ratio = None
                    if result['source_size_bytes'] and new_size_bytes:
                        try:
                            size_ratio = new_size_bytes / result['source_size_bytes']
                            # Méretcsökkenés nem gyanús ok - eltávolítva
                            # if size_ratio < SIZE_MISMATCH_RATIO:
                            #     suspicious_reasons.append(f"size ratio {size_ratio:.2%}")
                        except ZeroDivisionError:
                            pass
                    
                    duration_ratio = None
                    if result['source_duration_seconds'] and output_duration_seconds:
                        try:
                            duration_ratio = output_duration_seconds / result['source_duration_seconds']
                            if duration_ratio < DURATION_MISMATCH_RATIO:
                                # 1 másodperces tolerancia szabály alkalmazása
                                if abs(result['source_duration_seconds'] - output_duration_seconds) >= 1.0:
                                    suspicious_reasons.append(f"duration {duration_ratio:.2%}")
                                else:
                                    video_loading_log(f"  * Hossz arány alacsony ({duration_ratio:.2%}), de a különbség 1mp-en belül van -> elfogadva.")
                        except ZeroDivisionError:
                            pass
                    
                    # Detect if file is copied (not encoded) based on:
                    # - No encoder type in Settings tag
                    # - No CQ/CRF value
                    # - File size matches source (or very close, >95%)
                    is_likely_copy = False
                    if result['output_exists'] and not suspicious_reasons and not should_delete_output:
                        if output_encoder_type is None and output_cq_crf is None:
                            # No encoder type and no CQ/CRF = likely copied file
                            # Calculate size_ratio if not already calculated
                            if size_ratio is None and result['source_size_bytes'] and new_size_bytes:
                                try:
                                    size_ratio = new_size_bytes / result['source_size_bytes']
                                except ZeroDivisionError:
                                    size_ratio = None
                            if size_ratio is not None and size_ratio >= 0.95:
                                # File size is 95%+ of source = likely copy
                                is_likely_copy = True
                                log_file_check(f"[OK] Másolt videó detektálva (méret arány: {size_ratio:.2%}): {video_path.name}")
                    
                    # Set status: first from output_encoder_type (DB), then status_code, finally default
                    saved_output_encoder_type = saved_video.get('output_encoder_type')
                    status_code = saved_video.get('status_code')
                    saved_status_code = status_code  # Save original status_code
                    
                    # IMPORTANT: Preserve completed_copy and completed_exists status - don't overwrite with encoder type
                    if saved_output_encoder_type:
                        # Don't override completed_copy/completed_exists - these are copy statuses,
                        # encoder type in DB may be stale from a previous encoding attempt
                        if status_code not in ('completed_copy', 'completed_exists'):
                            if saved_output_encoder_type == 'nvenc':
                                status_code = 'completed_nvenc'
                            elif saved_output_encoder_type == 'svt-av1':
                                status_code = 'completed_svt'
                    elif status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        # If no output_encoder_type but status_code exists, try to infer from it
                        if status_code == 'completed_nvenc':
                            saved_output_encoder_type = 'nvenc'
                        elif status_code == 'completed_svt':
                            saved_output_encoder_type = 'svt-av1'
                    elif is_likely_copy and not status_code:
                        # Auto-detect copied file if no status_code saved
                        status_code = 'completed_copy'
                        saved_status_code = 'completed_copy'
                    
                    if not status_code:
                        saved_status = saved_video.get('status', '')
                        status_code = normalize_status_to_code(saved_status)
                        saved_status_code = status_code  # Update
                    
                    # If still no status_code and file looks like copy, set to completed_copy
                    if not status_code and is_likely_copy:
                        status_code = 'completed_copy'
                        saved_status_code = 'completed_copy'
                    
                    # IMPORTANT: If is_likely_copy and current status_code is not completed_copy/completed_exists, 
                    # but also no encoder type, then set to completed_copy
                    if is_likely_copy and status_code not in ('completed_copy', 'completed_exists', 'completed_nvenc', 'completed_svt'):
                        if output_encoder_type is None and saved_output_encoder_type is None:
                            status_code = 'completed_copy'
                            saved_status_code = 'completed_copy'
                    
                    if not status_code:
                        status_code = 'nvenc_queue' if nvenc_enabled_val else 'svt_queue'
                        saved_status_code = status_code  # Update
                    status_text = self._build_saved_manual_queue_status(
                        status_code,
                        saved_video.get('status', ''),
                        saved_video.get('manual_cq_value'),
                        saved_video.get('manual_quality_check')
                    )
                    completed_date = saved_video.get('completed_date', '')
                    if not completed_date:
                        completed_date = _format_completed_date_from_timestamp(saved_video.get('output_modified_timestamp'))
                    
                    duration_str, frames_str = self._build_duration_frames_display(
                        source_duration_seconds=result.get('source_duration_seconds'),
                        source_frame_count=result.get('source_frame_count'),
                        show_target=False
                    )
                    
                    # Always display freshly generated relative path
                    video_name_display = result['video_name'] or ""
                    if not video_name_display:
                        video_name_display = saved_video.get('video_name', str(video_path))
                    # If DB had old (filename only), update in-memory instance,
                    # so saving will put new value in table.
                    if saved_video.get('video_name') != video_name_display:
                        saved_video['video_name'] = video_name_display
                    
                    if result['output_exists'] and not suspicious_reasons and not should_delete_output:
                        duration_str, frames_str = self._build_duration_frames_display(
                            source_duration_seconds=result.get('source_duration_seconds'),
                            source_frame_count=result.get('source_frame_count'),
                            target_duration_seconds=output_duration_seconds,
                            target_frame_count=output_frame_count,
                            show_target=True
                        )
                        if output_cq_crf is not None:
                            cq_str = str(output_cq_crf)
                        if output_vmaf is not None:
                            vmaf_str = format_localized_number(output_vmaf, decimals=2)
                            # if output_denoise_info:
                            #     vmaf_str += f" [{output_denoise_info}]"
                        if output_psnr is not None:
                            psnr_str = format_localized_number(output_psnr, decimals=2)
                        progress_str = '100%'
                        completed_date = output_modified_date or completed_date
                        
                        # If output_encoder_type exists (from file or DB), use it
                        # IMPORTANT: only overwrite status_code if no encoder-specific status yet
                        # IMPORTANT: preserve completed_copy and completed_exists status if no encoder type
                        # IMPORTANT: if is_likely_copy, set to completed_copy even if status_code was something else
                        if is_likely_copy and output_encoder_type is None and saved_output_encoder_type is None:
                            # File is detected as copy - set to completed_copy
                            status_code = 'completed_copy'
                            saved_status_code = 'completed_copy'
                        elif status_code not in ('completed_nvenc', 'completed_svt'):
                            if output_encoder_type:
                                if output_encoder_type == 'nvenc':
                                    status_code = 'completed_nvenc'
                                elif output_encoder_type == 'svt-av1':
                                    status_code = 'completed_svt'
                                else:
                                    status_code = 'completed'
                            elif saved_output_encoder_type:
                                # If no output_encoder_type from file but exists in DB, use it
                                if saved_output_encoder_type == 'nvenc':
                                    status_code = 'completed_nvenc'
                                elif saved_output_encoder_type == 'svt-av1':
                                    status_code = 'completed_svt'
                                else:
                                    status_code = 'completed'
                            elif status_code in ('completed_copy', 'completed_exists'):
                                # Preserve completed_copy and completed_exists status (no encoder type = copied file)
                                pass  # Keep status_code as is
                            elif status_code not in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                                # If no encoder type info but status is completed, default to "completed"
                                status_code = 'completed'
                        
                        status_text = self._build_saved_manual_queue_status(
                            status_code,
                            saved_video.get('status', ''),
                            saved_video.get('manual_cq_value'),
                            saved_video.get('manual_quality_check')
                        )
                        # Denoise source of truth: prefer probed filter metadata, fallback to DB.
                        denoise_val = normalize_denoise_level(_safe_int(saved_video.get('denoise_enabled')))
                        inferred_denoise_val = normalize_denoise_level(infer_denoise_level_from_filter_info(output_denoise_info))
                        if inferred_denoise_val > 0:
                            denoise_val = inferred_denoise_val
                        denoise_str = _denoise_level_to_display(denoise_val)
                        result['denoise_enabled'] = denoise_val
                        saved_video['denoise_enabled'] = denoise_val
                        result['values'] = (denoise_str, hard_rotate_to_display(result.get('hard_rotate_degrees', 0)), video_name_display, status_text, cq_str, vmaf_str, psnr_str, progress_str,
                                          orig_size_str, new_size_str, change_percent_display, duration_str, frames_str, completed_date)
                        result['tag'] = 'completed'
                        # Store output metadata in result for tree_item_data persistence
                        # This prevents save_state_to_db from losing probed data on warm start
                        if output_encoder_type:
                            result['output_encoder_type'] = output_encoder_type
                        if output_file_modified is not None:
                            result['output_modified_timestamp'] = output_file_modified
                        if new_size_bytes is not None:
                            result['new_size_bytes'] = new_size_bytes
                        if output_duration_seconds is not None:
                            result['output_duration_seconds'] = output_duration_seconds
                        if output_frame_count is not None:
                            result['output_frame_count'] = output_frame_count
                        if output_duration_seconds and output_frame_count:
                            try:
                                result['output_fps'] = output_frame_count / output_duration_seconds
                            except (ValueError, TypeError, ZeroDivisionError):
                                pass
                        if output_cq_crf is not None:
                            result['cq'] = output_cq_crf
                        if output_vmaf is not None:
                            result['vmaf'] = output_vmaf
                        if output_psnr is not None:
                            result['psnr'] = output_psnr
                    else:
                        if result['output_exists']:
                            # Részletes naplózás az újrakódolási döntésről
                            log_details = []
                            log_details.append(f"* Újrakódolásra kerül: {video_path.name}")
                            log_details.append(f"   Korábbi státusz: {saved_status_code} (validálva volt)")
                            
                            if suspicious_reasons:
                                log_details.append(f"   Gyanús okok: {', '.join(suspicious_reasons)}")
                            
                            if should_delete_output:
                                log_details.append(f"   should_delete_output: True (get_output_file_info által jelzett)")
                            
                            # További részletek
                            if result.get('source_frame_count') and output_frame_count:
                                log_details.append(f"   Frame count: forrás={result['source_frame_count']}, output={output_frame_count}")
                            
                            if result.get('source_size_bytes') and new_size_bytes:
                                size_ratio_detail = new_size_bytes / result['source_size_bytes'] if result['source_size_bytes'] > 0 else 0
                                log_details.append(f"   Size ratio: {size_ratio_detail:.2%} (forrás={result['source_size_bytes']} bytes, output={new_size_bytes} bytes)")
                            
                            if result.get('source_duration_seconds') and output_duration_seconds:
                                duration_ratio_detail = output_duration_seconds / result['source_duration_seconds'] if result['source_duration_seconds'] > 0 else 0
                                log_details.append(f"   Duration ratio: {duration_ratio_detail:.2%} (forrás={result['source_duration_seconds']:.2f}s, output={output_duration_seconds:.2f}s)")
                            
                            if output_file_size_current and output_file_modified:
                                log_details.append(f"   Output file size: {output_file_size_current} bytes, modified: {output_file_modified}")
                            
                            if saved_file_size and saved_modified:
                                log_details.append(f"   Saved file size: {saved_file_size} bytes, modified: {saved_modified}")
                                if output_file_size_current != saved_file_size:
                                    log_details.append(f"   * Size mismatch: {output_file_size_current} != {saved_file_size}")
                                if output_file_modified != saved_modified:
                                    log_details.append(f"   * Date mismatch: {output_file_modified} != {saved_modified}")
                            
                            details_str = "\n".join(log_details)
                            log_file_check(details_str)
                            # Also print to console for --forceconsole
                            print(f"\n{details_str}\n")
                            
                            try:
                                print(f"[WARN] Deleting incomplete/corrupt output: {video_path.name}")
                                result['output_file'].unlink()
                            except (OSError, PermissionError) as e:
                                print(f"  [ERROR] Could not delete: {e}")
                                pass
                            result['output_exists'] = False
                        completed_date = ''
                        status_code = 'nvenc_queue' if nvenc_enabled_val else 'svt_queue'
                        status_text = self._build_saved_manual_queue_status(
                            status_code,
                            saved_video.get('status', ''),
                            saved_video.get('manual_cq_value'),
                            saved_video.get('manual_quality_check')
                        )
                        warning_progress = "* Incomplete output file, re-encoding needed" if (suspicious_reasons or should_delete_output) else '-'
                        denoise_val = normalize_denoise_level(_safe_int(saved_video.get('denoise_enabled')))
                        inferred_denoise_val = normalize_denoise_level(infer_denoise_level_from_filter_info(output_denoise_info))
                        if inferred_denoise_val > 0:
                            denoise_val = inferred_denoise_val
                        denoise_str = _denoise_level_to_display(denoise_val)
                        result['denoise_enabled'] = denoise_val
                        saved_video['denoise_enabled'] = denoise_val
                        result['values'] = (denoise_str, hard_rotate_to_display(result.get('hard_rotate_degrees', 0)), video_name_display, status_text, "-", "-", "-", warning_progress, orig_size_str, "-", "-", duration_str, frames_str, completed_date)
                        result['tag'] = 'pending'
                        # Store output encoder_type for tree_item_data persistence
                        if output_encoder_type:
                            result['output_encoder_type'] = output_encoder_type

                    if status_code == 'completed_copy':
                        result['tag'] = 'completed_copy'
                    elif status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_exists'):
                        result['tag'] = 'completed'
                    elif status_code == 'nvenc_queue':
                        result['tag'] = 'pending'
                    elif status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search'):
                        result['tag'] = 'encoding_nvenc'
                    elif status_code == 'svt_queue':
                        result['tag'] = 'pending'
                    elif status_code in ('svt_encoding', 'svt_validation', 'svt_crf_search'):
                        result['tag'] = 'encoding_svt'
                    elif status_code in ('failed', 'source_missing', 'file_missing', 'vmaf_error'):
                        result['tag'] = 'failed'
                    elif status_code in ('needs_check', 'needs_check_nvenc', 'needs_check_svt'):
                        result['tag'] = 'needs_check'
                    else:
                        result['tag'] = 'pending'
                else:
                    # New video, no saved state
                    # First read file size (if not yet set)
                    if result['source_size_bytes'] is None:
                        source_scan_info = _get_source_scan_info(video_path)
                        if source_scan_info:
                            result['source_size_bytes'] = source_scan_info.get('size')
                            result['source_modified_timestamp'] = source_scan_info.get('mtime')  # For caching
                        elif video_path.exists():
                            try:
                                stat_info = video_path.stat()
                                result['source_size_bytes'] = stat_info.st_size
                                result['source_modified_timestamp'] = stat_info.st_mtime  # For caching
                            except (OSError, PermissionError):
                                result['source_size_bytes'] = None
                    
                    orig_size_mb = None  # Initialization
                    if result['source_size_bytes']:
                        orig_size_mb = result['source_size_bytes'] / (1024**2)
                        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    else:
                        orig_size_str = "-"
                    
                    # Probe video
                    video_loading_log(f"  PROBING source video (new video, no saved data)")
                    try:
                        probe_start = time.time()
                        duration, fps = get_video_info(video_path)
                        probe_time = (time.time() - probe_start) * 1000
                        video_loading_log(f"  Source probe took {probe_time:.2f}ms: duration={duration}, fps={fps}")
                        if duration and fps:
                            result['source_frame_count'] = int(duration * fps)
                        result['source_duration_seconds'] = duration
                    except Exception:
                        pass
                    
                    duration_str, frames_str = self._build_duration_frames_display(
                        source_duration_seconds=result.get('source_duration_seconds'),
                        source_frame_count=result.get('source_frame_count'),
                        show_target=False
                    )
                    
                    # Output file check
                    if destination_is_empty:
                        result['output_exists'] = False
                    else:
                        resolved_output_file, output_scan_info = _find_existing_output_with_fallback(video_path, result['output_file'])
                        if output_scan_info:
                            result['output_file'] = resolved_output_file
                        result['output_exists'] = output_scan_info is not None
                    
                    output_cq_crf = None
                    output_vmaf = None
                    output_psnr = None
                    output_frame_count = None
                    output_file_size = None
                    output_modified_date = None
                    output_encoder_type = None
                    output_denoise_info = None
                    should_delete_output = False
                    
                    if result['output_exists']:
                        video_loading_log(f"  PROBING output file (new video)")
                        probe_start = time.time()
                        output_cq_crf, output_vmaf, output_psnr, output_frame_count, output_file_size, output_modified_date, output_encoder_type, should_delete_output, output_duration_seconds, output_denoise_info = get_output_file_info(result['output_file'])
                        probe_time = (time.time() - probe_start) * 1000
                        video_loading_log(f"  Output probe took {probe_time:.2f}ms: cq={output_cq_crf}, vmaf={output_vmaf}, size={output_file_size}")
                        manual_info = infer_manual_cq_from_settings(
                            extract_settings_from_file(result['output_file']),
                            output_cq_crf
                        )
                        if manual_info:
                            result.update(manual_info)
                        inferred_denoise_val = normalize_denoise_level(infer_denoise_level_from_filter_info(output_denoise_info))
                        if inferred_denoise_val > 0:
                            result['denoise_enabled'] = inferred_denoise_val
                        if result['source_frame_count'] and output_frame_count and frames_significantly_different(result['source_frame_count'], output_frame_count):
                            is_suspicious = True
                            # 1 másodperces tolerancia ellenőrzése új videó esetén is
                            if result.get('source_duration_seconds') and output_duration_seconds:
                                if abs(result['source_duration_seconds'] - output_duration_seconds) < 1.0:
                                    is_suspicious = False
                                    video_loading_log(f"  * [ÚJ VIDEÓ] Képkockaszám eltérés, de a hossz különbség 1mp-en belül van -> elfogadva.")
                            
                            if is_suspicious:
                                should_delete_output = True
                                msg = f"* Incomplete output file (frames: {output_frame_count}/{result['source_frame_count']}) -> re-encoding: {video_path.name}"
                                log_file_check(msg)
                                # Also print to console for --forceconsole
                                print(f"\n{msg}\n")

                                try:
                                    print(f"[WARN] Deleting incomplete output: {video_path.name}")
                                    result['output_file'].unlink()
                                except (OSError, PermissionError) as e:
                                    print(f"  [ERROR] Could not delete: {e}")
                                result['output_exists'] = False
                    
                    if result['output_exists']:
                        duration_str, frames_str = self._build_duration_frames_display(
                            source_duration_seconds=result.get('source_duration_seconds'),
                            source_frame_count=result.get('source_frame_count'),
                            target_duration_seconds=output_duration_seconds,
                            target_frame_count=output_frame_count,
                            show_target=True
                        )
                        if output_file_size:
                            new_size_mb = output_file_size / (1024**2)
                        else:
                            try:
                                new_size_mb = result['output_file'].stat().st_size / (1024**2)
                            except (OSError, PermissionError):
                                new_size_mb = 0
                        # Check if orig_size_mb is defined
                        if orig_size_mb is not None and orig_size_mb > 0:
                            change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100
                        else:
                            change_percent = 0
                        cq_str = str(output_cq_crf) if output_cq_crf is not None else "-"
                        vmaf_str = format_localized_number(output_vmaf, decimals=2) if output_vmaf is not None else "-"
                        psnr_str = format_localized_number(output_psnr, decimals=2) if output_psnr is not None else "-"
                        progress_str = "100%" if output_cq_crf is not None else "-"
                        is_likely_copy_new = False
                        if output_encoder_type is None and output_cq_crf is None:
                            if result.get('source_size_bytes') and new_size_mb:
                                try:
                                    size_ratio_new = (new_size_mb * (1024 ** 2)) / result['source_size_bytes']
                                    if size_ratio_new >= 0.95:
                                        is_likely_copy_new = True
                                except (ValueError, TypeError, ZeroDivisionError):
                                    pass

                        if is_likely_copy_new:
                            status_str = t('status_completed_copy')
                        elif output_encoder_type == 'nvenc':
                            status_str = t('status_completed_nvenc')
                        elif output_encoder_type == 'svt-av1':
                            status_str = t('status_completed_svt')
                        else:
                            status_str = t('status_completed')
                        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        change_percent_display = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                        denoise_val = normalize_denoise_level(result.get('denoise_enabled'))
                        result['denoise_enabled'] = denoise_val
                        denoise_str = _denoise_level_to_display(denoise_val)
                        result['values'] = (denoise_str, hard_rotate_to_display(result.get('hard_rotate_degrees', 0)), result['video_name'], status_str, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_percent_display, duration_str, frames_str, output_modified_date or "")
                        result['tag'] = "completed_copy" if is_likely_copy_new else "completed"
                        # Store output metadata in result for tree_item_data persistence
                        if output_encoder_type:
                            result['output_encoder_type'] = output_encoder_type
                        if output_file_size is not None:
                            result['new_size_bytes'] = output_file_size
                        if output_duration_seconds is not None:
                            result['output_duration_seconds'] = output_duration_seconds
                        if output_frame_count is not None:
                            result['output_frame_count'] = output_frame_count
                        if output_duration_seconds and output_frame_count:
                            try:
                                result['output_fps'] = output_frame_count / output_duration_seconds
                            except (ValueError, TypeError, ZeroDivisionError):
                                pass
                        if output_cq_crf is not None:
                            result['cq'] = output_cq_crf
                        if output_vmaf is not None:
                            result['vmaf'] = output_vmaf
                        if output_psnr is not None:
                            result['psnr'] = output_psnr
                        try:
                            result['output_modified_timestamp'] = result['output_file'].stat().st_mtime
                        except (OSError, PermissionError, AttributeError):
                            pass
                    else:
                        status_text = self._build_saved_manual_queue_status(
                            'nvenc_queue' if nvenc_enabled_val else 'svt_queue',
                            saved_video.get('status', '') if saved_video else '',
                            saved_video.get('manual_cq_value') if saved_video else None,
                            saved_video.get('manual_quality_check') if saved_video else None
                        )
                        result['denoise_enabled'] = 0
                        result['values'] = (_denoise_level_to_display(0), hard_rotate_to_display(result.get('hard_rotate_degrees', 0)), result['video_name'], status_text, "-", "-", "-", "-", orig_size_str, "-", "-", duration_str, frames_str, "")
                        result['tag'] = "pending"
                
                # Subtitle files
                valid_subtitles, invalid_subtitles = _get_validated_subtitles_from_index(video_path)
                # Mindkettőt beletesszük a subtitle_files-ba, hogy a treeview-ban megjelenjenek
                # Az invalid feliratoknál jelöljük, hogy hibásak (harmadik elem: reason)
                result['subtitle_files'] = valid_subtitles + [(sub_path, lang_part, reason) for sub_path, lang_part, reason in invalid_subtitles]
                if invalid_subtitles:
                    result['invalid_subtitles'] = invalid_subtitles

                try:
                    from .core_audio_video_ops import get_video_extra_metadata

                    saved_source_extra = None
                    saved_output_extra = None
                    if saved_video:
                        saved_source_extra = self._normalize_extra_metadata_dict(saved_video.get('source_extra_metadata'))
                        saved_output_extra = self._normalize_extra_metadata_dict(saved_video.get('output_extra_metadata'))

                    _src_has_streams = bool(saved_source_extra and hasattr(self, '_extra_metadata_has_any_stream_rows') and self._extra_metadata_has_any_stream_rows(saved_source_extra))
                    _src_has_size_br = bool(saved_source_extra and hasattr(self, '_extra_metadata_has_size_and_bitrate') and self._extra_metadata_has_size_and_bitrate(saved_source_extra))
                    saved_source_extra_ready = bool(saved_source_extra and _src_has_streams and _src_has_size_br)

                    if saved_source_extra and not saved_source_extra_ready and hasattr(self, '_diagnose_metadata_readiness'):
                        log_file_check(f"  SOURCE cache diagnosis: {self._diagnose_metadata_readiness(saved_source_extra, 'src')}")

                    if saved_source_extra and should_skip_probe and saved_source_extra_ready:
                        result['source_extra_metadata'] = saved_source_extra
                    else:
                        _reasons = []
                        if not saved_source_extra:
                            _reasons.append("no cached data")
                        elif not should_skip_probe:
                            _reasons.append("file changed (should_skip_probe=False)")
                        elif not _src_has_streams:
                            _reasons.append("missing stream rows")
                        elif not _src_has_size_br:
                            _reasons.append("missing file_size/bit_rate")
                        _reason_str = ", ".join(_reasons) if _reasons else "unknown"
                        log_file_check(f"  [RE-PROBE] SOURCE {video_path.name}: {_reason_str}")
                        _extra_t0 = time.time()
                        result['source_extra_metadata'] = get_video_extra_metadata(video_path)
                        _extra_dt = (time.time() - _extra_t0) * 1000
                        log_file_check(f"  [RE-PROBE] SOURCE {video_path.name}: took {_extra_dt:.0f}ms")

                    output_file_for_extra = result.get('output_file')
                    output_exists_for_extra = bool(result.get('output_exists') and output_file_for_extra and Path(output_file_for_extra).exists())
                    output_is_unchanged = bool(
                        result.get('output_size_matches', False) or
                        (should_skip_probe and not should_probe_output)
                    )
                    _out_has_streams = bool(saved_output_extra and hasattr(self, '_extra_metadata_has_any_stream_rows') and self._extra_metadata_has_any_stream_rows(saved_output_extra))
                    _out_has_size_br = bool(saved_output_extra and hasattr(self, '_extra_metadata_has_size_and_bitrate') and self._extra_metadata_has_size_and_bitrate(saved_output_extra))
                    saved_output_extra_ready = bool(saved_output_extra and _out_has_streams and _out_has_size_br)

                    if saved_output_extra and not saved_output_extra_ready and hasattr(self, '_diagnose_metadata_readiness'):
                        log_file_check(f"  OUTPUT cache diagnosis: {self._diagnose_metadata_readiness(saved_output_extra, 'out')}")

                    if output_exists_for_extra:
                        if saved_output_extra and output_is_unchanged and saved_output_extra_ready:
                            result['output_extra_metadata'] = saved_output_extra
                        else:
                            _reasons = []
                            if not saved_output_extra:
                                _reasons.append("no cached data")
                            elif not output_is_unchanged:
                                _reasons.append("output file changed")
                            elif not _out_has_streams:
                                _reasons.append("missing stream rows")
                            elif not _out_has_size_br:
                                _reasons.append("missing file_size/bit_rate")
                            _reason_str = ", ".join(_reasons) if _reasons else "unknown"
                            log_file_check(f"  [RE-PROBE] OUTPUT {Path(output_file_for_extra).name}: {_reason_str}")
                            _extra_t0 = time.time()
                            result['output_extra_metadata'] = get_video_extra_metadata(output_file_for_extra)
                            _extra_dt = (time.time() - _extra_t0) * 1000
                            log_file_check(f"  [RE-PROBE] OUTPUT {Path(output_file_for_extra).name}: took {_extra_dt:.0f}ms")
                except Exception as extra_meta_error:
                    log_file_check(f"  Extra metadata probe skipped: {extra_meta_error}")

                video_loading_log(f"END process_video_data: {result['video_name']} - SUCCESS")
                
            except Exception as e:
                result['error'] = str(e)
                log_file_check(f"[ERROR] Error during video processing ({video_path}): {e}")
                video_name_display = result.get('video_name') or str(video_path)
                error_text = str(e)
                if len(error_text) > 120:
                    error_text = error_text[:117] + "..."
                status_text = f"{t('status_load_error')}: {error_text}" if error_text else t('status_load_error')
                load_debug_log(f"process_video_data exception: {video_name_display} -> {error_text}")
                video_loading_log(f"END process_video_data: {video_name_display} - ERROR: {error_text}")
                result['values'] = ("", "0", video_name_display, status_text, "-", "-", "-", "-", "-", "-", "-", "-", "-", "")
                result['tag'] = "failed"
            
            return result
        
        # Parallel processing - multiple workers for faster loading
        total_videos = len(self.video_files)
        # Optimized worker count: not too many to avoid making computer unusable
        # I/O-bound operations, but don't need too many threads (4-8 enough)
        cpu_count = os.cpu_count() or 4
        max_workers = min(8, total_videos, max(4, cpu_count))  # Max 8 workers, min 4 (if enough CPU)
        
        # Thread-safe queue for completed data
        completed_data_queue = queue.Queue()
        processed_count = [0]  # List for mutable counter
        
        # GUI update timer (every second)
        last_update_time = [time.time()]
        
        def update_gui_from_queue(force=False):
            """
            GUI update from completed data
            """
            current_time = time.time()
            if not force and current_time - last_update_time[0] < 1.0:  # Only every second (unless force=True)
                if LOAD_DEBUG:
                    load_debug_log(f"update_gui_from_queue skipped (throttle) | force={force} | processed={processed_count[0]} | queue={completed_data_queue.qsize()}")
                return
            
            last_update_time[0] = current_time
            items_to_add = []
            if LOAD_DEBUG:
                load_debug_log(f"update_gui_from_queue starting | force={force} | queue={completed_data_queue.qsize()} | processed={processed_count[0]}")
            
            # Collect completed data
            while not completed_data_queue.empty():
                try:
                    data = completed_data_queue.get_nowait()
                    # Do not count 'finished' flag
                    if not data.get('finished'):
                        items_to_add.append(data)
                        processed_count[0] += 1
                except queue.Empty:
                    break
            
            if LOAD_DEBUG:
                load_debug_log(f"update_gui_from_queue: {len(items_to_add)} items processed, counter={processed_count[0]}/{total_videos}")
            
            # Add to table
            for data in items_to_add:
                # Check 'finished' flag
                if data.get('finished'):
                    continue
                
                # Safety check: values and tag must exist
                if data.get('values') is None or data.get('tag') is None:
                    log_file_check(f"* Missing values or tag: {data.get('video_path', 'unknown')}")
                    continue
                
                try:
                    order_num = data.get('order_num', 0)
                    values = data['values']
                    tag = data['tag']
                    
                    # Check if not already in table (avoid duplication)
                    if data.get('video_path') and data['video_path'] in self.video_items:
                        continue
                    
                    item_id = self.tree.insert("", tk.END, text=str(order_num), values=values, tags=(tag,))
                    if data.get('video_path'):
                        self.video_items[data['video_path']] = item_id
                    if data.get('output_file'):
                        self.video_to_output[data['video_path']] = data['output_file']
                    
                    # Store original data behind tree item (for fast DB save without parsing)
                    original_data = {}
                    if values:
                        try:
                            original_data['denoise_display'] = values[self.COLUMN_INDEX['denoise']] if len(values) > self.COLUMN_INDEX['denoise'] else ''
                            original_data['hard_rotate_display'] = values[self.COLUMN_INDEX['hard_rotate']] if len(values) > self.COLUMN_INDEX['hard_rotate'] else '0'
                            original_data['status_display'] = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else '-'
                            original_data['progress_display'] = values[self.COLUMN_INDEX['progress']] if len(values) > self.COLUMN_INDEX['progress'] else '-'
                            original_data['orig_size_display'] = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else '-'
                            original_data['new_size_display'] = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else '-'
                            original_data['size_change_display'] = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else '-'
                            original_data['duration_display'] = values[self.COLUMN_INDEX['duration']] if len(values) > self.COLUMN_INDEX['duration'] else '-'
                            original_data['frames_display'] = values[self.COLUMN_INDEX['frames']] if len(values) > self.COLUMN_INDEX['frames'] else '-'
                            original_data['completed_date'] = values[self.COLUMN_INDEX['completed_date']] if len(values) > self.COLUMN_INDEX['completed_date'] else ''
                        except Exception:
                            pass
                    if data.get('source_duration_seconds') is not None:
                        original_data['source_duration_seconds'] = data['source_duration_seconds']
                    if data.get('source_frame_count') is not None:
                        original_data['source_frame_count'] = data['source_frame_count']
                    if data.get('source_fps') is not None:
                        original_data['source_fps'] = data['source_fps']
                    if data.get('output_encoder_type'):
                        original_data['output_encoder_type'] = data['output_encoder_type']
                    if data.get('output_modified_timestamp') is not None:
                        original_data['output_modified_timestamp'] = data['output_modified_timestamp']
                    if data.get('new_size_bytes') is not None:
                        original_data['output_size_bytes'] = data['new_size_bytes']
                    if data.get('output_duration_seconds') is not None:
                        original_data['output_duration_seconds'] = data['output_duration_seconds']
                    if data.get('output_frame_count') is not None:
                        original_data['output_frame_count'] = data['output_frame_count']
                    if data.get('output_fps') is not None:
                        original_data['output_fps'] = data['output_fps']
                    elif data.get('output_duration_seconds') and data.get('output_frame_count'):
                        try:
                            original_data['output_fps'] = data['output_frame_count'] / data['output_duration_seconds']
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass
                    if data.get('source_size_bytes') is not None:
                        original_data['source_size_bytes'] = data['source_size_bytes']
                    if data.get('source_modified_timestamp') is not None:
                        original_data['source_modified_timestamp'] = data['source_modified_timestamp']
                    if data.get('cq') is not None:
                        original_data['cq'] = data['cq']
                    if data.get('vmaf') is not None:
                        original_data['vmaf'] = data['vmaf']
                    if data.get('psnr') is not None:
                        original_data['psnr'] = data['psnr']
                    if data.get('source_extra_metadata'):
                        original_data['source_extra_metadata'] = data['source_extra_metadata']
                    elif data.get('saved_video') and data['saved_video'].get('source_extra_metadata'):
                        original_data['source_extra_metadata'] = data['saved_video']['source_extra_metadata']
                    if data.get('output_extra_metadata'):
                        original_data['output_extra_metadata'] = data['output_extra_metadata']
                    elif data.get('saved_video') and data['saved_video'].get('output_extra_metadata'):
                        original_data['output_extra_metadata'] = data['saved_video']['output_extra_metadata']

                    # FASE 4: Store manual encoding parameters for pending quality checks
                    if data.get('status_code'):
                        original_data['status_code'] = data['status_code']
                    if data.get('manual_quality_check'):
                        original_data['manual_quality_check'] = data['manual_quality_check']
                    if data.get('denoise_enabled') is not None:
                        original_data['denoise_enabled'] = normalize_denoise_level(data['denoise_enabled'])
                    if data.get('hard_rotate_degrees') is not None:
                        original_data['hard_rotate_degrees'] = normalize_hard_rotate_degrees(data['hard_rotate_degrees'])
                    if data.get('manual_cq_range'):
                        original_data['manual_cq_range'] = data['manual_cq_range']
                    if data.get('manual_cq_value') is not None:
                        original_data['manual_cq_value'] = data['manual_cq_value']
                    
                    if original_data:  # Only store if data exists
                        self.merge_tree_item_meta(item_id, original_data)
                    
                    src_meta = data.get('source_extra_metadata')
                    out_meta = data.get('output_extra_metadata')
                    if (src_meta or out_meta) and data.get('video_path'):
                        try:
                            self.update_single_video_extra_metadata_in_db(
                                data['video_path'],
                                source_extra_metadata=src_meta,
                                output_extra_metadata=out_meta
                            )
                        except Exception:
                            pass
                    
                    # CRITICAL FIX: Load denoise_enabled into video_denoise_enabled dict!
                    # Without this, denoise settings are lost on app restart because:
                    # - save_state_to_db reads from video_denoise_enabled dict (line 866-876)
                    # - but load was only populating tree_item_data, NOT video_denoise_enabled dict
                    # This caused denoise settings to be overwritten with 0 on next save after restart
                    if data.get('denoise_enabled') is not None and data.get('video_path'):
                        with self.video_denoise_lock:
                            self.video_denoise_enabled[data['video_path']] = normalize_denoise_level(data['denoise_enabled'])
                    if data.get('video_path') is not None:
                        rotate_val = normalize_hard_rotate_degrees(data.get('hard_rotate_degrees', 0))
                        with self.video_hard_rotate_lock:
                            self.video_hard_rotate_degrees[data['video_path']] = rotate_val
                    
                    # Cache stat() values (for cold start optimization)
                    video_path = data.get('video_path')
                    if video_path and data.get('source_size_bytes') is not None:
                        # Cache source_size_bytes (stat()-ed during load)
                        cache_entry = {'source_size_bytes': data['source_size_bytes']}
                        # If source_modified_timestamp in result, cache it too
                        # Note: stat() result in process_video_data is in source_modified_current
                        # But not always added to result, as only used in warm start
                        # In cold start we stat() new videos too, but timestamp not always saved
                        # Try to read from result if available
                        if 'source_modified_timestamp' in data and data['source_modified_timestamp'] is not None:
                            cache_entry['source_modified_timestamp'] = data['source_modified_timestamp']
                        self.video_stat_cache[video_path] = cache_entry
                    
                    # Completed status check - először ezt ellenőrizzük
                    if is_status_completed(values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else "") and self.hide_completed.get():
                        try:
                            self.tree.detach(item_id)
                            self.hidden_items.add(item_id)
                        except (tk.TclError, KeyError, AttributeError):
                            pass
                    
                    # Subtitle files - gyermek elemként bekerülnek, automatikusan kibonthatók
                    # A korábbi verzióban is a hide_completed ellenőrzés UT*N volt, és ott működött
                    subtitle_files_list = data.get('subtitle_files', [])
                    # DEBUG: Ellenőrizzük, hogy a feliratok bekerültek-e
                    if subtitle_files_list:
                        if LOAD_DEBUG:
                            load_debug_log(f"Adding subtitles for {data.get('video_path', 'unknown')}: {len(subtitle_files_list)} subtitles")
                        # Logoljuk, hogy lássuk, hogy a feliratok bekerülnek-e
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"DEBUG: Adding {len(subtitle_files_list)} subtitles for {data.get('video_path', 'unknown')}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                    
                    # FONTOS: Csak akkor adjuk hozzá a feliratokat, ha az item_id nem detach-olva van
                    if subtitle_files_list and item_id:
                        # Ha van felirat, a szülő elem kibontható lesz (plusz ikon automatikusan megjelenik)
                        try:
                            # Ellenőrizzük, hogy a szülő elem létezik-e
                            if item_id:
                                # Ellenőrizzük, hogy a szülő elem valóban létezik a treeview-ban
                                # Ha a videó el van rejtve (detach), akkor nem lehet hozzá gyermek elemeket adni
                                try:
                                    self.tree.item(item_id)  # Ez hibát dob, ha az item_id nem létezik vagy detach-olva van
                                except tk.TclError:
                                    if LOAD_DEBUG:
                                        load_debug_log(f"  Item {item_id} does not exist or is detached, skipping subtitles")
                                    subtitle_files_list = []  # Ne próbáljuk hozzáadni, ha az item_id nem létezik vagy detach-olva van
                                
                                if subtitle_files_list:
                                    for subtitle_entry in subtitle_files_list:
                                        # Az invalid feliratok formátuma: (sub_path, lang_part, reason)
                                        # A valid feliratok formátuma: (sub_path, lang_part)
                                        if len(subtitle_entry) == 3:
                                            # Invalid felirat
                                            sub_path, lang_part, reason = subtitle_entry
                                            iso_code = normalize_language_code(lang_part)
                                            lang_display = f"* {lang_part if lang_part else 'UND'} ({iso_code}) - {reason}"
                                            tags = ("subtitle", "invalid_subtitle")
                                        else:
                                            # Valid felirat
                                            sub_path, lang_part = subtitle_entry
                                            iso_code = normalize_language_code(lang_part)
                                            lang_display = f"{lang_part if lang_part else 'UND'} ({iso_code})"
                                            tags = ("subtitle",)
                                        # Gyermek elemként beszúrás - a treeview automatikusan megjeleníti a plusz ikont
                                        # A plusz ikon automatikusan megjelenik, ha van gyermek elem
                                        sub_item_id = self.tree.insert(item_id, tk.END, text="", values=("", "", lang_display, "", "", "", "", "", "", "", "", "", "", ""), tags=tags)
                                        self.subtitle_items[sub_item_id] = (sub_path, lang_part)
                                        if LOAD_DEBUG:
                                            load_debug_log(f"  Added subtitle: {sub_path.name} ({lang_part})")
                                    # A treeview automatikusan megjeleníti a plusz ikont, ha van gyermek elem
                                    # Alapértelmezetten bezárt állapotban jelennek meg a gyermek elemek
                                    # Frissítjük a treeview-t, hogy biztosan megjelenjen a plusz ikon
                                    try:
                                        # Ellenőrizzük, hogy a gyermek elemek valóban bekerültek-e
                                        children = self.tree.get_children(item_id)
                                        if LOAD_DEBUG:
                                            load_debug_log(f"  Item {item_id} now has {len(children)} children")
                                        # Explicit frissítés, hogy biztosan megjelenjen a plusz ikon
                                        # További frissítés a root-on keresztül
                                        # Logoljuk, hogy lássuk, hogy a gyermek elemek bekerültek-e
                                    except Exception as update_e:
                                        if LOAD_DEBUG:
                                            load_debug_log(f"  Error updating treeview: {update_e}")
                        except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as sub_e:
                            log_file_check(f"[ERROR] Error adding subtitle items: {sub_e}")
                            if LOAD_DEBUG:
                                load_debug_log(f"  Error details: {sub_e}")
                except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as e:
                    log_file_check(f"[ERROR] Error during GUI update: {e}")
                    continue
            
            # Status update with loading progress
            if items_to_add:
                # Calculate ETA
                elapsed = time.time() - self.loading_progress.get('phase_start_time', time.time())
                if processed_count[0] > 0 and elapsed > 0:
                    avg_time_per_video = elapsed / processed_count[0]
                    remaining_videos = total_videos - processed_count[0]
                    eta_seconds = avg_time_per_video * remaining_videos
                    self.loading_progress['estimated_remaining_seconds'] = eta_seconds
                
                percent = int((processed_count[0] / total_videos) * 100) if total_videos > 0 else 0
                self.loading_progress['processed_files'] = processed_count[0]
                self.loading_progress['percent'] = percent
                
                # Format status text with ETA
                if self.loading_progress.get('estimated_remaining_seconds', 0) and self.loading_progress['estimated_remaining_seconds'] > 0:
                    eta = int(self.loading_progress['estimated_remaining_seconds'])
                    if eta >= 60:
                        eta_text = f" (~{eta // 60}:{eta % 60:02d} hátra)"
                    else:
                        eta_text = f" (~{eta} mp hátra)"
                else:
                    eta_text = ""
                
                self.loading_progress['phase_text'] = f"Feldolgozás: {processed_count[0]}/{total_videos} ({percent}%){eta_text}"
                self.status_label.config(text=f"Feldolgozás: {processed_count[0]}/{total_videos} ({percent}%){eta_text}")
                # Do not sort on every incremental update; final sort is done once.
        
        # Start parallel processing - update loading progress
        self.loading_progress['phase'] = 'processing'
        self.loading_progress['phase_text'] = f"Feldolgozás: 0/{total_videos} (0%)"
        self.loading_progress['phase_start_time'] = time.time()
        self.status_label.config(text=f"Feldolgozás: 0/{total_videos} (0%)")
        self.root.update_idletasks()  # Non-blocking update
        
        # Thread pool and futures
        executor = ThreadPoolExecutor(max_workers=max_workers)
        future_to_video = {executor.submit(process_video_data, vp): vp for vp in self.video_files}
        all_futures = list(future_to_video.keys())
        
        # Separate thread to handle futures completion (non-blocking)
        def collect_results():
            """Collects results in separate thread"""
            log_file_check("DEBUG: collect_results thread started")
            try:
                for future in as_completed(all_futures):
                    try:
                        data = future.result()
                        completed_data_queue.put(data)
                        video_path = future_to_video.get(future)
                        load_debug_log(f"Collector: done {video_path} | error={bool(data.get('error')) if isinstance(data, dict) else 'n/a'}")
                    except Exception as e:
                        log_file_check(f"[ERROR] Error retrieving thread result: {e}")
                        video_path = future_to_video.get(future)
                        error_data = build_load_error_result(video_path, e)
                        completed_data_queue.put(error_data)
            except Exception as e:
                log_file_check(f"[ERROR] Error in as_completed loop: {e}")
            finally:
                # Signal that everything is done
                load_debug_log("Collector: all futures processed, sending FINISHED signal")
                completed_data_queue.put({'finished': True})
        
        # Start collector thread
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()
        
        # Flag to signal completion
        final_update_called = [False]
        final_update_running = [False]
        
        # GUI update timer (non-blocking)
        def periodic_gui_update():
            """Timer-called GUI update"""
            # log_file_check(f"DEBUG: periodic_gui_update running (queue={completed_data_queue.qsize()})")
            # If final_gui_update already called, do nothing
            if final_update_called[0]:
                if LOAD_DEBUG:
                    load_debug_log("periodic_gui_update: final_update already called, exiting")
                return
            
            update_gui_from_queue(force=False)
            
            # Check if everything is done
            if collector_thread.is_alive():
                # Still running, continue timer (every 100ms to avoid blocking)
                if LOAD_DEBUG:
                    load_debug_log(f"periodic_gui_update: collector still running | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
                self.root.after(100, periodic_gui_update)
            else:
                # Finished, last update
                # Wait a bit for collector thread to finish last data
                if not completed_data_queue.empty():
                    if LOAD_DEBUG:
                        load_debug_log(f"periodic_gui_update: collector done but queue not empty (size={completed_data_queue.qsize()}), checking again in 50ms")
                    self.root.after(50, periodic_gui_update)
                elif not final_update_called[0]:  # Only if not yet called
                    final_update_called[0] = True  # Signal that we are calling it
                    if LOAD_DEBUG:
                        load_debug_log(f"periodic_gui_update: collector done, scheduling final_gui_update | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
                    self.root.after(200, final_gui_update)
        
        def final_gui_update():
            """Final GUI update - add all data"""
            # If already running, do nothing (avoid race condition)
            if final_update_running[0]:
                if LOAD_DEBUG:
                    load_debug_log("final_gui_update: already running, skipping")
                return
            # If called prematurely (items still in queue), wait
            if not completed_data_queue.empty() and processed_count[0] < total_videos:
                if LOAD_DEBUG:
                    load_debug_log(f"final_gui_update: queue not empty (size={completed_data_queue.qsize()}), rescheduling")
                self.root.after(50, final_gui_update)
                return
    
            final_update_running[0] = True  # Jelezzük, hogy fut
            if LOAD_DEBUG:
                load_debug_log(f"final_gui_update indul | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
            
            try:
                # Helper function to process remaining data
                def process_remaining_data():
                    try:
                        # Final GUI update - all remaining data (no time check, force=True)
                        load_debug_log("process_remaining_data: calling force update") if LOAD_DEBUG else None
                        update_gui_from_queue(force=True)
                        
                        # Check once more for remaining data
                        items_to_add = []
                        while not completed_data_queue.empty():
                            try:
                                data = completed_data_queue.get_nowait()
                                if data.get('finished'):
                                    continue
                                items_to_add.append(data)
                                processed_count[0] += 1
                            except queue.Empty:
                                break
                        
                        # Process data
                        if items_to_add:
                            if LOAD_DEBUG:
                                load_debug_log(f"process_remaining_data: processing extra {len(items_to_add)} items, counter={processed_count[0]}/{total_videos}")
                            for data in items_to_add:
                                # Ellenőrizzük a 'finished' jelzőt
                                if data.get('finished'):
                                    continue
                                
                                # Biztonsági ellenőrzés: values és tag kell legyen
                                if data.get('values') is None or data.get('tag') is None:
                                    log_file_check(f"* Hiányzó values vagy tag: {data.get('video_path', 'unknown')}")
                                    continue
                                
                                try:
                                    order_num = data.get('order_num', 0)
                                    values = data['values']
                                    tag = data['tag']
                                    
                                    # Ellenőrizzük, hogy még nincs-e már a táblázatban (duplikáció elkerülése)
                                    if data.get('video_path') and data['video_path'] in self.video_items:
                                        continue
                                    
                                    item_id = self.tree.insert("", tk.END, text=str(order_num), values=values, tags=(tag,))
                                    if data.get('video_path'):
                                        self.video_items[data['video_path']] = item_id
                                    if data.get('output_file'):
                                        self.video_to_output[data['video_path']] = data['output_file']
                                    
                                    # Subtitle files - gyermek elemként bekerülnek, automatikusan kibonthatók
                                    # FONTOS: A feliratok hozzáadása a hide_completed ellenőrzés EL*TT kell történjen,
                                    # mert ha a videó el van rejtve (detach), akkor nem lehet hozzá gyermek elemeket adni
                                    subtitle_files_list = data.get('subtitle_files', [])
                                    if subtitle_files_list:
                                        # Ha van felirat, a szülő elem kibontható lesz (plusz ikon automatikusan megjelenik)
                                        try:
                                            if item_id:
                                                for subtitle_entry in subtitle_files_list:
                                                    # Az invalid feliratok formátuma: (sub_path, lang_part, reason)
                                                    # A valid feliratok formátuma: (sub_path, lang_part)
                                                    if len(subtitle_entry) == 3:
                                                        # Invalid felirat
                                                        sub_path, lang_part, reason = subtitle_entry
                                                        iso_code = normalize_language_code(lang_part)
                                                        lang_display = f"* {lang_part if lang_part else 'UND'} ({iso_code}) - {reason}"
                                                        tags = ("subtitle", "invalid_subtitle")
                                                    else:
                                                        # Valid felirat
                                                        sub_path, lang_part = subtitle_entry
                                                        iso_code = normalize_language_code(lang_part)
                                                        lang_display = f"{lang_part if lang_part else 'UND'} ({iso_code})"
                                                        tags = ("subtitle",)
                                                    # Gyermek elemként beszúrás - a treeview automatikusan megjeleníti a plusz ikont
                                                    # A plusz ikon automatikusan megjelenik, ha van gyermek elem
                                                    sub_item_id = self.tree.insert(item_id, tk.END, text="", values=("", "", lang_display, "", "", "", "", "", "", "", "", "", "", ""), tags=tags)
                                                    self.subtitle_items[sub_item_id] = (sub_path, lang_part)
                                                # A treeview automatikusan megjeleníti a plusz ikont, ha van gyermek elem
                                                # Alapértelmezetten bezárt állapotban jelennek meg a gyermek elemek
                                                # Frissítjük a treeview-t, hogy biztosan megjelenjen a plusz ikon
                                                try:
                                                    self.tree.update_idletasks()
                                                except Exception:
                                                    pass
                                        except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as sub_e:
                                            log_file_check(f"[ERROR] Error adding subtitle items in process_remaining_data: {sub_e}")
                                    
                                    # Completed státusz ellenőrzés - először ezt ellenőrizzük
                                    if is_status_completed(values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else "") and self.hide_completed.get():
                                        try:
                                            self.tree.detach(item_id)
                                            self.hidden_items.add(item_id)
                                        except (tk.TclError, KeyError, AttributeError):
                                            pass
                                except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as e:
                                    log_file_check(f"* Hiba GUI frissítés során: {e}")
                                    continue
                            
                            # Update final status - ensuring total_videos is reached
                            self.status_label.config(text=f"{t('status_processing_videos')}: {processed_count[0]}/{total_videos}")
                        
                        # Finishing operations
                        finish_loading()
                    except Exception as e:
                        log_file_check(f"[ERROR] Error during process_remaining_data: {e}")
                        import traceback
                        log_file_check(traceback.format_exc())
                        finish_loading()
                
                def finish_loading():
                    """Finishes loading"""
                    try:
                        # Ensure source_path and dest_path are set
                        if not hasattr(self, 'source_path') or not self.source_path:
                            source = self.source_entry.get()
                            if source:
                                self.source_path = Path(source)
                        if not hasattr(self, 'dest_path') or not self.dest_path:
                            dest = self.dest_entry.get()
                            if dest:
                                self.dest_path = Path(dest)
                        
                        if LOAD_DEBUG:
                            load_debug_log(f"finish_loading: processed={processed_count[0]}/{total_videos} | videos in tree={len(self.video_items)} | queue={completed_data_queue.qsize()}")
                            if processed_count[0] < total_videos:
                                missing_videos = []
                                try:
                                    for video_path in self.video_files:
                                        if video_path not in self.video_items:
                                            missing_videos.append(str(video_path))
                                            if len(missing_videos) >= 10:
                                                break
                                except Exception as debug_exc:
                                    missing_videos.append(f"<error creating missing list: {debug_exc}>")
                                load_debug_log(f"finish_loading: estimating missing items ({total_videos - processed_count[0]} pcs). Example: {missing_videos}")
                            if getattr(self, 'last_load_errors', None):
                                err_count = len(self.last_load_errors)
                                if err_count:
                                    load_debug_log(f"Total load errors: {err_count}")
                                    for video_name, status_text in self.last_load_errors[:20]:
                                        load_debug_log(f"  - {video_name}: {status_text}")
                                    if err_count > 20:
                                        load_debug_log(f"  ... +{err_count - 20} more errors")
                        # Shutdown executor
                        try:
                            executor.shutdown(wait=False)
                        except Exception as e:
                            log_file_check(f"[ERROR] Error during executor shutdown: {e}")
                        
                        # Final sort by order_num (ABC order)
                        self._sort_tree_by_order_num()
                        
                        # Finishing operations
                        self.update_summary_row()
                        
                        # Calculate total loading time
                        total_load_time = time.time() - self.loading_progress.get('start_time', time.time())
                        if total_load_time >= 60:
                            time_text = f"{int(total_load_time // 60)}:{int(total_load_time % 60):02d}"
                        else:
                            time_text = f"{total_load_time:.1f} mp"
                        
                        # Update loading progress - finished
                        self.loading_progress['phase'] = 'finished'
                        self.loading_progress['phase_text'] = f"Kész: {len(self.video_files)} videó ({time_text})"
                        self.loading_progress['percent'] = 100
                        self.loading_progress['processed_files'] = len(self.video_files)
                        self.loading_progress['estimated_remaining_seconds'] = 0
                        
                        self.status_label.config(text=f"Kész: {len(self.video_files)} videó betöltve ({time_text})")
                        self.is_loading_videos = False
                        
                        # Hide completed items if checkbox is enabled
                        if self.hide_completed.get():
                            self.toggle_hide_completed()
                        
                        # Debug: check video_items state
                        if LOAD_DEBUG:
                            pending_count = 0
                            completed_count = 0
                            for video_path, item_id in self.video_items.items():
                                try:
                                    tags = self.tree.item(item_id, 'tags') or ()
                                    current_values = self.tree.item(item_id, 'values')
                                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                                    if any(tag in ('pending', 'encoding_nvenc', 'encoding_svt', 'needs_check', 'needs_check_nvenc', 'needs_check_svt') for tag in tags):
                                        pending_count += 1
                                    elif any(tag in ('completed', 'completed_copy') for tag in tags):
                                        completed_count += 1
                                except Exception:
                                    pass
                            load_debug_log(f"finish_loading: video_items={len(self.video_items)} | pending={pending_count} | completed={completed_count}")
                        
                        # Ensure Start button enabled after load if videos loaded
                        # and no encoding in progress
                        if getattr(self, 'start_button', None) and not self.is_encoding:
                            if self.video_items or self.video_files:
                                # If videos exist, activate button (has_pending_tasks() already checked)
                                has_pending = self.has_pending_tasks()
                                if LOAD_DEBUG:
                                    load_debug_log(f"finish_loading: start_button activation | has_pending={has_pending} | video_items={len(self.video_items)} | video_files={len(self.video_files)}")
                                if has_pending or self.video_items:
                                    self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                                    if LOAD_DEBUG:
                                        load_debug_log(f"finish_loading: start_button activated (state={self.start_button.cget('state')})")
                        
                        # Update start button state (confirms state, doesn't overwrite)
                        self.update_start_button_state()
    
                        # If auto load requested during Start button press, encoding can start now
                        if getattr(self, 'auto_start_after_load', False):
                            has_items = bool(self.video_items)
                            has_tasks = self.has_pending_tasks() if has_items else False
                            if has_items and has_tasks:
                                if LOAD_DEBUG:
                                    load_debug_log("finish_loading: auto_start_after_load -> scheduling start_encoding")
                                self.auto_start_after_load = False
    
                                def _delayed_start():
                                    # Only start if no reload or encoding happened in the meantime
                                    if not self.is_encoding and not self.is_loading_videos:
                                        self.start_encoding()
    
                                self.root.after(150, _delayed_start)
                            else:
                                if LOAD_DEBUG:
                                    load_debug_log("finish_loading: auto_start flag cleared (no loaded tasks)")
                                self.auto_start_after_load = False
                        if getattr(self, 'immediate_stop_button', None):
                            self.immediate_stop_button.config(state=tk.DISABLED)
                        self.progress_bar['maximum'] = len(self.video_files)
                        self.progress_bar['value'] = 0
                        log_file_check(f"Successfully loaded: {len(self.video_files)} videos.")
                        
                        # Check and fix misnamed .av1.mkv copies after loading
                        try:
                            fixed_count = self.check_and_fix_misnamed_copies()
                            if fixed_count > 0:
                                log_file_check(f"[OK] {fixed_count} misnamed copies fixed")
                                self.update_summary_row()  # Refresh summary to show corrected files
                        except Exception as e:
                            log_file_check(f"[ERROR] Error during misnamed copy check: {e}")
                        
                        # Database save in background thread to avoid blocking GUI update
                        # Ensure at least source_path is set (dest_path is optional for saving)
                        if hasattr(self, 'source_path') and self.source_path:
                            # THREAD-SAFETY FIX: Pre-cache settings on GUI thread before DB thread
                            _load_settings_snapshot = self._build_settings_snapshot()
                            def save_db_in_background():
                                thread_start_time = time.time()
                                try:
                                    if LOAD_DEBUG:
                                        load_debug_log(f"Database save starting in background thread | source={self.source_path} | dest={self.dest_path} | db_path={self.db_path}")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[save_db_in_background] Database save starting in background thread | source={self.source_path} | dest={self.dest_path} | db_path={self.db_path}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                    # Sending message that database save started
                                    if hasattr(self, 'encoding_queue'):
                                        self.encoding_queue.put(("db_progress", f"Saving database state: {self.db_path.name}"))
                                    
                                    # Progress callback for post-cold-start DB save
                                    def progress_callback(msg):
                                        """Progress callback for database save"""
                                        try:
                                            if hasattr(self, 'encoding_queue'):
                                                self.encoding_queue.put_nowait(("db_progress", msg))
                                        except queue.Full:
                                            pass
                                    

                                    self.save_state_to_db(progress_callback=progress_callback, settings_snapshot=_load_settings_snapshot)
                                    thread_duration = time.time() - thread_start_time
                                    # Check if database was actually created
                                    if self.db_path.exists():
                                        file_size = self.db_path.stat().st_size
                                        if LOAD_DEBUG:
                                            load_debug_log(f"[OK] Database save in background thread finished | file: {self.db_path} | size: {file_size} bytes | duration: {thread_duration:.2f}s")
                                        # Sending message that database save finished successfully
                                        if hasattr(self, 'encoding_queue'):
                                            self.encoding_queue.put(("db_saved",))
                                            # Post-cold-start auto save - not showing notification
                                        # Set flag: post-load DB save finished
                                        if hasattr(self, 'load_db_save_completed'):
                                            self.load_db_save_completed.set()
                                    else:
                                        error_msg = f"[ERROR] Database file not created: {self.db_path}"
                                        if LOAD_DEBUG:
                                            load_debug_log(error_msg)
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(f"[ERROR] [save_db_in_background] {error_msg} | duration: {thread_duration:.2f}s\n")
                                                LOG_WRITER.flush()
                                            except Exception:
                                                pass
                                except Exception as e:
                                    thread_duration = time.time() - thread_start_time
                                    error_msg = f"[ERROR] Error during database save (background thread): {e}"
                                    if LOAD_DEBUG:
                                        load_debug_log(error_msg)
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[ERROR] [save_db_in_background] {error_msg} | duration: {thread_duration:.2f}s\n")
                                            import traceback
                                            LOG_WRITER.write(traceback.format_exc())
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                    # Send message even on error
                                    if hasattr(self, 'encoding_queue'):
                                        self.encoding_queue.put(("db_error", f"[ERROR] Database save error: {e}"))
                                        # Post-cold-start auto save error - not showing notification
                                finally:
                                    # Log that thread finished (always)
                                    thread_duration = time.time() - thread_start_time
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[save_db_in_background] Background thread finished | duration: {thread_duration:.2f}s\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                            
                            # Starting background thread
                            db_thread = self._start_db_thread(save_db_in_background, name="SaveDBBackground")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[load_videos] DB save background thread started (daemon=True)\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            if LOAD_DEBUG:
                                load_debug_log(f"DB save background thread started (daemon=True)")
                        else:
                            if LOAD_DEBUG:
                                load_debug_log(f"Database save skipped: source_path or dest_path not set")
                        
                        # FASE 2: Check for pending manual quality checks after loading finishes
                        # Delayed to ensure tree and all data structures are fully populated
                        self.root.after(1000, self.check_pending_manual_quality_checks)
                        
                    except Exception as e:
                        log_file_check(f"[ERROR] Error during finishing operations: {e}")
                
                # Call process_remaining_data() to process remaining data and call finish_loading()
                process_remaining_data()
            except Exception as e:
                log_file_check(f"[ERROR] Critical error during final_gui_update: {e}")
                import traceback
                log_file_check(traceback.format_exc())
                # Try to finish loading even on error
                try:
                    finish_loading()
                except Exception:
                    pass
        
        # Starting timer
        self.root.after(100, periodic_gui_update)

    def calculate_file_sizes(self, video_path, output_file):
        """Calculate file sizes in MB and change percentage"""
        if not video_path.exists():
            return 0, 0, 0
        orig_size_mb = video_path.stat().st_size / (1024**2)
        new_size_mb = output_file.stat().st_size / (1024**2) if output_file.exists() else 0
        change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100 if orig_size_mb > 0 else 0
        return orig_size_mb, new_size_mb, change_percent

    def check_and_fix_misnamed_copies(self):
        """Check and fix misnamed .av1.mkv copies after loading.
        
        Detects .av1.mkv files that are actually unchanged copies (same size as source)
        and renames them to original extension. Also ensures subtitles are copied.
        
        Returns:
            int: Number of files fixed.
        """
        fixed_count = 0
        
        for video_path, item_id in list(self.video_items.items()):
            output_file = self.video_to_output.get(video_path)
            
            if not output_file or not output_file.exists():
                continue
            
            if not is_misnamed_copy(video_path, output_file):
                continue
            
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n🔄 Misnamed copy detected: {output_file.name}\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
            
            new_output = rename_misnamed_copy_file(output_file, video_path)
            
            if not new_output:
                continue
            
            self.video_to_output[video_path] = new_output
            
            try:
                subs_copied = verify_and_copy_subtitles(video_path, new_output)
            except Exception:
                pass
            
            try:
                try:
                    orig_size_mb = video_path.stat().st_size / (1024**2)
                    new_size_mb = new_output.stat().st_size / (1024**2)
                    orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    new_size_display = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                except (OSError, ValueError):
                    orig_size_display = "-"
                    new_size_display = "-"
                
                completed_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.tree.set(item_id, 'status', t('status_completed_copy'))
                self.tree.set(item_id, 'orig_size', orig_size_display)
                self.tree.set(item_id, 'new_size', new_size_display)
                self.tree.set(item_id, 'size_change', "0%")
                self.tree.set(item_id, 'completed_date', completed_date)
                self.tree.item(item_id, tags=('completed_copy',))
                
                self.set_tree_item_meta(item_id, status_code='completed_copy')
            except (tk.TclError, KeyError, AttributeError):
                pass
            
            fixed_count += 1
        
        return fixed_count

    def _parse_size_string_to_bytes(self, size_str):
        """Parse size string to bytes, handling MB, GB, TB units"""
        if not size_str or size_str == "-":
            return None
        try:
            # Remove spaces and convert to uppercase for easier matching
            clean = size_str.strip().upper()
            
            # Try to extract number and unit
            # Handle localized format (comma as decimal separator)
            clean_normalized = clean.replace(',', '.')
            
            # Match patterns like "123.45 MB", "1,234.56 GB", etc.
            match = re.match(r'([\d.,]+)\s*(KB|MB|GB|TB|B)', clean_normalized)
            if match:
                number_str = match.group(1).replace(',', '')
                unit = match.group(2).upper()
                number = float(number_str)
                
                if unit == 'TB':
                    return int(number * (1024 ** 4))
                elif unit == 'GB':
                    return int(number * (1024 ** 3))
                elif unit == 'MB':
                    return int(number * (1024 ** 2))
                elif unit == 'KB':
                    return int(number * 1024)
                elif unit == 'B':
                    return int(number)
            
            # Fallback: try to parse as MB (old format)
            clean = clean.replace("MB", "").replace("GB", "").replace("TB", "").replace("KB", "").replace("B", "").strip()
            clean = clean.replace(',', '.')
            value = float(clean)
            # Assume MB if no unit found
            return int(value * (1024 ** 2))
        except (ValueError, TypeError, AttributeError):
            return None

    # =============================================================================
    # FASE 2: Check and queue pending manual quality checks after video loading
    # =============================================================================
    
    def check_pending_manual_quality_checks(self):
        """
        Check all loaded videos for pending manual quality checks.
        If a video is completed but has manual_quality_check set and no VMAF/PSNR,
        queue the quality check automatically.
        
        Called after load_videos() finishes to resume interrupted quality checks.
        """
        # LOG_WRITER is available via gui_imports (proxy), messagebox via tkinter import

        # Only check if we have video items loaded
        if not self.video_items:
            return
        
        pending_checks = []
        
        # Iterate through loaded videos in tree
        for video_path, item_id in self.video_items.items():
            try:
                # Get tree item data
                values = self.tree.item(item_id, 'values')
                if not values or len(values) < 13:
                    continue
                
                # Extract status and quality values
                status_text = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ''
                status_code = self.get_tree_item_meta(item_id, 'status_code', '')
                vmaf_str = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else '-'
                psnr_str = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else '-'
                
                # Check item data for manual_quality_check
                manual_quality_check = self.get_tree_item_meta(item_id, 'manual_quality_check')
                
                # Check if this video needs quality check
                if (manual_quality_check and 
                    manual_quality_check in ('vmaf', 'psnr', 'both') and
                    status_code in ('completed', 'completed_nvenc', 'completed_svt')):
                    
                    # Check if VMAF/PSNR is missing
                    needs_vmaf = (manual_quality_check in ('vmaf', 'both') and 
                                 (not vmaf_str or vmaf_str == '-'))
                    needs_psnr = (manual_quality_check in ('psnr', 'both') and 
                                 (not psnr_str or psnr_str == '-'))
                    
                    if needs_vmaf or needs_psnr:
                        # Found a video that needs quality check
                        pending_checks.append({
                            'video_path': video_path,
                            'item_id': item_id,
                            'quality_check': manual_quality_check,
                            'needs_vmaf': needs_vmaf,
                            'needs_psnr': needs_psnr,
                            'values': values
                        })
            
            except Exception as e:
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[ERROR] Error checking pending quality check for {video_path}: {e}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
        
        # If we found pending checks, inform user and queue them
        if pending_checks:
            count = len(pending_checks)
            msg = (f"Újraindítás után {count} videó vár VMAF/PSNR számításra.\n\n"
                   f"Akarod most elindítani a minőség ellenőrzést?")
            
            result = messagebox.askyesno(
                "VMAF/PSNR számítás folytatása",
                msg
            )
            
            if result:
                # Queue all pending checks
                for check_info in pending_checks:
                    self._queue_pending_quality_check(check_info)
                
                # Start SVT workers if not running
                self.root.after(0, self._ensure_svt_workers_running)
                
                # Update status
                self.status_label.config(
                    text=f"{count} VMAF/PSNR számítás elindítva..."
                )
                
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[OK] {count} pending quality check(s) queued for resumption\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

    def _queue_pending_quality_check(self, check_info):
        """Queue a single pending quality check."""
        # LOG_WRITER, t, get_output_filename are available via gui_imports (module-level wildcard import)
        
        video_path = check_info['video_path']
        item_id = check_info['item_id']
        
        try:
            # Get output file
            output_file = self.video_to_output.get(video_path)
            if not output_file:
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
            
            if not output_file or not output_file.exists():
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] Pending quality check: output file not found for {video_path}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                return
            
            # Extract data from values
            values = check_info['values']
            orig_size_str = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else '-'
            cq_str = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else '-'
            vmaf_str = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else '-'
            psnr_str = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else '-'
            new_size_str = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else '-'
            change_str = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else '-'
            completed_date = values[self.COLUMN_INDEX['completed_date']] if len(values) > self.COLUMN_INDEX['completed_date'] else ''
            status_text = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else 'Completed'
            
            # Get status_code from tree_item_data
            status_code = self.get_tree_item_meta(item_id, 'status_code', 'completed')
            quality_check_type = check_info['quality_check']
            
            # Queue to SVT for VMAF/PSNR calculation
            self.add_to_svt_queue(
                video_path=video_path,
                item_id=item_id,
                task_type='vmaf',
                is_manual=True,  # Treat as manual priority
                output_file=output_file,
                orig_size_str=orig_size_str,
                check_vmaf=check_info['needs_vmaf'],
                check_psnr=check_info['needs_psnr'],
                current_cq_str=cq_str,
                current_vmaf_str=vmaf_str,
                current_psnr_str=psnr_str,
                current_new_size_str=new_size_str,
                current_size_change=change_str,
                current_completed_date=completed_date,
                current_status=status_text,
                quality_check_type=quality_check_type,
                final_status_code=status_code,
                reason='resume_pending_manual_quality_check'
            )
            
            # Update status in tree
            if quality_check_type == 'vmaf':
                pending_status = t('status_vmaf_pending')
            elif quality_check_type == 'psnr':
                pending_status = t('status_psnr_pending')
            else:  # 'both'
                pending_status = t('status_vmaf_psnr_pending')
            
            self.encoding_queue.put((
                "update", item_id, pending_status, cq_str, vmaf_str, psnr_str,
                "100%", orig_size_str, new_size_str, change_str, completed_date,
                {'status_code': normalize_status_to_code(pending_status)}
            ))
            self.encoding_queue.put(("tag", item_id, "pending"))
            
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[OK] Pending quality check queued: {video_path} -> {quality_check_type}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[ERROR] Error queueing pending quality check for {video_path}: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
        
