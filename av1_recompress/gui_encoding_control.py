from .gui_imports import *
from .gui_shared import *

class EncodingControlMixin:
    def _build_manual_queue_status_text(self, base_status_text, manual_cq_value=None, manual_quality_check=None):
        """Preserve manual re-encode suffix for queued rows."""
        base_status_text = str(base_status_text or "")
        if not base_status_text or " (M " in base_status_text:
            return base_status_text
        if manual_cq_value is None:
            return base_status_text
        try:
            cq_value = int(manual_cq_value)
        except (TypeError, ValueError):
            return base_status_text

        if hasattr(self, '_format_manual_status_suffix'):
            try:
                return base_status_text + self._format_manual_status_suffix(cq_value, manual_quality_check)
            except Exception:
                pass

        quality_check = str(manual_quality_check or '').strip().upper()
        return f"{base_status_text} (M CQ:{cq_value}{(' ' + quality_check) if quality_check else ''})"

    def set_encoding_state(
        self,
        is_encoding: bool | None = None,
        encoding_worker_running: bool | None = None,
        graceful_stop_requested: bool | None = None
    ) -> None:
        """Thread-safe setter for encoding state variables.

        Args:
            is_encoding: New value for is_encoding flag (None to skip).
            encoding_worker_running: New value for encoding_worker_running flag.
            graceful_stop_requested: New value for graceful_stop_requested flag.
        """
        # J6: Timeout-védelem - 5 másodperc után feladjuk, nem deadlockolunk
        acquired = self.encoding_state_lock.acquire(timeout=5.0)
        if not acquired:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write("[ERROR] encoding_state_lock acquire timeout (5s) in set_encoding_state\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            return
        try:
            if is_encoding is not None:
                self.is_encoding = is_encoding
            if encoding_worker_running is not None:
                self.encoding_worker_running = encoding_worker_running
            if graceful_stop_requested is not None:
                self.graceful_stop_requested = graceful_stop_requested
        finally:
            self.encoding_state_lock.release()

    def get_encoding_state(self) -> tuple[bool, bool, bool]:
        """Thread-safe getter for encoding state variables.

        Returns:
            tuple: (is_encoding, encoding_worker_running, graceful_stop_requested)
        """
        # J6: Timeout-védelem - 5 másodperc után alapértelmezett értékekkel tér vissza
        acquired = self.encoding_state_lock.acquire(timeout=5.0)
        if not acquired:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write("[ERROR] encoding_state_lock acquire timeout (5s) in get_encoding_state\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            return (False, False, False)
        try:
            return (
                self.is_encoding,
                self.encoding_worker_running,
                self.graceful_stop_requested
            )
        finally:
            self.encoding_state_lock.release()

    def start_encoding(self):
        """Start the encoding process.

        Initiates the encoding workflow:
        1. Copies non-video files.
        2. Saves current state to database.
        3. Starts worker threads (NVENC, SVT-AV1).
        """
        # J4: Védelem - ha stop szál még fut, nem indítható új kódolás
        if getattr(self, '_stop_thread_running', False):
            return

        skip_queue_clear = getattr(self, '_skip_queue_clear_on_start', False)
        self._skip_queue_clear_on_start = False

        # Ha még tart a betöltés, soroljuk be automatikus indulásra
        if self.is_loading_videos:
            self.auto_start_after_load = True
            return
    
        # Ha nincs semmilyen videó a táblázatban, de a forrás/cél mappa ki van töltve,
        # automatikusan töltsük be a listát, majd induljon a kódolás.
    
        if not self.video_items:
            source_path_str = self.source_entry.get()
            dest_path_str = self.dest_entry.get()
            if source_path_str and os.path.exists(source_path_str) and dest_path_str:
                self.auto_start_after_load = True
                self.load_videos()
                return
            else:
                # Nincs semmilyen videó, és nincs forrás sem -> figyelmeztetés
                messagebox.showwarning(t('msg_warning'), t('msg_no_videos'))
                return

        # Normal start must not mirror worker console output into main log.
        for logger in getattr(self, 'svt_loggers', []) + getattr(self, 'nvenc_loggers', []):
            try:
                logger.mirror_to_main_log = False
            except Exception:
                pass

        # Tool path preflight: fail fast with clear message instead of silent probe/fallback loops.
        try:
            self.apply_tool_paths_from_gui()
        except Exception:
            pass

        missing_tools = []
        ffmpeg_path = (self.ffmpeg_path.get().strip() if hasattr(self, 'ffmpeg_path') else '')
        abav1_path = (self.abav1_path.get().strip() if hasattr(self, 'abav1_path') else '')
        vdub_path = (self.virtualdub_path.get().strip() if hasattr(self, 'virtualdub_path') else '')

        ffmpeg_ok = (ffmpeg_path and Path(ffmpeg_path).exists()) or bool(shutil.which("ffmpeg")) or bool(shutil.which("ffmpeg.exe"))
        abav1_ok = (abav1_path and Path(abav1_path).exists()) or bool(shutil.which("ab-av1")) or bool(shutil.which("ab-av1.exe"))
        vdub_ok = (vdub_path and Path(vdub_path).exists()) or bool(shutil.which("vdub2")) or bool(shutil.which("vdub2.exe")) or bool(shutil.which("vdub64")) or bool(shutil.which("vdub64.exe"))

        if not ffmpeg_ok:
            missing_tools.append("ffmpeg")
        if not abav1_ok:
            missing_tools.append("ab-av1")
        if not vdub_ok:
            missing_tools.append("VirtualDub2")

        if missing_tools:
            msg = (
                "Hiányzó vagy hibás eszközútvonal(ak): "
                + ", ".join(missing_tools)
                + ".\nEllenőrizd a beállításokat, majd indítsd újra."
            )
            try:
                messagebox.showerror(t('msg_error'), msg)
            except Exception:
                pass
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[ERROR] Start aborted due to missing tool paths: {missing_tools}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            self.status_label.config(text=msg)
            return
        
        
        # Check and fix misnamed .av1.mkv copies before encoding starts
        try:
            fixed_count = self.check_and_fix_misnamed_copies()
            if fixed_count > 0 and LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n[OK] {fixed_count} misnamed copy fixed\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n[WARN] Error checking misnamed copies: {e}\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
        
        # CRITICAL FIX: STOP_EVENT is thread-safe, no lock needed
        STOP_EVENT.clear()
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        self.set_encoding_state(graceful_stop_requested=False)
    
        # LIST-BASED QUEUE: Clear pending SVT tasks (will be reloaded from tree)
        # Skip clearing if called from _start_remaining_pending_files() to preserve
        # the already-queued manual task.
        if not skip_queue_clear:
            self.clear_all_svt_tasks()
    
        self._refresh_encoding_worker_snapshot()
    
        DEBUG_MODE.set(self.debug_mode.get())
        
        # SVT-AV1 queue-ban lévő videók betöltése a list-alapú SVT queue-ba
        initial_min_vmaf = self.current_min_vmaf
        vmaf_step = self.current_vmaf_step
        max_encoded = self.current_max_encoded_percent
        resize_enabled = self.current_resize_enabled
        resize_height = self.current_resize_height
        
        # Pre-read tkinter variables for background thread (thread safety)
        # CRITICAL: tkinter .get() is NOT thread-safe — Tcl interpreter mutex deadlock
        # if called from a non-GUI thread while GUI event loop is running.
        nvenc_enabled = self.current_nvenc_enabled
        audio_compression_enabled = self.current_audio_compression_enabled
        audio_compression_method = self.current_audio_compression_method
        vdub_validation_disabled = self.vdub_validation_disabled.get() if hasattr(self, 'vdub_validation_disabled') else False
        configured_nvenc_workers = self.get_configured_nvenc_workers()
        configured_svt_workers = self.get_configured_svt_workers()

        # Queue loading state
        self.is_queue_loading = True
        self.queue_loading_stop_requested = False

        # Gombok AZONNALI átállítása (queue feltöltés ELŐTT)
        self.start_button.config(text=t('btn_stop'), command=self.stop_queue_loading_or_encoding, state=tk.NORMAL)
        self.immediate_stop_button.config(state=tk.NORMAL)
        self.load_videos_btn.config(state=tk.DISABLED)

        # Encoding state AZONNALI beállítása
        with self.encoding_state_lock:
            self.is_encoding = True
            self.encoding_worker_running = True
            self.current_video_index = -1

        # KRITIKUS: Pre-read MINDEN tree adat a GUI szálban (tkinter szál-biztonság!)
        # A háttérszál NEM hívhat self.tree.item()-et!
        item_id_to_path = {v: k for k, v in self.video_items.items()}
        tree_children = self.tree.get_children()

        pre_read_items = []
        for item_id in tree_children:
            if item_id not in item_id_to_path:
                continue
            try:
                values = list(self.tree.item(item_id, 'values'))
                tags = list(self.tree.item(item_id, 'tags') or ())
            except (tk.TclError, RuntimeError):
                continue
            pre_read_items.append({
                'item_id': item_id,
                'video_path': item_id_to_path[item_id],
                'values': values,
                'tags': tags,
            })

        total_videos_to_check = len(pre_read_items)

        # Status frissítés
        self.status_label.config(
            text=t('status_queue_filling').format(current="0", total=f"{total_videos_to_check:,}")
        )

        # check_encoding_queue KORAI indítás (a háttérszál üzenetei EZZEL dolgozódnak fel)
        self.root.after(100, self.check_encoding_queue)

        # Queue feltöltés HÁTTÉRSZÁLBAN (tkinter-mentes, megszakítható)
        queue_thread = threading.Thread(
            target=self._queue_loading_thread,
            args=(pre_read_items, initial_min_vmaf, vmaf_step, max_encoded,
                  resize_enabled, resize_height, nvenc_enabled,
                  audio_compression_enabled, audio_compression_method,
                  vdub_validation_disabled, configured_nvenc_workers,
                  configured_svt_workers),
            daemon=True,
            name="QueueLoadingThread"
        )
        queue_thread.start()
        self.queue_loading_thread = queue_thread
        
        # 1. LÉPÉS: Nem-videó fájlok másolása (ha van cél mappa)
        if self.dest_path:
            def copy_callback(msg):
                try:
                    # Tuple formátum: (típus, total, current, message)
                    if isinstance(msg, tuple):
                        self.encoding_queue.put_nowait(("copy_progress", msg))
                    else:
                        # Régi formátum kompatibilitás
                        self.encoding_queue.put_nowait(("copy_status", msg))
                except queue.Full:
                    pass

            def copy_files_sync():
                """Szinkron másolás - a Start gomb után azonnal"""
                try:
                    copy_non_video_files(self.source_path, self.dest_path, copy_callback, stop_event=STOP_EVENT)
                except Exception as e:
                    try:
                        copy_callback(("copy_error", t('copy_error').format(error=e)))
                    except Exception:
                        pass

            # Másolás aszinkron módon (hogy lássuk a progressbar-t és ne fagyjon be)
            copy_thread = threading.Thread(target=copy_files_sync, daemon=True)
            copy_thread.start()
            self.copy_thread = copy_thread
            # FIRE-AND-FORGET: ne blokkoljuk a GUI thread-et a másolásra várva

        # 2. LÉPÉS: Adatbázis mentés (aszinkron módon, hogy ne blokkolja a GUI-t)
        # Notification-ban jelenítjük meg, ne a status_label-ban
        from .gui_shared import is_app_closing
        try:
            if not is_app_closing():
                self.root.after(0, lambda: self.db_notification_label.config(text=t('db_saving'), foreground="blue"))
        except (tk.TclError, AttributeError):
            pass

        # Ellenőrizzük, hogy már fut-e DB mentés (hidegindítás után)
        # Ha fut, várunk rá, hogy ne legyen lock ütközés
        db_save_in_progress = False
        try:
            # Próbáljuk meg lockolni a DB-t (non-blocking)
            if self.db_lock.acquire(blocking=False):
                # Nincs futó DB mentés, szabadon használhatjuk
                self.db_lock.release()
            else:
                # Már fut DB mentés, várunk rá
                db_save_in_progress = True
                try:
                    if not is_app_closing():
                        self.root.after(0, lambda: self.db_notification_label.config(text=t('db_saving_waiting'), foreground="orange"))
                except (tk.TclError, AttributeError):
                    pass
        except Exception:
            pass

        # THREAD-SAFETY FIX: Pre-cache all tkinter settings on GUI thread
        # before starting the DB save background thread. This avoids calling
        # self.*.get() from a non-GUI thread which can deadlock the Tcl interpreter.
        _settings_snapshot = self._build_settings_snapshot()

        def save_db_async():
            """Adatbázis mentés aszinkron módon (nem blokkolja a GUI thread-et)."""
            try:
                # Ha már fut DB mentés, várunk rá
                if db_save_in_progress:
                    # Várunk maximum 5 percet a korábbi DB mentés befejezésére
                    wait_start = time.time()
                    wait_timeout = 300  # 5 perc
                    while not self.db_lock.acquire(blocking=False):
                        if time.time() - wait_start > wait_timeout:
                            try:
                                self.encoding_queue.put_nowait(("db_error", f"[WARN] {t('db_timeout')}"))
                            except queue.Full:
                                pass
                            return
                        if self._wait_briefly(0.1):
                            return
                    self.db_lock.release()

                def progress_callback(msg):
                    """Progress callback az adatbázis mentéshez"""
                    try:
                        self.encoding_queue.put_nowait(("db_progress", msg))
                    except queue.Full:
                        pass

                self.save_state_to_db(progress_callback=progress_callback, settings_snapshot=_settings_snapshot)
            except Exception as e:
                try:
                    self.encoding_queue.put_nowait(("db_error", f"[ERROR] {t('db_error')}: {e}"))
                except queue.Full:
                    pass

        self._start_db_thread(save_db_async, name="SaveDBAsync")

        # MEGJEGYZÉS: encoding_worker, VMAF/SVT/NVENC workerek NEM indulnak itt!
        # A _queue_loading_thread() automatikusan indítja a workereket (_ensure_*_workers_running).
        # Az encoding_worker a check_encoding_queue() queue_loading_finished handler-ben indul.

    def _start_remaining_pending_files(self):
        """Load remaining pending files into queues after a manual re-encode.

        Triggers start_encoding() with a flag that preserves the already-queued
        manual task (skips clear_all_svt_tasks). If encoding is already fully
        running (e.g. Start button was pressed), this is a no-op.
        """
        # If queue loading is already running (Start was pressed), nothing to do
        if getattr(self, 'is_queue_loading', False):
            return
        # If graceful stop was requested, don't start new work
        _, _, graceful_stop = self.get_encoding_state()
        if graceful_stop or STOP_EVENT.is_set():
            return

        self._skip_queue_clear_on_start = True
        self._manual_start_active = True
        self.start_encoding()

    def _queue_loading_thread(self, pre_read_items, initial_min_vmaf, vmaf_step,
                              max_encoded, resize_enabled, resize_height,
                              nvenc_enabled, audio_compression_enabled,
                              audio_compression_method,
                              vdub_validation_disabled=False,
                              configured_nvenc_workers=None,
                              configured_svt_workers=None):
        """Queue feltöltés háttérszálban - megszakítható, tkinter-mentes.

        KRITIKUS: Ez a metódus NEM férhet hozzá tkinter widgetekhez (self.tree, self.root, stb.)!
        Minden tree adat pre-read-ként érkezik a pre_read_items listában.
        GUI frissítés kizárólag self.encoding_queue-n keresztül történik.
        """
        svt_queued = 0
        nvenc_queued = 0
        vmaf_queued = 0
        skipped_non_finished = 0
        total = len(pre_read_items)
        last_progress_time = time.time()

        try:
            for idx, item_data in enumerate(pre_read_items, 1):
                # Megszakíthatóság ellenőrzése (minden iterációban)
                if self.queue_loading_stop_requested or STOP_EVENT.is_set():
                    self.encoding_queue.put(("queue_loading_stopped", svt_queued, nvenc_queued, vmaf_queued))
                    return

                item_id = item_data['item_id']
                video_path = item_data['video_path']
                current_values = item_data['values']
                current_tags = item_data['tags']

                # Státusz kód meghatározás
                current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                status_code = normalize_status_to_code(current_status)

                # Robust fallback: normalize from cached status_code/tag
                if not status_code:
                    try:
                        status_code = self.get_tree_item_meta(item_id, 'status_code')
                    except Exception:
                        status_code = None

                # Resume interrupted/in-progress states as queue states
                if status_code in ('svt_encoding', 'svt_validation', 'svt_crf_search'):
                    status_code = 'svt_queue'
                elif status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search'):
                    status_code = 'nvenc_queue'
                elif status_code == 'encoding':
                    status_upper = str(current_status).upper()
                    tags_set = set(current_tags)
                    if 'SVT' in status_upper or 'encoding_svt' in tags_set:
                        status_code = 'svt_queue'
                    elif 'NVENC' in status_upper or 'encoding_nvenc' in tags_set:
                        status_code = 'nvenc_queue'
                    else:
                        status_code = 'nvenc_queue' if nvenc_enabled else 'svt_queue'
                elif status_code == 'pending':
                    status_upper = str(current_status).upper()
                    tags_set = set(current_tags)
                    if 'SVT' in status_upper or 'encoding_svt' in tags_set:
                        status_code = 'svt_queue'
                    elif 'NVENC' in status_upper or 'encoding_nvenc' in tags_set:
                        status_code = 'nvenc_queue'
                    else:
                        status_code = 'nvenc_queue' if nvenc_enabled else 'svt_queue'

                # NVENC queue
                if status_code == 'nvenc_queue':
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                    valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                    subtitle_files = valid_subtitles
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

                    # Graceful stop ellenőrzése
                    _, _, graceful_stop = self.get_encoding_state()
                    if graceful_stop:
                        continue

                    # Manual paraméter kinyerés a státusz szövegből
                    row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                    manual_cq_value = row_meta.get('manual_cq_value')
                    manual_quality_check = row_meta.get('manual_quality_check')
                    manual_cq_range = row_meta.get('manual_cq_range')
                    skip_crf_search = False

                    if manual_cq_value is not None:
                        skip_crf_search = True
                    if manual_cq_value is None and " (M " in current_status:
                        try:
                            manual_part = current_status.split(" (M ")[1].rstrip(")")
                            if "CQ:" in manual_part:
                                cq_str = manual_part.split("CQ:")[1].split()[0]
                                manual_cq_value = int(cq_str)
                                manual_cq_range = 'single'
                                skip_crf_search = True
                            parts = manual_part.split()
                            if len(parts) >= 2 and not manual_quality_check:
                                quality_str = parts[-1].lower()
                                if quality_str in ('vmaf', 'psnr', 'both'):
                                    manual_quality_check = quality_str
                        except (IndexError, ValueError, AttributeError):
                            pass

                    success = self.add_to_nvenc_queue(
                        video_path=video_path,
                        item_id=item_id,
                        is_manual=(manual_cq_value is not None),
                        pre_cached_values=current_values,
                        pre_cached_tags=current_tags,
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
                        target_cq=manual_cq_value,
                        skip_crf_search=skip_crf_search,
                        manual_cq_range=manual_cq_range if manual_cq_value is not None else None,
                        manual_cq_value=manual_cq_value,
                        manual_quality_check=manual_quality_check,
                        vdub_validation_disabled=vdub_validation_disabled,
                        reason='start_encoding'
                    )

                    if success:
                        nvenc_queued += 1
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        updated_status = self._build_manual_queue_status_text(t('status_nvenc_queue'), manual_cq_value, manual_quality_check)
                        self.encoding_queue.put(("update", item_id, updated_status, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # Worker auto-start (pre-cached worker count to avoid tkinter from bg thread)
                        self._ensure_nvenc_workers_running(configured_workers=configured_nvenc_workers)

                # SVT-AV1 queue
                elif status_code == 'svt_queue':
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                    valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                    subtitle_files = valid_subtitles
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

                    # Graceful stop ellenőrzése
                    _, _, graceful_stop = self.get_encoding_state()
                    if graceful_stop:
                        continue

                    # Manual paraméter kinyerés a státusz szövegből
                    row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                    manual_cq_value = row_meta.get('manual_cq_value')
                    manual_quality_check = row_meta.get('manual_quality_check')
                    manual_cq_range = row_meta.get('manual_cq_range')
                    skip_crf_search = False

                    if manual_cq_value is not None:
                        skip_crf_search = True
                    if manual_cq_value is None and " (M " in current_status:
                        try:
                            manual_part = current_status.split(" (M ")[1].rstrip(")")
                            if "CQ:" in manual_part:
                                cq_str = manual_part.split("CQ:")[1].split()[0]
                                manual_cq_value = int(cq_str)
                                manual_cq_range = 'single'
                                skip_crf_search = True
                            parts = manual_part.split()
                            if len(parts) >= 2 and not manual_quality_check:
                                quality_str = parts[-1].lower()
                                if quality_str in ('vmaf', 'psnr', 'both'):
                                    manual_quality_check = quality_str
                        except (IndexError, ValueError, AttributeError):
                            pass

                    success = self.add_to_svt_queue(
                        video_path=video_path,
                        item_id=item_id,
                        task_type='encode',
                        is_manual=(manual_cq_value is not None),
                        pre_cached_values=current_values,
                        pre_cached_tags=current_tags,
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
                        target_cq=manual_cq_value,
                        skip_crf_search=skip_crf_search,
                        manual_cq_range=manual_cq_range if manual_cq_value is not None else None,
                        manual_cq_value=manual_cq_value,
                        manual_quality_check=manual_quality_check,
                        vdub_validation_disabled=vdub_validation_disabled,
                        reason='start_encoding'
                    )

                    if success:
                        svt_queued += 1
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        updated_status = self._build_manual_queue_status_text(t('status_svt_queue'), manual_cq_value, manual_quality_check)
                        self.encoding_queue.put(("update", item_id, updated_status, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # Worker auto-start (pre-cached worker count to avoid tkinter from bg thread)
                        self._ensure_svt_workers_running(configured_workers=configured_svt_workers)

                # VMAF / PSNR ellenőrzésre váró videók
                elif status_code in ('vmaf_waiting', 'psnr_waiting', 'vmaf_psnr_waiting'):
                    output_file = self.video_to_output.get(video_path)
                    if output_file and output_file.exists():
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        vmaf_task = {
                            'video_path': video_path,
                            'output_file': output_file,
                            'item_id': item_id,
                            'orig_size_str': orig_size_str,
                            'current_cq_str': current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                            'current_vmaf_str': current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                            'current_psnr_str': current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                            'current_new_size_str': current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                            'current_size_change': current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                            'current_completed_date': current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else "",
                            'current_status': current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else t('status_completed'),
                            'check_vmaf': status_code != 'psnr_waiting',
                            'check_psnr': status_code != 'vmaf_waiting'
                        }
                        VMAF_QUEUE.put(vmaf_task)
                        vmaf_queued += 1

                else:
                    # Item not matched by any queue branch — check if it's
                    # genuinely unfinished and should be queued as fallback.
                    finished_codes_check = {
                        'completed', 'completed_nvenc', 'completed_svt',
                        'completed_copy', 'completed_exists',
                        'failed', 'source_missing', 'file_missing', 'load_error',
                        'needs_check', 'needs_check_nvenc', 'needs_check_svt'
                    }
                    is_unfinished = (
                        status_code not in finished_codes_check
                        and 'completed' not in current_tags
                        and 'failed' not in current_tags
                        and 'needs_check' not in current_tags
                    )
                    if is_unfinished:
                        # FALLBACK: unrecognised but unfinished → queue it
                        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                        valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

                        _, _, graceful_stop_fb = self.get_encoding_state()
                        if graceful_stop_fb:
                            continue

                        # Decide target queue based on status text, tags, and NVENC setting
                        use_svt = (
                            not nvenc_enabled
                            or 'SVT' in str(current_status).upper()
                            or 'encoding_svt' in set(current_tags)
                        )

                        if use_svt:
                            success = self.add_to_svt_queue(
                                video_path=video_path, item_id=item_id,
                                task_type='encode', is_manual=False,
                                pre_cached_values=current_values, pre_cached_tags=current_tags,
                                output_file=output_file,
                                subtitle_files=valid_subtitles, invalid_subtitles=invalid_subtitles,
                                orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                                max_encoded=max_encoded,
                                resize_enabled=resize_enabled, resize_height=resize_height,
                                audio_compression_enabled=audio_compression_enabled,
                                audio_compression_method=audio_compression_method,
                                vdub_validation_disabled=vdub_validation_disabled,
                                reason='fallback_unrecognised'
                            )
                            if success:
                                svt_queued += 1
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                self._ensure_svt_workers_running(configured_workers=configured_svt_workers)
                        else:
                            success = self.add_to_nvenc_queue(
                                video_path=video_path, item_id=item_id, is_manual=False,
                                pre_cached_values=current_values, pre_cached_tags=current_tags,
                                output_file=output_file,
                                subtitle_files=valid_subtitles, invalid_subtitles=invalid_subtitles,
                                orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                                max_encoded=max_encoded,
                                resize_enabled=resize_enabled, resize_height=resize_height,
                                audio_compression_enabled=audio_compression_enabled,
                                audio_compression_method=audio_compression_method,
                                vdub_validation_disabled=vdub_validation_disabled,
                                reason='fallback_unrecognised'
                            )
                            if success:
                                nvenc_queued += 1
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                self._ensure_nvenc_workers_running(configured_workers=configured_nvenc_workers)

                        skipped_non_finished += 1
                        if LOG_WRITER and skipped_non_finished <= 10:
                            try:
                                LOG_WRITER.write(
                                    f"  [DIAG] Queue loading fallback: status_code={status_code!r}, "
                                    f"status_text={current_status!r}, tags={current_tags}, "
                                    f"target={'SVT' if use_svt else 'NVENC'}, path={Path(video_path).name}\n"
                                )
                                LOG_WRITER.flush()
                            except Exception:
                                pass

                # GUI progress frissítés (időalapú, thread-safe)
                now = time.time()
                if idx % 50 == 0 or (now - last_progress_time) > 0.2:
                    self.encoding_queue.put(("queue_progress", idx, total))
                    last_progress_time = now

            if LOG_WRITER and skipped_non_finished > 0:
                try:
                    LOG_WRITER.write(
                        f"  [WARN] Queue loading: {skipped_non_finished} non-finished items skipped "
                        f"(SVT={svt_queued}, NVENC={nvenc_queued}, VMAF={vmaf_queued})\n"
                    )
                    LOG_WRITER.flush()
                except Exception:
                    pass

            # Queue feltöltés kész
            self.encoding_queue.put(("queue_loading_finished", svt_queued, nvenc_queued, vmaf_queued))

        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"Queue loading thread error: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            # Hiba esetén is jelezzük a befejezést
            self.encoding_queue.put(("queue_loading_finished", svt_queued, nvenc_queued, vmaf_queued))

    def stop_queue_loading_or_encoding(self):
        """Intelligens leállítás: queue loading VAGY encoding fázisban."""
        if self.is_queue_loading:
            # Queue feltöltés leállítása (a háttérszál ellenőrzi ezt a flag-et)
            self.queue_loading_stop_requested = True
            self.status_label.config(text="Queue feltöltés leállítása...")
            if LOG_WRITER:
                try:
                    LOG_WRITER.write("Queue loading stop requested\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
        else:
            # Normál encoding leállítás
            self.stop_encoding_graceful()

    def stop_encoding_immediate(self):
        """Stop encoding immediately (public API with confirmation dialog).

        Terminates all worker threads and subprocesses immediately.
        Shows confirmation dialog before proceeding.
        """
        self._stop_encoding_immediate_internal(skip_confirm=False)

    def _stop_encoding_immediate_internal(self, skip_confirm=False):
        """Stop encoding immediately (internal implementation).

        Args:
            skip_confirm: If True, skip the confirmation dialog. Used by
                graceful stop timeout escalation to avoid blocking the GUI.
        """
        # J3: Dupla immediate stop védelem - ha már fut egy stop szál, ne indítsunk újat
        if getattr(self, '_stop_thread_running', False):
            return

        # VMAF számítás ellenőrzése
        vmaf_running = False
        if hasattr(self, 'vmaf_worker_threads'):
            vmaf_running = any(t.is_alive() for t in self.vmaf_worker_threads)
        elif hasattr(self, 'vmaf_thread'):
             vmaf_running = self.vmaf_thread.is_alive()

        has_vmaf_work = not VMAF_QUEUE.empty() or vmaf_running

        # 1. lépés: Állapot ellenőrzés lock alatt
        with self.encoding_state_lock:
            if not self.is_encoding and not has_vmaf_work and not self.is_queue_loading:
                return

        # 2. lépés: Megerősítés lock NÉLKÜL (nem blokkol worker szálakat)
        if not skip_confirm:
            from tkinter import messagebox
            confirm = messagebox.askyesno(
                title=t('confirm_immediate_stop_title'),
                message=t('confirm_immediate_stop_message')
            )
            if not confirm:
                return

        # Graceful stop timeout lemondása (ha volt aktív)
        self._cancel_graceful_stop_timeout()

        # J3: Stop szál védelem flag beállítása
        self._stop_thread_running = True

        # 3. lépés: Queue loading leállítása (ha fut)
        if self.is_queue_loading:
            self.queue_loading_stop_requested = True
            self.is_queue_loading = False

        # 4. lépés: Állapot módosítás lock alatt (közvetlenül, nem set_encoding_state-tel)
        with self.encoding_state_lock:
            # Újraellenőrzés: közben megváltozhatott az állapot
            if not self.is_encoding and not has_vmaf_work:
                # Ha csak queue loading futott (és már leállítottuk), engedjük tovább
                pass
            else:
                self.is_encoding = False
            self.encoding_worker_running = False
            self.graceful_stop_requested = False

        # Thread-safe STOP_EVENT set
        STOP_EVENT.set()
        self.status_label.config(text=t('status_immediate_stopping'))
        # Start gomb visszaállítása "Start"-ra
        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
        self.update_start_button_state()
        self.immediate_stop_button.config(state=tk.DISABLED)
        # Videók betöltése gomb aktívvá tétele
        self.load_videos_btn.config(state=tk.NORMAL)
        
        # Adatbázis mentés leállítás után
        # Ezt majd a worker thread végzi el

        # Gyűjtsük össze azokat a videókat, amelyeket vissza kell állítani (a fő szálon, mert tree-t olvasunk)
        videos_data_to_reset = []
        active_video_dirs = set()
        
        try:
            for video_path, item_id in list(self.video_items.items()):
                try:
                    current_values = self.tree.item(item_id, 'values')
                    current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    current_tags = self.tree.item(item_id, 'tags')
                    status_code = normalize_status_to_code(current_status)
                except (tk.TclError, KeyError):
                    continue

                # Kész vagy ellenőrizendő állapotot nem bolygatunk
                if status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists', 'needs_check', 'needs_check_nvenc', 'needs_check_svt'):
                    continue

                if status_code in ('audio_edit_queue', 'audio_editing'):
                    # Az audio edit visszaállítást azonnal meg tudjuk tenni (csak memória művelet)
                    original_info = self.audio_edit_task_info.get(item_id)
                    self._restore_audio_task_state(item_id, original_info)
                    self.audio_edit_task_info.pop(item_id, None)
                    continue

                # Ha encoding állapotban van, gyűjtsük az adatokat a későbbi visszaállításhoz
                if ("encoding" in current_tags or "encoding_nvenc" in current_tags or "encoding_svt" in current_tags or 
                    status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search', 
                                   'svt_encoding', 'svt_validation', 'svt_crf_search', 
                                   'nvenc_queue', 'svt_queue', 'denoising')):
                    
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    
                    # Eldöntjük, hogy melyik queue-ba kell visszaállítani
                    is_svt = (status_code in ('svt_encoding', 'svt_validation', 'svt_crf_search', 'svt_queue') or 
                             'encoding_svt' in current_tags)
                    target_queue = 'svt' if is_svt else 'nvenc'
                    
                    row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                    manual_suffix = ""
                    manual_cq_value = row_meta.get('manual_cq_value')
                    manual_quality_check = row_meta.get('manual_quality_check')
                    if manual_cq_value is not None:
                        base_status = t('status_svt_queue') if target_queue == 'svt' else t('status_nvenc_queue')
                        rebuilt_status = self._build_manual_queue_status_text(base_status, manual_cq_value, manual_quality_check)
                        if rebuilt_status.startswith(base_status):
                            manual_suffix = rebuilt_status[len(base_status):]
                    elif " (M " in current_status:
                        try:
                            manual_suffix = " (M " + current_status.split(" (M ")[1]
                            if not manual_suffix.endswith(")"):
                                manual_suffix += ")"
                        except (IndexError, AttributeError):
                            pass
                    
                    videos_data_to_reset.append({
                        'item_id': item_id,
                        'video_path': video_path,
                        'orig_size_str': orig_size_str,
                        'completed_date': completed_date,
                        'target_queue': target_queue,
                        'manual_suffix': manual_suffix  # Store manual parameters
                    })
                    
                    # Aktív videók mappáinak gyűjtése (cleanup-hoz)
                    if status_code not in ('nvenc_queue', 'svt_queue'): # Csak ha már futott
                        active_video_dirs.add(video_path.parent)

        except Exception as e:
            print(f"Error collecting videos to stop: {e}")
        
        # Audio queue törlése, ha nincs futó worker
        if not (self.audio_edit_thread and self.audio_edit_thread.is_alive()):
            self._reset_audio_tasks_pending()
            self.audio_edit_thread = None
        self.audio_edit_only_mode = False
    
        # Processek lekérése thread-safe módon (globális listából)
        # Gyors, nem blokkol
        with ACTIVE_PROCESSES_LOCK:
            processes_to_stop = list(ACTIVE_PROCESSES)
        
        # Háttérszál a leállítási műveletekhez (fagyás elkerülése végett)
        def stop_worker_thread():
            # 1. Processek leállítása (ez a legfontosabb)
            self._terminate_processes_safe(processes_to_stop)
            
            # 2. Félkész output fájlok törlése
            if not DEBUG_MODE and videos_data_to_reset:
                for video_data in videos_data_to_reset:
                    video_path = video_data['video_path']
                    # Csak akkor törlünk, ha nem queue státuszban volt (tehát már létrejöhetett fájl)
                    # De a videos_data_to_reset minden queue/encoding elemet tartalmaz.
                    # Biztonságosabb, ha mindegyikre megpróbáljuk a törlést, ha van output fájl
                    output_file = self.video_to_output.get(video_path)
                    if output_file and output_file.exists():
                        # CRITICAL FIX #3: Retry mechanism for file deletion
                        max_retries = 3
                        deleted = False
                        for retry_idx in range(max_retries):
                            try:
                                output_file.unlink()
                                deleted = True
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: leállítás, félkész output törlése | fájl: {output_file.name}\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                                break
                            except (OSError, PermissionError) as e:
                                if retry_idx < max_retries - 1:
                                    self._wait_briefly(0.2)
                                else:
                                    # Log final failure
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[WARN] Failed to delete {output_file} after {max_retries} retries: {e}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                            except FileNotFoundError:
                                # File already deleted (race condition with another thread)
                                deleted = True
                                break

            # 3. LIST-BASED QUEUE: Clear all pending tasks from queues
            # This ensures no tasks will be picked up by workers after stop
            if hasattr(self, 'clear_all_svt_tasks') and hasattr(self, 'clear_all_nvenc_tasks'):
                self.clear_all_svt_tasks()
                self.clear_all_nvenc_tasks()
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write("[OK] Cleared all pending tasks from SVT and NVENC queues\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            # 4. .ab-av1-* mappák törlése
            if active_video_dirs:
                for video_dir in active_video_dirs:
                    try:
                        cleanup_ab_av1_temp_dirs(video_dir)
                    except Exception:
                        pass

            # 5. Státuszok visszaállítása a UI-n (queue-n keresztül, chunk-olva a reszponzivitásért)
            chunk_size = 50
            for i in range(0, len(videos_data_to_reset), chunk_size):
                chunk = videos_data_to_reset[i:i+chunk_size]
                for data in chunk:
                    # Base status text
                    status_text = t('status_svt_queue') if data['target_queue'] == 'svt' else t('status_nvenc_queue')
                    # CRITICAL FIX: Append manual suffix if present
                    if data.get('manual_suffix'):
                        status_text += data['manual_suffix']
                    
                    self.encoding_queue.put(("update", data['item_id'], status_text, "-", "-", "-", "-", data['orig_size_str'], "-", "-", data['completed_date']))
                    self.encoding_queue.put(("tag", data['item_id'], "pending"))
                
                # Kis szünet a chunkok között, hogy a UI processzálhassa az üzeneteket
                # Reduced from 500ms to 100ms for faster exit
                time.sleep(0.1)  # 100ms várakozás (still enough for GUI processing)
            
            # 6. Adatbázis mentés (mindig mentsünk, ha vannak videók, hogy ne felejtse el az állapotot)
            if self.video_items:
                # THREAD-SAFETY FIX: Pre-cache settings on GUI thread before DB thread
                _reset_settings_snapshot = self._build_settings_snapshot()
                def save_db_task():
                    try:
                        self.save_state_to_db(settings_snapshot=_reset_settings_snapshot)
                    except Exception as e:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[ERROR] [stop_worker_thread] Final Save DB Error: {e}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                self._start_db_thread(save_db_task, name="SaveDBVideosReset")

            # 7. UI visszaállítása (fő szálon) - CSAK ha az ablak még él
            # CRITICAL DEADLOCK FIX: root.after() must not be called if app is closing!
            # After root.destroy(), root.after() callbacks will NEVER execute,
            # causing this non-daemon thread to wait forever.
            from .gui_shared import is_app_closing
            try:
                if not is_app_closing() and self.root.winfo_exists():
                    self.root.after(0, self._on_immediate_stop_completed)
                else:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write("[DEBUG] stop_worker_thread: app closing or root destroyed, skipping UI callback\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            except tk.TclError:
                # root already destroyed
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write("[DEBUG] stop_worker_thread: TclError - root destroyed, skipping UI callback\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            # J7: Memória takarítás - feldolgozási halmazok és stop events ürítése
            # Ez biztosítja, hogy árva bejegyzések nem akadályozzák az újraindítást
            try:
                if hasattr(self, 'svt_processing_videos'):
                    self.svt_processing_videos.clear()
                if hasattr(self, 'nvenc_processing_videos'):
                    self.nvenc_processing_videos.clear()
                if hasattr(self, 'vmaf_processing_videos'):
                    with self.nvenc_selection_lock:
                        self.vmaf_processing_videos.clear()
                if hasattr(self, 'video_stop_events'):
                    with self.video_stop_events_lock:
                        self.video_stop_events.clear()
            except Exception:
                pass

            # J8: VMAF_QUEUE ürítése és státusz visszaállítása
            try:
                while not VMAF_QUEUE.empty():
                    try:
                        VMAF_QUEUE.get_nowait()
                    except queue.Empty:
                        break
            except Exception:
                pass

            # J3: Stop szál védelem flag törlése
            self._stop_thread_running = False

        # Thread indítása - nem-daemon, hogy biztosan befejeződjön a mentés akkor is, ha zárják az ablakot
        from .gui_shared import is_app_closing
        stop_thread = threading.Thread(target=stop_worker_thread, name="ImmediateStopWorker")
        stop_thread.daemon = is_app_closing()
        stop_thread.start()
        self.last_stop_thread = stop_thread

    def _ensure_svt_workers_running(self, configured_workers=None):
        """Ensure configured number of SVT workers are running.

        Called when tasks are added to ensure workers pick them up.
        Workers auto-stop when no tasks remain.

        Args:
            configured_workers: Pre-cached worker count (pass when calling from
                non-GUI threads to avoid tkinter .get() cross-thread deadlock).
        """
        with self.task_list_lock:
            if not hasattr(self, 'svt_worker_threads'):
                self.svt_worker_threads = []

            # Clean up dead threads
            self.svt_worker_threads = [t for t in self.svt_worker_threads if t.is_alive()]

            if configured_workers is None:
                configured_workers = self.get_configured_svt_workers()
            active_workers = len(self.svt_worker_threads)
            pending_tasks = len(getattr(self, 'pending_svt_tasks', []))

            # Only start workers if we have pending tasks and not enough active workers
            if pending_tasks > 0 and active_workers < configured_workers:
                workers_to_start = min(configured_workers - active_workers, pending_tasks)
                # BUG FIX: Find actually used indices to avoid duplicates.
                # Previously start_idx=active_workers caused collisions when a
                # lower-indexed worker exited while a higher-indexed one was alive.
                used_indices = {getattr(t, 'worker_index', None) for t in self.svt_worker_threads}
                started = 0
                for candidate_idx in range(configured_workers):
                    if started >= workers_to_start:
                        break
                    if candidate_idx not in used_indices:
                        thread = threading.Thread(target=self.svt_worker, args=(candidate_idx,), daemon=True)
                        thread.worker_index = candidate_idx
                        thread.start()
                        self.svt_worker_threads.append(thread)
                        started += 1

                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[OK] SVT worker #{candidate_idx + 1} started (auto-restart)\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass

    def _ensure_nvenc_workers_running(self, configured_workers=None):
        """Ensure configured number of NVENC workers are running.

        Called when tasks are added to ensure workers pick them up.
        Workers auto-stop when no tasks remain.

        Args:
            configured_workers: Pre-cached worker count (pass when calling from
                non-GUI threads to avoid tkinter .get() cross-thread deadlock).
        """
        with self.task_list_lock:
            if not hasattr(self, 'nvenc_worker_threads'):
                self.nvenc_worker_threads = []

            # Clean up dead threads
            self.nvenc_worker_threads = [t for t in self.nvenc_worker_threads if t.is_alive()]

            if configured_workers is None:
                configured_workers = self.get_configured_nvenc_workers()
            active_workers = len(self.nvenc_worker_threads)
            pending_tasks = len(getattr(self, 'pending_nvenc_tasks', []))

            # Only start workers if we have pending tasks and not enough active workers
            if pending_tasks > 0 and active_workers < configured_workers:
                workers_to_start = min(configured_workers - active_workers, pending_tasks)
                # BUG FIX: Find actually used indices to avoid duplicates.
                used_indices = {getattr(t, 'worker_index', None) for t in self.nvenc_worker_threads}
                started = 0
                for candidate_idx in range(configured_workers):
                    if started >= workers_to_start:
                        break
                    if candidate_idx not in used_indices:
                        thread = threading.Thread(target=self.nvenc_worker, args=(candidate_idx,), daemon=True)
                        thread.worker_index = candidate_idx
                        thread.start()
                        self.nvenc_worker_threads.append(thread)
                        started += 1

                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[OK] NVENC worker #{candidate_idx + 1} started (auto-restart)\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass

    @property
    def is_encoding_active(self):
        """
        Check if any encoding is currently in progress.

        Returns True if:
        - Any SVT worker thread is alive
        - OR any NVENC worker thread is alive
        - OR VMAF thread is alive
        - OR there are pending tasks in any queue

        Returns False only when all workers have stopped AND no tasks are pending.
        """
        with self.task_list_lock:
            # Check for pending tasks
            has_pending_svt = len(getattr(self, 'pending_svt_tasks', [])) > 0
            has_pending_nvenc = len(getattr(self, 'pending_nvenc_tasks', [])) > 0

            # Check for active workers
            svt_workers_alive = any(t.is_alive() for t in getattr(self, 'svt_worker_threads', []))
            nvenc_workers_alive = any(t.is_alive() for t in getattr(self, 'nvenc_worker_threads', []))
            vmaf_worker_alive = hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive()

            # Check VMAF queue
            from .core_workers_and_flows import VMAF_QUEUE
            has_vmaf_work = not VMAF_QUEUE.empty()

            # Encoding is active if ANY of these conditions is true
            return (has_pending_svt or has_pending_nvenc or
                    svt_workers_alive or nvenc_workers_alive or
                    vmaf_worker_alive or has_vmaf_work)

    def _terminate_processes_safe(self, processes):
        """Segédfüggvény processzek leállítására (háttérszálban fut).
        
        Psutil-t használ a rekurzív gyermekfolyamatok (grandchildren) leállítására.
        """
        if not processes:
            return

        # Próbáljuk psutil-lal (megbízhatóbb a child-of-child folyamatokhoz)
        try:
            import psutil
            has_psutil = True
        except ImportError:
            has_psutil = False

        if has_psutil:
            # Psutil-os megoldás: rekurzívan leállítja az összes leszármazottat
            all_pids_to_kill = set()
            
            for process in processes:
                if process and process.poll() is None:
                    try:
                        parent = psutil.Process(process.pid)
                        # Rekurzívan megkeressük az ÖSSZES leszármazott folyamatot
                        children = parent.children(recursive=True)
                        for child in children:
                            all_pids_to_kill.add(child.pid)
                        all_pids_to_kill.add(process.pid)
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        # Ha a folyamat már nem létezik, csak a pid-et adjuk hozzá
                        all_pids_to_kill.add(process.pid)
                    except Exception:
                        all_pids_to_kill.add(process.pid)
            
            # Először SIGTERM (gyerekek először, szülő utoljára = reversed order)
            for pid in all_pids_to_kill:
                try:
                    p = psutil.Process(pid)
                    p.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
                except Exception:
                    pass
            
            # Rövid várakozás
            time.sleep(1)
            
            # SIGKILL a még futóknak
            for pid in all_pids_to_kill:
                try:
                    p = psutil.Process(pid)
                    if p.is_running():
                        p.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
                except Exception:
                    pass
            
            # Extra: taskkill /T /F a biztonság kedvéért (ha psutil-lal nem sikerült)
            for process in processes:
                if process and process.poll() is None:
                    try:
                        process.wait(timeout=0.2)
                    except subprocess.TimeoutExpired:
                        pass
                    except Exception:
                        pass
            for process in processes:
                if process and process.poll() is None:
                    try:
                        if platform.system() == 'Windows':
                            subprocess.run(['taskkill', '/T', '/F', '/PID', str(process.pid)],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=600)
                    except Exception:
                        pass
        else:
            # Fallback: taskkill (ha psutil nincs telepítve)
            # 1. lépés: Terminate
            for process in processes:
                if process and process.poll() is None:
                    try:
                        process.terminate()
                    except Exception:
                        pass
            
            time.sleep(3) # Várakozás
            
            # 2. lépés: Ellenőrzés és Kill
            still_running = [p for p in processes if p and p.poll() is None]
            if still_running:
                for process in still_running:
                    try:
                        pid = process.pid
                        if platform.system() == 'Windows':
                            subprocess.run(['taskkill', '/T', '/F', '/PID', str(pid)],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=600)
                        else:
                            process.kill()
                    except Exception:
                        try:
                            process.kill()
                        except Exception:
                            pass
        
        # Lista takarítása
        with ACTIVE_PROCESSES_LOCK:
            for proc in processes:
                if proc in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(proc)

    def _on_immediate_stop_completed(self):
        """Azonnali leállítás befejezésekor hívandó (UI szálon)."""
        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
        self.immediate_stop_button.config(state=tk.DISABLED)
        self.load_videos_btn.config(state=tk.NORMAL)
        self.status_label.config(text=t('status_stopped'))
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        self.set_encoding_state(graceful_stop_requested=False)

        # Notification megjelenítése
        self.show_db_notification()


    def stop_encoding_graceful(self):
        """Stop encoding gracefully.
        
        Signals workers to stop after finishing their current task.
        Includes a 60-second timeout that auto-escalates to immediate stop
        if workers don't drain in time (e.g., stuck FFmpeg process).
        """
        # VMAF számítás ellenőrzése
        has_vmaf_work = not VMAF_QUEUE.empty() or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive()) or (hasattr(self, 'vmaf_worker_threads') and any(t.is_alive() for t in self.vmaf_worker_threads))

        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        is_enc, _, graceful_stop = self.get_encoding_state()
        if not is_enc and not has_vmaf_work:
            return
        if graceful_stop:
            return
    
        # CRITICAL FIX #1: Thread-safe graceful_stop set
        with self.encoding_state_lock:
            self.graceful_stop_requested = True
        self.status_label.config(text=t('status_graceful_stopping'))
        # Start gomb (ami most "Leállítás" gombként működik) letiltva, mert már leállítás folyamatban
        # Az update_start_button_state automatikusan kezeli ezt a graceful_stop_requested flag alapján
        self.update_start_button_state()
        # Azonnali leállítás aktív marad
        self.immediate_stop_button.config(state=tk.NORMAL)

        # TIMEOUT ESCALATION: 60 másodperc után automatikus eszkaláció immediate stop-ra.
        # Ez megakadályozza, hogy a rendszer végtelenig várjon egy elakadt FFmpeg folyamatra.
        self._cancel_graceful_stop_timeout()
        self._graceful_stop_timeout_id = self.root.after(
            60000,
            self._escalate_grace_to_immediate
        )

    def _cancel_graceful_stop_timeout(self):
        """Meglévő graceful stop timeout timer lemondása."""
        timeout_id = getattr(self, '_graceful_stop_timeout_id', None)
        if timeout_id is not None:
            try:
                self.root.after_cancel(timeout_id)
            except (ValueError, tk.TclError):
                pass
            self._graceful_stop_timeout_id = None

    def _escalate_grace_to_immediate(self):
        """Graceful stop timeout eszkaláció: automatikus immediate stop 60s után.
        
        Ez a metódus a GUI szálon fut (root.after callback), tehát biztonságosan
        hívhat tkinter metódusokat. A felhasználó nem kap megerősítő dialogot,
        mert a graceful stop már egy tudatos döntés volt.
        
        FONTOS: Ha bármilyen aktív folyamat van (kódolás, VMAF/PSNR, worker
        szálak, vagy függőben lévő feladatok), a timeout automatikusan
        meghosszabbodik 60 másodperccel ahelyett, hogy azonnali leállításra
        eszkalálna. Ez biztosítja, hogy:
        - Egy 95%-ban kész kódolás befejeződjön
        - A kódolás utáni VMAF/PSNR ellenőrzés is végigmenjen
        - A manuális CQ + utó-VMAF is teljesen befejeződjön
        Csak akkor eszkalál azonnalira, ha valóban NINCS aktív folyamat
        (elakadt/ragadt állapot).
        """
        self._graceful_stop_timeout_id = None
        _, _, graceful = self.get_encoding_state()
        if not graceful:
            return

        should_extend = False

        # 1. Aktív kódolási folyamatok (NVENC/SVT processing set)
        try:
            with self.nvenc_selection_lock:
                has_nvenc_active = len(self.nvenc_processing_videos) > 0
                has_svt_active = len(self.svt_processing_videos) > 0
                has_vmaf_processing = len(getattr(self, 'vmaf_processing_videos', set())) > 0
            if has_nvenc_active or has_svt_active or has_vmaf_processing:
                should_extend = True
        except (AttributeError, RuntimeError):
            pass

        # 2. VMAF queue + VMAF worker szálak
        if not should_extend:
            try:
                has_vmaf_active = not VMAF_QUEUE.empty() or (
                    hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive()
                ) or (
                    hasattr(self, 'vmaf_worker_threads') and any(t.is_alive() for t in self.vmaf_worker_threads)
                )
                if has_vmaf_active:
                    should_extend = True
            except (AttributeError, RuntimeError):
                pass

        # 3. SVT worker szálak élnek-e (kódolás vagy VMAF taskot végezhetnek)
        if not should_extend:
            try:
                has_live_svt = (
                    hasattr(self, 'svt_worker_threads') and
                    any(t.is_alive() for t in self.svt_worker_threads)
                )
                if has_live_svt:
                    should_extend = True
            except (AttributeError, RuntimeError):
                pass

        # 4. NVENC worker szálak élnek-e
        if not should_extend:
            try:
                has_live_nvenc = (
                    hasattr(self, 'nvenc_worker_threads') and
                    any(t.is_alive() for t in self.nvenc_worker_threads)
                )
                if has_live_nvenc:
                    should_extend = True
            except (AttributeError, RuntimeError):
                pass

        # 5. Függőben lévő SVT VMAF/PSNR feladatok a listában
        if not should_extend:
            try:
                with self.task_list_lock:
                    has_pending_vmaf = any(
                        t.get('type') == 'vmaf'
                        for t in getattr(self, 'pending_svt_tasks', [])
                    )
                    has_pending_encode = len(getattr(self, 'pending_svt_tasks', [])) > 0 or len(getattr(self, 'pending_nvenc_tasks', [])) > 0
                if has_pending_vmaf or has_pending_encode:
                    should_extend = True
            except (AttributeError, RuntimeError):
                pass

        # 6. Audio edit worker aktív-e
        if not should_extend:
            try:
                has_audio_edit = (
                    hasattr(self, 'audio_edit_thread') and
                    self.audio_edit_thread and
                    self.audio_edit_thread.is_alive()
                )
                if has_audio_edit:
                    should_extend = True
            except (AttributeError, RuntimeError):
                pass

        if should_extend:
            # Még van aktív folyamat - timeout meghosszabbítása
            if LOG_WRITER:
                try:
                    LOG_WRITER.write("[INFO] Graceful stop timeout: aktív folyamat észlelve, timeout meghosszabbítva +60s.\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            self._graceful_stop_timeout_id = self.root.after(
                60000,
                self._escalate_grace_to_immediate
            )
            return

        if LOG_WRITER:
            try:
                LOG_WRITER.write("[WARN] Graceful stop timeout: nincs aktív folyamat, auto-eszkaláció immediate stop-ra.\n")
                LOG_WRITER.flush()
            except Exception:
                pass

        self.status_label.config(text="Graceful stop timeout → azonnali leállítás...")

        # Direct immediate stop without confirmation dialog
        self._stop_encoding_immediate_internal(skip_confirm=True)

    def update_start_button_state(self):
        """Update the state of the Start button based on pending tasks.
        
        Enables the button if there are pending tasks and encoding is not running.
        Disables it otherwise.
        """
    
        if LOAD_DEBUG:
            import traceback
            caller = traceback.extract_stack()[-2].name if len(traceback.extract_stack()) > 1 else "unknown"
            load_debug_log(f"update_start_button_state hívva: caller={caller} | is_encoding={self.is_encoding} | is_loading={getattr(self, 'is_loading_videos', False)} | graceful_stop={getattr(self, 'graceful_stop_requested', False)}")
        
        # Ha graceful stop kérvényezve van, a gomb inaktív legyen, amíg a leállítás folyamatban van
        if getattr(self, 'graceful_stop_requested', False):
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.DISABLED)
            self.immediate_stop_button.config(state=tk.NORMAL)  # Azonnali leállítás továbbra is aktív
            return
        
        if self.is_encoding:
            # Folyamatban van valami – Start gomb "Leállítás"-ként aktív
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            return
        
        # VMAF munkálat ellenőrzése (kódolás befejeződött, de VMAF számítás folyik)
        has_vmaf_work = (not VMAF_QUEUE.empty()) or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive())
        if has_vmaf_work:
            # VMAF munkálat esetén a gomb "Leállítás"-ként aktív maradjon
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            return
        
        if getattr(self, 'is_loading_videos', False):
            # Betöltés közben ne lehessen indítani
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
            if hasattr(self, 'immediate_stop_button'):
                self.immediate_stop_button.config(state=tk.DISABLED)
            return
        has_tasks = self.has_pending_tasks()
        if has_tasks:
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: has_tasks=True, aktiváljuk a gombot")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.DISABLED)
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: gomb aktiválva (state={self.start_button.cget('state')})")
            return
    
        if self.video_items and not self.is_loading_videos:
            # Ha vannak betöltött videók, engedjük a Start gombot akkor is, ha nem találtunk pending státuszt
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: video_items={len(self.video_items)}, aktiváljuk a gombot")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
        else:
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: nincs video_items vagy loading, inaktív gomb")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
        self.immediate_stop_button.config(state=tk.DISABLED)

    def _reset_encoding_ui_if_idle(self, status_text=None):
        """Visszaállítja a vezérlőket, ha nincs aktív worker és nincsenek függő feladatok."""
        if (getattr(self, 'encoding_worker_running', False) or
                getattr(self, 'manual_nvenc_active', False) or
                getattr(self, 'vmaf_worker_active', False) or
                self.audio_edit_only_mode):
            return
        if not hasattr(self, 'tree'):
            return
        if self.has_pending_tasks():
            return
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        self.set_encoding_state(is_encoding=False)
        if hasattr(self, 'load_videos_btn'):
            self.load_videos_btn.config(state=tk.NORMAL)
        if hasattr(self, 'immediate_stop_button'):
            self.immediate_stop_button.config(state=tk.DISABLED)
        if hasattr(self, 'status_label'):
            self.status_label.config(text=status_text or t('status_ready'))
        self.update_start_button_state()

    def _refresh_encoding_worker_snapshot(self):
        """Capture worker runtime settings on the GUI thread."""
        self.current_min_vmaf = float(self.min_vmaf.get())
        self.current_vmaf_step = float(self.vmaf_step.get())
        self.current_max_encoded_percent = float(self.max_encoded_percent.get())
        self.current_resize_enabled = bool(self.resize_enabled.get())
        self.current_resize_height = self.resize_height.get()
        self.current_deband_enabled = bool(self.deband_enabled.get()) if hasattr(self, 'deband_enabled') else True
        self.current_force_8bit_denoised_master = bool(self.force_8bit_denoised_master.get()) if hasattr(self, 'force_8bit_denoised_master') else False
        self.current_audio_compression_enabled = bool(self.audio_compression_enabled.get())
        audio_method = self.audio_compression_method.get()
        if audio_method == t('audio_compression_fast'):
            audio_method = 'fast'
        elif audio_method == t('audio_compression_dialogue'):
            audio_method = 'dialogue'
        self.current_audio_compression_method = audio_method
        self.current_nvenc_enabled = bool(self.nvenc_enabled.get())
        self.current_auto_vmaf_psnr = bool(self.auto_vmaf_psnr.get())
        self.current_max_cq_limit = int(self.max_cq_limit.get())
        self.current_nvenc_worker_count = self.get_configured_nvenc_workers()
        self.current_svt_worker_count = self.get_configured_svt_workers()
        self.current_vdub_validation_disabled = bool(self.vdub_validation_disabled.get()) if hasattr(self, 'vdub_validation_disabled') else False

    def _requeue_stalled_pending_to_svt(self, initial_min_vmaf, vmaf_step, max_encoded,
                                         resize_enabled, resize_height,
                                         audio_compression_enabled, audio_compression_method):
        """Re-queue stalled pending tree items to the SVT queue.

        Called by encoding_worker when it detects a stuck state: pending items
        exist in the tree but the SVT queue is empty and no SVT workers are alive.
        This happens primarily when NVENC is disabled — find_next_waiting_video()
        always returns None, so the encoding_worker loop cannot produce SVT tasks
        by itself.

        Returns:
            int: Number of tasks successfully re-queued.
        """
        requeued = 0
        finished_codes = {
            'completed', 'completed_nvenc', 'completed_svt',
            'completed_copy', 'completed_exists',
            'failed', 'source_missing', 'file_missing', 'load_error',
            'needs_check', 'needs_check_nvenc', 'needs_check_svt'
        }

        with self.video_items_lock:
            video_items_snapshot = list(self.video_items.items())

        for video_path, item_id in video_items_snapshot:
            if STOP_EVENT.is_set():
                break
            _, _, graceful = self.get_encoding_state()
            if graceful:
                break
            try:
                # Skip if already queued or being processed
                with self.task_list_lock:
                    if video_path in self.svt_processing_videos:
                        continue
                    if any(t['video_path'] == video_path for t in self.pending_svt_tasks):
                        continue

                current_values, current_tags = self._get_tree_data_snapshot(item_id)
                current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                status_code = normalize_status_to_code(current_status)

                if status_code in finished_codes:
                    continue
                if 'completed' in current_tags or 'failed' in current_tags or 'needs_check' in current_tags:
                    continue

                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

                row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                manual_cq_value = row_meta.get('manual_cq_value')
                manual_quality_check = row_meta.get('manual_quality_check')
                manual_cq_range = row_meta.get('manual_cq_range')

                success = self.add_to_svt_queue(
                    video_path=video_path,
                    item_id=item_id,
                    task_type='encode',
                    is_manual=(manual_cq_value is not None),
                    pre_cached_values=current_values,
                    pre_cached_tags=current_tags,
                    output_file=output_file,
                    subtitle_files=valid_subtitles,
                    invalid_subtitles=invalid_subtitles,
                    orig_size_str=orig_size_str,
                    initial_min_vmaf=initial_min_vmaf,
                    vmaf_step=vmaf_step,
                    max_encoded=max_encoded,
                    resize_enabled=resize_enabled,
                    resize_height=resize_height,
                    audio_compression_enabled=audio_compression_enabled,
                    audio_compression_method=audio_compression_method,
                    target_cq=manual_cq_value,
                    skip_crf_search=(manual_cq_value is not None),
                    manual_cq_range=manual_cq_range if manual_cq_value is not None else None,
                    manual_cq_value=manual_cq_value,
                    manual_quality_check=manual_quality_check,
                    vdub_validation_disabled=getattr(self, 'current_vdub_validation_disabled', False),
                    reason='requeue_stalled'
                )

                if success:
                    requeued += 1
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    updated_status = self._build_manual_queue_status_text(t('status_svt_queue'), manual_cq_value, manual_quality_check)
                    self.encoding_queue.put(("update", item_id, updated_status, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "pending"))

            except (tk.TclError, KeyError, AttributeError, IndexError):
                continue

        return requeued

    def _count_tree_results_for_worker(self):
        """Summarize tree results without touching tkinter from the worker thread."""
        completed = 0
        failed = 0
        needs_check = 0

        with self.video_items_lock:
            video_items_snapshot = list(self.video_items.items())

        for _, item_id in video_items_snapshot:
            current_values, tags = self._get_tree_data_snapshot(item_id)
            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            if is_status_completed(status) or "completed" in tags:
                if is_status_needs_check(status) or "needs_check" in tags:
                    needs_check += 1
                else:
                    completed += 1
            elif "[ERROR]" in status or "failed" in tags or "Hiba" in status:
                failed += 1

        return completed, failed, needs_check

    def encoding_worker(self):
        debug_pause.gui_queue = self.encoding_queue
    
        # Nem-videó fájlok másolása már a start_encoding-ban megtörtént,
        # itt csak a kódolás worker logikája következik
    
        completed = 0
        failed = 0
        needs_check = 0
    
        initial_min_vmaf = self.current_min_vmaf
        vmaf_step = self.current_vmaf_step
        max_encoded_snapshot = self.current_max_encoded_percent
        resize_enabled = self.current_resize_enabled
        resize_height = self.current_resize_height
        audio_compression_enabled = self.current_audio_compression_enabled
        audio_compression_method = self.current_audio_compression_method

        stop_requested = False

        # Végigmegyünk a videókon, és mindig az első várakozót választjuk
        # Optimalizálás: késleltetjük a ciklust, hogy ne blokkolja a GUI-t
        processed_videos = 0
        worker_poll_interval = 0.1
        last_worker_scan = time.time()
        last_requeue_time = 0.0
        requeue_cooldown = 5.0  # Minimum seconds between stalled re-queue attempts
        
        # Ha csak queue-ból dolgozunk (nincs pending videó a tree-ben), akkor várunk, amíg a queue-k kiürülnek
        # és akkor küldünk "finished" üzenetet
        has_any_pending_in_tree = self.has_pending_tasks()
        
        while True:
            if stop_requested or STOP_EVENT.is_set():
                break
            if not self.is_encoding:
                stop_requested = True
                # Leállítás esetén minden várakozó videó státuszát visszaállítjuk
                # Optimalizálás: csak az első néhány videót állítjuk vissza egyszerre, hogy ne blokkolja a GUI-t
                videos_to_reset = list(self.video_files)
                for video_path in videos_to_reset:
                    if video_path in self.video_items:
                        item_id = self.video_items[video_path]
                        current_values, current_tags = self._get_tree_data_snapshot(item_id)
                        current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if not is_status_completed(current_status) and "completed" not in current_tags and not is_status_needs_check(current_status) and "needs_check" not in current_tags:
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            # Ha SVT queue-ban volt, akkor t('status_svt_queue'), egyébként "NVENC queue-ban vár..."
                            row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                            manual_cq_value = row_meta.get('manual_cq_value')
                            manual_quality_check = row_meta.get('manual_quality_check')
                            if "SVT-AV1" in current_status and "queue" in current_status.lower():
                                status_text = self._build_manual_queue_status_text(t('status_svt_queue'), manual_cq_value, manual_quality_check)
                            else:
                                status_text = self._build_manual_queue_status_text(t('status_nvenc_queue'), manual_cq_value, manual_quality_check)
                            self.encoding_queue.put(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                
                # JSON mentés a frissített állapottal
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                break
    
            # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            _, _, graceful_stop_loop = self.get_encoding_state()
            if graceful_stop_loop:
                # Csak a ténylegesen folyamatban lévő videókat várjuk meg (NVENC és SVT)
                # Nem a teljes queue-t és nem az összes élő threadet várjuk meg,
                # mert az elakadáshoz vezethet, ha a queue nem ürül ki.
                with self.nvenc_selection_lock:
                    has_active_processing = (len(self.nvenc_processing_videos) > 0 or
                                            len(self.svt_processing_videos) > 0 or
                                            len(getattr(self, 'vmaf_processing_videos', set())) > 0)
                
                if not has_active_processing:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write("[INFO] Graceful stop: Nincs több aktív kódolási folyamat, leállítás.\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    break
                
                # Még van folyamatban lévő kódolás, várunk
                self._wait_for_worker_poll(worker_poll_interval)
                continue
    
    
            # Megkeressük az első várakozó videót
            # Optimalizálás: rövid, stop-aware poll ciklust használunk, hogy
            # ne pörögjön a worker, de leállításra gyorsan reagáljon.
            current_time = time.time()
            wait_remaining = worker_poll_interval - (current_time - last_worker_scan)
            if wait_remaining > 0:
                self._wait_for_worker_poll(wait_remaining)
                continue
            last_worker_scan = current_time
            
            # Ellenőrizzük, hogy van-e szabad NVENC worker slot
            # Csak akkor keressük a következő videót, ha van szabad worker
            with self.task_list_lock:
                nvenc_threads_snapshot = list(getattr(self, 'nvenc_worker_threads', []))
            has_active_nvenc_workers = len(nvenc_threads_snapshot) > 0 and any(t.is_alive() for t in nvenc_threads_snapshot)
            nvenc_queue_size = NVENC_QUEUE.qsize()
            nvenc_worker_count = self.get_configured_nvenc_workers()
            
            # Csak akkor keressük a következő videót, ha van szabad worker slot
            # (queue méret < worker szám, vagy nincs aktív worker)
            if has_active_nvenc_workers and nvenc_queue_size >= nvenc_worker_count:
                # Nincs szabad worker slot, várunk
                self._wait_for_worker_poll(worker_poll_interval)
                continue
            
            video_path = self.find_next_waiting_video()
            if video_path is None:
                # Nincs több várakozó NVENC videó
                # Ellenőrizzük, hogy van-e még SVT queue-ban várakozó videó vagy aktív NVENC/SVT worker
                # LIST-BASED QUEUE: Check list-based queues
                with self.task_list_lock:
                    has_svt_queue_items = len(getattr(self, 'pending_svt_tasks', [])) > 0
                    has_nvenc_queue_items = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
                has_active_svt_worker = hasattr(self, 'svt_worker_threads') and any(t.is_alive() for t in self.svt_worker_threads) if hasattr(self, 'svt_worker_threads') else False
                has_vmaf_queue_items = not VMAF_QUEUE.empty()
                has_audio_edit_queue_items = not AUDIO_EDIT_QUEUE.empty()
                
                # Ellenőrizzük, hogy van-e valóban várakozó videó a tree-ben
                has_any_pending = self.has_pending_tasks()
                
                # Check active processing status
                with self.nvenc_selection_lock:
                    is_nvenc_processing = len(self.nvenc_processing_videos) > 0
                    is_svt_processing = len(self.svt_processing_videos) > 0
                
                # Ha nincs pending videó ÉS nincs aktív feldolgozás ÉS nincs queue-ban várakozó feladat, akkor minden kész
                if not has_any_pending and not has_svt_queue_items and not is_nvenc_processing and not is_svt_processing and not has_nvenc_queue_items and not has_vmaf_queue_items and not has_audio_edit_queue_items:
                    # Minden kész, várunk, amíg a queue-k kiürülnek
                    # LIST-BASED QUEUE: NVENC_QUEUE.join() still valid (NVENC uses old queue)
                    for _ in range(100):
                        if NVENC_QUEUE.unfinished_tasks == 0:
                            break
                        self._wait_for_worker_poll(0.1)
                    # SVT_QUEUE.join() removed - using list-based queue now
                    # Számoljuk meg a befejezett videókat a tree-ből
                    completed, failed, needs_check = self._count_tree_results_for_worker()
                    # Encoding worker befejeződött - flag frissítése
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(encoding_worker_running=False)
                    # Küldjük a "finished" üzenetet
                    self.encoding_queue.put(("finished", completed, failed, needs_check))
                    return
                
                # STUCK STATE DETECTION: pending tree items exist but all
                # queues are empty and no workers are alive.  This typically
                # happens when NVENC is disabled — find_next_waiting_video()
                # always returns None so the encoding_worker loop cannot
                # produce SVT tasks by itself.  Re-scan the tree and push
                # stalled items into the SVT queue, then restart workers.
                if (has_any_pending
                        and not has_svt_queue_items and not is_svt_processing
                        and not has_active_svt_worker
                        and not has_nvenc_queue_items and not is_nvenc_processing
                        and not has_vmaf_queue_items and not has_audio_edit_queue_items):
                    now_rq = time.time()
                    if now_rq - last_requeue_time >= requeue_cooldown:
                        last_requeue_time = now_rq
                        requeued = self._requeue_stalled_pending_to_svt(
                            initial_min_vmaf, vmaf_step, max_encoded_snapshot,
                            resize_enabled, resize_height,
                            audio_compression_enabled, audio_compression_method
                        )
                        if requeued > 0:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[INFO] Encoding worker: {requeued} stalled task(s) re-queued to SVT\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            self._ensure_svt_workers_running()
                            continue  # Immediately re-check instead of sleeping

                # Van még feldolgozás alatt lévő videó, várunk
                self._wait_for_worker_poll(worker_poll_interval)
                continue
    
            # Graceful stop ellenőrzése - ne feldolgozzuk a videót, ha leállítás kérvényezve van
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            _, _, graceful_stop_video = self.get_encoding_state()
            if graceful_stop_video:
                # Várunk, amíg a folyamatban lévő feladatok befejeződnek
                self._wait_for_worker_poll(worker_poll_interval)
                # Ellenőrizzük, hogy van-e még aktív worker vagy queue-ban várakozó feladat
                with self.nvenc_selection_lock:
                    is_nvenc_processing = len(self.nvenc_processing_videos) > 0
                # LIST-BASED QUEUE: Check new list-based queue instead of old SVT_QUEUE
                with self.task_list_lock:
                    has_svt_queue_items = len(getattr(self, 'pending_svt_tasks', [])) > 0
                    has_nvenc_queue_items = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
                with self.nvenc_selection_lock:
                    is_svt_processing = len(self.svt_processing_videos) > 0
                
                # Ha nincs több aktív worker és nincs queue-ban várakozó feladat, kilépünk
                if not is_nvenc_processing and not has_svt_queue_items and not is_svt_processing and not has_nvenc_queue_items:
                    break
                # Folytatjuk a várakozást, de NEM feldolgozzuk a videót
                continue
    
            # A "VIDEÓ X/Y" üzenet a worker-ben lesz kiírva, ahol már tudjuk, hogy melyik worker dolgozik rajta
            # Így elkerüljük, hogy minden üzenet az első logger-be menjen
            
            # Ellenőrizzük, hogy a forrás videó létezik-e
            if not video_path.exists():
                with console_redirect(self.nvenc_logger):
                    print(t('log_source_not_found').format(path=video_path))
                if video_path in self.video_items:
                    item_id = self.video_items[video_path]
                    current_values, _ = self._get_tree_data_snapshot(item_id)
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_source_missing'), "-", "-", "-", "-", "-", "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                failed += 1
                self.encoding_queue.put(("progress_bar", completed + failed + needs_check))
                continue
    
            output_file = get_output_filename(video_path, self.source_path, self.dest_path)
            if video_path in self.video_items:
                item_id = self.video_items[video_path]
            else:
                continue
            if output_file.exists():
                # Ellenőrizzük a fájl méretét és időtartamát
                file_size = output_file.stat().st_size
                should_delete = False
                
                # Ha 0 byte méretű, törölhetjük
                if file_size == 0:
                    should_delete = True
                    with console_redirect(self.nvenc_logger):
                        print(f"[WARN] Célfájl 0 byte méretű, törlés és újrakódolás: {output_file.name}")
                else:
                    # Ellenőrizzük az időtartamot
                    source_duration, _ = get_video_info(video_path)
                    output_duration, _ = get_video_info(output_file)
                    
                    if source_duration is not None and output_duration is not None:
                        # Ha a célfájl rövidebb, mint a forrás (több mint 1 másodperc különbség), töröljük
                        if output_duration < source_duration - 1.0:
                            should_delete = True
                            with console_redirect(self.nvenc_logger):
                                output_duration_str = format_localized_number(output_duration, decimals=1) if output_duration is not None else "-"
                                source_duration_str = format_localized_number(source_duration, decimals=1) if source_duration is not None else "-"
                                print(f"[WARN] Célfájl rövidebb ({output_duration_str}s) mint a forrás ({source_duration_str}s), törlés és újrakódolás: {output_file.name}")
                
                if should_delete:
                    if LOG_WRITER:
                        try:
                            reason = "0 byte" if file_size == 0 else f"rövidebb ({format_localized_number(output_duration, decimals=1)}s < {format_localized_number(source_duration, decimals=1)}s)"
                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: {reason} | fájl: {output_file.name} | video: {video_path.name}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    try:
                        output_file.unlink()
                    except Exception as e:
                        with console_redirect(self.nvenc_logger):
                            print(t('log_file_delete_error').format(error=e))
                else:
                    # Fájl rendben van, jelöljük késznek
                    orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                    orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    
                    # Státusz frissítése "Kész (létezik)"-re
                    self.mark_encoding_completed(item_id, t('status_completed_exists'), "-", "-", "-", orig_size_display, new_size_mb, change_percent)
                    
                    completed += 1
                    # progress_bar update is in mark_encoding_completed
                    continue
    
            # Az aktuális videó indexét beállítjuk (opcionális, csak debug célra)
            if video_path in self.video_files:
                self.current_video_index = self.video_files.index(video_path)
            item_id = self.video_items[video_path]
            current_values, _ = self._get_tree_data_snapshot(item_id)
            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
    
            # Ha a betöltött státusz már SVT-AV1 queue-ban vár, akkor automatikusan SVT queue-ba helyezzük
            # Ne próbáljuk újra NVENC-cel
            if "SVT-AV1" in current_status and ("queue-ban vár" in current_status or "várakozás" in current_status.lower() or "vár" in current_status.lower()):
                with console_redirect(self.svt_logger):
                    print(f"\n[WARN] Videó már SVT-AV1 queue-ban van (betöltött státusz) -> SVT queue-ba újrahelyezés: {video_path.name}")
                
                valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                subtitle_files = valid_subtitles
                max_encoded = max_encoded_snapshot
                
                svt_task = {
                    'video_path': video_path,
                    'output_file': output_file,
                    'subtitle_files': subtitle_files,
                    'invalid_subtitles': invalid_subtitles,
                    'item_id': item_id,
                    'orig_size_str': orig_size_str,
                    'initial_min_vmaf': initial_min_vmaf,
                    'vmaf_step': vmaf_step,
                    'max_encoded': max_encoded,
                    'resize_enabled': resize_enabled,
                    'resize_height': resize_height,
                    'audio_compression_enabled': audio_compression_enabled,
                    'audio_compression_method': audio_compression_method,
                    'reason': 'resume_from_json'
                }
                # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
                # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
                _, _, graceful_stop_resume = self.get_encoding_state()
                if graceful_stop_resume:
                    # Ne indítsunk új feladatot, ha graceful stop kérvényezve van
                    continue
                
                # LIST-BASED QUEUE: Resume task
                success = self.add_to_svt_queue(
                    video_path=video_path, item_id=item_id, task_type='encode', is_manual=False,
                    output_file=output_file, subtitle_files=subtitle_files,
                    invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                    initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                    max_encoded=max_encoded,
                    resize_enabled=resize_enabled, resize_height=resize_height,
                    audio_compression_enabled=audio_compression_enabled,
                    audio_compression_method=audio_compression_method,
                    reason='resume_from_json'
                )
                if success:
                    # CRITICAL FIX: Ensure SVT workers are running after adding task
                    self._ensure_svt_workers_running()

                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik  # JSON mentés SVT queue-ba kerülés után
                continue
    
            valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
            subtitle_files = valid_subtitles
            max_encoded = max_encoded_snapshot
    
            def status_callback(msg):
                self.encoding_queue.put(("status_only", item_id, msg))
    
            def progress_callback(msg):
                self.encoding_queue.put(("progress", item_id, msg))
                # Becsült befejezési idő számítása a progress alapján (frame szám alapján számolódik)
                self.update_estimated_end_time_from_progress(item_id, msg)
    
            # Normál folyamat - nincs task itt, ez a normál video_files feldolgozás
            # skip_crf_search csak a manuális újrakódolásnál van, ami külön worker-ben történik

            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            _, _, graceful_stop_normal = self.get_encoding_state()
            if graceful_stop_normal:
                break
    
            # Kezdeti státusz a cél VMAF értékkel
            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
            localized_vmaf = format_localized_number(initial_min_vmaf, decimals=2)
            self.encoding_queue.put(("update", item_id, f"NVENC CRF keresés (VMAF: {localized_vmaf})...", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            
            # Kezdési időpont tárolása
            self.encoding_start_times[item_id] = time.time()
    
            # NOTE: Do NOT add video_path to nvenc_processing_videos here!
            # The add_to_nvenc_queue() call will check for duplicates and fail if we add it preemptively.
            # Let get_next_nvenc_task() add it to the processing set when a worker picks it up.
            
            # Státusz frissítése: NVENC queue-ban vár (pending tag - kék szín)
            current_values, _ = self._get_tree_data_snapshot(item_id)
            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
            # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            _, _, graceful_stop_queue = self.get_encoding_state()
            if graceful_stop_queue:
                # Ne indítsunk új feladatot, ha graceful stop kérvényezve van
                # Eltávolítjuk a videót a processing set-ből, hogy ne maradjon ott
                with self.nvenc_selection_lock:
                    self.nvenc_processing_videos.discard(video_path)
                continue
            
            self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put(("tag", item_id, "pending"))
            # Rövid átadás az UI queue-nak, de ne álljon fél másodpercet a worker.
            self._wait_for_worker_poll(0.05)
            
            # Több workeres NVENC megoldás: NVENC queue-ba tesszük a feladatot
            nvenc_task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': subtitle_files,
                'invalid_subtitles': invalid_subtitles,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'initial_min_vmaf': initial_min_vmaf,
                'vmaf_step': vmaf_step,
                'max_encoded': max_encoded,
                'resize_enabled': resize_enabled,
                'resize_height': resize_height,
                'audio_compression_enabled': audio_compression_enabled,
                'audio_compression_method': audio_compression_method,
                'reason': 'start_encoding'
            }
            # LIST-BASED QUEUE: Resume NVENC task
            success = self.add_to_nvenc_queue(
                video_path=video_path, item_id=item_id, is_manual=False,
                output_file=output_file, subtitle_files=subtitle_files,
                invalid_subtitles=invalid_subtitles, orig_size_str=orig_size_str,
                initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step,
                max_encoded=max_encoded,
                resize_enabled=resize_enabled, resize_height=resize_height,
                audio_compression_enabled=audio_compression_enabled,
                audio_compression_method=audio_compression_method,
                reason='start_encoding'
            )
            if success:
                # CRITICAL FIX: Ensure NVENC workers are running after adding task
                self._ensure_nvenc_workers_running()
            else:
                completed_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.encoding_queue.put(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "failed"))
                failed += 1
            continue
    
        # Várakozás a befejezésre (csak ha nem leállítás kérés történt)
        # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
        _, _, graceful_stop_end = self.get_encoding_state()
        if not graceful_stop_end and not STOP_EVENT.is_set():
            for _ in range(100):
                if NVENC_QUEUE.unfinished_tasks == 0:
                    break
                self._wait_for_worker_poll(0.1)
            # SVT_QUEUE.join() removed - SVT uses list-based queue now

        if stop_requested or STOP_EVENT.is_set():
            with console_redirect(self.nvenc_logger):
                print(f"\n{t('log_immediate_stop_encoding')}\n")
            return

        # Re-check graceful stop state after join
        _, _, graceful_stop_final = self.get_encoding_state()
        if graceful_stop_final:
            with console_redirect(self.nvenc_logger):
                print(f"\n[STOP] Leállítás kérése - új feladatok nem indulnak\n")
            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
            self.encoding_queue.put(("paused", completed, failed, needs_check))
            return
    
        # Várunk, amíg minden queue kiürül
        NVENC_QUEUE.join()
        # SVT_QUEUE.join() removed - SVT uses list-based queue now
        
        # Számoljuk meg a befejezett videókat a tree-ből
        completed, failed, needs_check = self._count_tree_results_for_worker()
        
        with console_redirect(self.nvenc_logger):
            print(f"\n{'#'*80}\n### ENCODING WORKER KÉSZ ###\n{'#'*80}\n")
            print(f"Végeredmény:")
            print(f"  - Kódolások: {completed} OK, {needs_check} ellenőrizendő, {failed} hiba")
        
        # Encoding worker befejeződött - flag frissítése a queue handler által történik (finished üzenetkor)
        # self.encoding_worker_running = False - REMOVED to prevent race condition
        
        # Végső statisztika
        self.encoding_queue.put(("finished", completed, failed, needs_check))
    
    def _periodic_encoding_status_check(self):
        """Periodikus ellenőrzés, hogy van-e még aktív feladat.
        
        Gyorsan ellenőrzi, hogy van-e aktív worker vagy queue-ban várakozó feladat.
        Ha minden kész, visszaállítja a gombot "Start"-ra.
        Ez hatékony, mert csak gyors ellenőrzéseket végez, nem kell végigmenni az összes videón.
        """
        try:
            # SVT konzol fülek frissítése (hogy a leállt (már inaktív) workerek fülei eltűnjenek)
            if hasattr(self, 'refresh_svt_console_tabs') and hasattr(self, 'svt_worker_count'):
                self.refresh_svt_console_tabs(self.svt_worker_count.get())
            
            # NVENC konzol fülek frissítése (hogy a leállt (már inaktív) workerek fülei eltűnjenek)
            if hasattr(self, 'refresh_nvenc_console_tabs') and hasattr(self, 'nvenc_worker_count'):
                self.refresh_nvenc_console_tabs(self.nvenc_worker_count.get())
            
            # Gyors ellenőrzések - nem kell végigmenni az összes videón
            with self.nvenc_selection_lock:
                has_active_nvenc = len(self.nvenc_processing_videos) > 0
            
                has_active_svt = len(self.svt_processing_videos) > 0
            
            has_active_vmaf = len(self.vmaf_processing_videos) > 0
            
            # LIST-BASED QUEUE: Check list-based queues instead of old queue.Queue
            with self.task_list_lock:
                has_queue_items = (len(getattr(self, 'pending_nvenc_tasks', [])) > 0 or 
                                 len(getattr(self, 'pending_svt_tasks', [])) > 0 or 
                                 not VMAF_QUEUE.empty() or
                                 (hasattr(self, 'manual_nvenc_tasks') and len(self.manual_nvenc_tasks) > 0))
            
            # Encoding worker thread ellenőrzése (ha létezik)
            has_encoding_worker_thread = (hasattr(self, 'encoding_worker_thread') and 
                                        self.encoding_worker_thread and 
                                        self.encoding_worker_thread.is_alive())
            has_encoding_worker_flag = getattr(self, 'encoding_worker_running', False)
            is_queue_loading = getattr(self, 'is_queue_loading', False)
            
            # Ha van aktív worker vagy queue-ban várakozó feladat, akkor még dolgozik
            
            if has_active_nvenc or has_active_svt or has_active_vmaf or has_queue_items or has_encoding_worker_thread or has_encoding_worker_flag or is_queue_loading:
                # Még dolgozik, folytatjuk a következő ellenőrzést
                # DEADLOCK FIX: Check if app is closing before scheduling next check
                try:
                    if not is_app_closing() and self.root.winfo_exists():
                        self.root.after(1000, self._periodic_encoding_status_check)  # 1 másodperc múlva újra
                except tk.TclError:
                    pass  # root already destroyed
                return
            
            # Ha nincs aktív worker és nincs queue-ban várakozó feladat, de is_encoding még True
            # akkor valószínűleg befejeződött, de nem küldték el a "finished" üzenetet
            if self.is_encoding:
                # Ellenőrizzük, hogy van-e pending státuszú videó (hatékony ellenőrzés)
                has_pending = self.has_pending_tasks()
                
                if not has_pending:
                    # Nincs pending státusz, befejeződött
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(
                        is_encoding=False,
                        encoding_worker_running=False,
                        graceful_stop_requested=False
                    )
                    # VMAF munkálat ellenőrzése
                    has_vmaf_work = (not VMAF_QUEUE.empty()) or (hasattr(self, 'vmaf_thread') and self.vmaf_thread and self.vmaf_thread.is_alive())
                    if not has_vmaf_work:
                        # Visszaállítjuk a gombot
                        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.DISABLED)
                        self.load_videos_btn.config(state=tk.NORMAL)
                        if hasattr(self, 'status_label'):
                            self.status_label.config(text=t('status_ready'))
            
            # Folytatjuk a következő ellenőrzést
            # DEADLOCK FIX: Check if app is closing before scheduling next check
            try:
                if not is_app_closing() and self.root.winfo_exists():
                    self.root.after(1000, self._periodic_encoding_status_check)  # 1 másodperc múlva újra
            except tk.TclError:
                pass  # root already destroyed
        except Exception as e:
            # Hiba esetén is folytatjuk, hogy ne szakadjon meg
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Periodikus ellenőrzés hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            # DEADLOCK FIX: Check if app is closing before scheduling next check
            try:
                if not is_app_closing() and self.root.winfo_exists():
                    self.root.after(1000, self._periodic_encoding_status_check)  # 1 másodperc múlva újra
            except tk.TclError:
                pass  # root already destroyed

    def _ensure_worker_log_files_ready(self):
        """Ensure worker console log files exist and are open (including autotest mode)."""
        def _safe_log(msg):
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(msg)
                    LOG_WRITER.flush()
                except Exception:
                    pass

        # Keep tab visibility synced with configured worker counts.
        try:
            if hasattr(self, 'refresh_nvenc_console_tabs') and hasattr(self, 'nvenc_worker_count'):
                self.refresh_nvenc_console_tabs(self.nvenc_worker_count.get())
            if hasattr(self, 'refresh_svt_console_tabs') and hasattr(self, 'svt_worker_count'):
                self.refresh_svt_console_tabs(self.svt_worker_count.get())
        except Exception as e:
            _safe_log(f"[WARN] AUTOTEST: Worker tab refresh warning: {e}\n")

        # Reopen any closed/missing worker log files so worker output always has a target file.
        def _ensure_open(files_attr, paths_attr, label):
            files = getattr(self, files_attr, None)
            paths = getattr(self, paths_attr, None)
            if not isinstance(files, list) or not isinstance(paths, list):
                return
            for idx, path_obj in enumerate(paths):
                if idx >= len(files):
                    files.append(None)
                current = files[idx]
                if current is not None and not getattr(current, "closed", True):
                    continue
                try:
                    files[idx] = open(path_obj, "a", encoding="utf-8")
                    _safe_log(f"[INFO] AUTOTEST: Reopened {label} worker log #{idx + 1}: {path_obj}\n")
                except Exception as e:
                    _safe_log(f"[WARN] AUTOTEST: Failed to reopen {label} worker log #{idx + 1}: {e}\n")

        _ensure_open('nvenc_log_files', 'nvenc_log_paths', 'NVENC')
        _ensure_open('svt_log_files', 'svt_log_paths', 'SVT')

    def _autotest_log(self, message):
        """Write a single autotest log line safely."""
        line = f"[AUTOTEST] {message}\n"
        if LOG_WRITER:
            try:
                LOG_WRITER.write(line)
                LOG_WRITER.flush()
                return
            except Exception:
                pass
        try:
            print(line, end="")
        except Exception:
            pass

    def _autotest_dump_tree_state(self, phase="snapshot"):
        """Dump current tree and queue state to JSON for post-mortem analysis."""
        try:
            tree = getattr(self, 'tree', None)
            if tree is None:
                return None

            profile = (getattr(self, 'autotest_profile', '') or 'unknown').lower()
            base_dir = Path(self.db_path).resolve().parent if getattr(self, 'db_path', None) else Path.cwd()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            dump_path = base_dir / f"autotest_tree_dump_{profile}_{phase}_{timestamp}.json"

            columns = list(getattr(tree, "cget", lambda _x: [])("columns") or ())

            def _build_item_payload(item_id):
                item_data = tree.item(item_id)
                values = list(item_data.get('values') or [])
                row = {}
                for idx, value in enumerate(values):
                    key = columns[idx] if idx < len(columns) else f"col_{idx}"
                    row[key] = value
                children = []
                for child_id in tree.get_children(item_id):
                    try:
                        children.append(_build_item_payload(child_id))
                    except Exception:
                        continue
                payload = {
                    'item_id': str(item_id),
                    'text': item_data.get('text', ''),
                    'tags': list(item_data.get('tags') or ()),
                    'row': row,
                    'children': children
                }
                try:
                    raw_meta = dict(getattr(self, 'tree_item_data', {}).get(item_id) or {})
                except Exception:
                    raw_meta = {}

                # Ensure status_code is always present in dump raw_meta for every row.
                status_code = raw_meta.get('status_code')
                if not status_code:
                    try:
                        status_code = normalize_status_to_code(row.get('status', ''))
                    except Exception:
                        status_code = None
                if not status_code:
                    tags_set = set(payload.get('tags') or ())
                    if 'subtitle' in tags_set:
                        status_code = 'subtitle'
                    elif 'completed_copy' in tags_set:
                        status_code = 'completed_copy'
                    elif 'completed' in tags_set:
                        status_code = 'completed'
                    elif 'encoding_svt' in tags_set:
                        status_code = 'svt_queue'
                    elif 'encoding_nvenc' in tags_set:
                        status_code = 'nvenc_queue'
                    elif any(tag in tags_set for tag in ('needs_check', 'needs_check_nvenc', 'needs_check_svt')):
                        status_code = 'needs_check'
                    elif 'failed' in tags_set:
                        status_code = 'failed'
                    elif 'pending' in tags_set:
                        status_code = 'pending'
                if not status_code:
                    status_code = 'unknown'

                raw_meta['status_code'] = status_code
                payload['raw_meta'] = raw_meta
                return payload

            visible_root_ids = list(tree.get_children(""))
            visible_items = []
            for item_id in visible_root_ids:
                try:
                    visible_items.append(_build_item_payload(item_id))
                except Exception:
                    continue

            hidden_items = []
            for item_id in sorted(getattr(self, 'hidden_items', set()), key=lambda x: str(x)):
                try:
                    hidden_items.append(_build_item_payload(item_id))
                except Exception:
                    continue

            queue_snapshot = {}
            try:
                with self.task_list_lock:
                    queue_snapshot = {
                        'pending_svt_tasks': len(getattr(self, 'pending_svt_tasks', [])),
                        'pending_nvenc_tasks': len(getattr(self, 'pending_nvenc_tasks', [])),
                    }
            except Exception:
                pass
            try:
                queue_snapshot['vmaf_queue_size'] = VMAF_QUEUE.qsize()
            except Exception:
                queue_snapshot['vmaf_queue_size'] = None
            try:
                queue_snapshot['audio_queue_size'] = AUDIO_EDIT_QUEUE.qsize()
            except Exception:
                queue_snapshot['audio_queue_size'] = None

            payload = {
                'generated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                'profile': profile,
                'phase': phase,
                'state': {
                    'is_loading_videos': bool(getattr(self, 'is_loading_videos', False)),
                    'is_queue_loading': bool(getattr(self, 'is_queue_loading', False)),
                    'is_encoding': bool(getattr(self, 'is_encoding', False)),
                    'video_files_count': len(getattr(self, 'video_files', [])),
                    'video_items_count': len(getattr(self, 'video_items', {})),
                    'visible_root_count': len(visible_items),
                    'hidden_root_count': len(hidden_items),
                    'load_db_save_completed': bool(getattr(self, 'load_db_save_completed', None) and self.load_db_save_completed.is_set()),
                },
                'queues': queue_snapshot,
                'visible_tree': visible_items,
                'hidden_tree': hidden_items,
            }

            with open(dump_path, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)

            self._autotest_log(f"Tree dump saved: {dump_path}")
            return dump_path
        except Exception as e:
            self._autotest_log(f"Tree dump failed ({phase}): {e}")
            return None

    def _autotest_start_profile(self):
        """Start profile-driven autotest workflow."""
        profile = (getattr(self, 'autotest_profile', '') or '').lower()
        if profile not in ('mini', 'medium', 'maxi', 'legacy'):
            self._autotest_log(f"Unknown profile '{profile}', skipping autotest")
            return

        if profile == 'mini':
            self._autotest_log("Mini profile selected: scheduling exit in 10 seconds")
            self.root.after(10000, self._autotest_exit)
            return

        if profile == 'legacy':
            self._autotest_log("Legacy profile selected, starting encode workflow directly")
            self.root.after(1000, self._autotest_start_encoding)
            return

        self._autotest_wait_started_at = time.time()
        self._autotest_load_idle_since = None
        self._autotest_log(f"{profile} profile: waiting for saved-state loading to finish")
        self.root.after(1000, self._autotest_wait_for_load_completion)

    def _autotest_wait_for_load_completion(self):
        """Wait until saved-state loading and post-load DB save are done."""
        profile = (getattr(self, 'autotest_profile', '') or '').lower()
        if profile not in ('medium', 'maxi'):
            return

        now = time.time()
        started_at = getattr(self, '_autotest_wait_started_at', now)
        elapsed = now - started_at
        load_done = not bool(getattr(self, 'is_loading_videos', False))
        queue_load_done = not bool(getattr(self, 'is_queue_loading', False))
        db_save_done = bool(getattr(self, 'load_db_save_completed', None) and self.load_db_save_completed.is_set())
        has_source = bool(self.source_entry.get().strip()) if hasattr(self, 'source_entry') else False
        has_videos = bool(getattr(self, 'video_items', {})) or bool(getattr(self, 'video_files', []))

        if load_done and queue_load_done:
            idle_since = getattr(self, '_autotest_load_idle_since', None)
            if idle_since is None:
                self._autotest_load_idle_since = now
                idle_since = now

            idle_elapsed = now - idle_since
            if db_save_done or idle_elapsed >= 60.0:
                if not db_save_done:
                    self._autotest_log("Warning: load DB save completion flag not set after 60s idle, continuing")

                self._autotest_dump_tree_state("post_load")

                if profile == 'medium':
                    self._autotest_log("Medium profile completed, exiting")
                    self.root.after(300, self._autotest_exit)
                    return

                self._autotest_log("Maxi profile: starting encoding phase")
                self.root.after(300, self._autotest_start_encoding)
                return
        else:
            self._autotest_load_idle_since = None

        if elapsed >= 900.0:
            self._autotest_log("Timeout while waiting for load completion, dumping state and exiting")
            self._autotest_dump_tree_state("load_timeout")
            self.root.after(300, self._autotest_exit)
            return

        if elapsed >= 20.0 and not has_source and not has_videos and load_done and queue_load_done:
            self._autotest_log("No saved state detected, dumping state and exiting")
            self._autotest_dump_tree_state("no_saved_state")
            self.root.after(300, self._autotest_exit)
            return

        self.root.after(1000, self._autotest_wait_for_load_completion)

    def _autotest_start_encoding(self):
        """Autotest mode: start encoding phase."""
        profile = (getattr(self, 'autotest_profile', '') or '').lower()
        self._autotest_log(f"Start requested (profile={profile})")
        self._ensure_worker_log_files_ready()
        # Keep worker logs separated from main log (no duplication).
        for logger in getattr(self, 'svt_loggers', []) + getattr(self, 'nvenc_loggers', []):
            try:
                logger.mirror_to_main_log = False
            except Exception:
                pass
        # If loading is still running, retry start shortly instead of exiting early.
        if getattr(self, 'is_loading_videos', False):
            self._autotest_log("Video loading still in progress, retrying in 1 second")
            self.root.after(1000, self._autotest_start_encoding)
            return
        has_video_items = bool(getattr(self, 'video_items', {}))
        has_video_files = bool(getattr(self, 'video_files', []))
        source_path_str = self.source_entry.get().strip() if hasattr(self, 'source_entry') else ""
        if not has_video_items and not has_video_files and not source_path_str:
            self._autotest_log("No videos/source available, dumping state and exiting")
            self._autotest_dump_tree_state("no_videos_before_start")
            self.root.after(300, self._autotest_exit)
            return
        # Start flow is robust: it can auto-load and auto-start if list is not ready yet.
        self.start_encoding()
        self._autotest_dump_tree_state("after_start_request")
        # Start stop timer exactly once per autotest run.
        if not getattr(self, '_autotest_stop_scheduled', False):
            self._autotest_stop_scheduled = True
            stop_delay_ms = 60000
            if profile == 'maxi':
                stop_delay_ms = 300000  # 5 minutes
            self._autotest_log(f"Stop timer started: {int(stop_delay_ms / 1000)} seconds")
            self.root.after(stop_delay_ms, self._autotest_stop_and_exit)
    def _autotest_stop_and_exit(self):
        """Autotest mode: immediate stop then exit."""
        self._autotest_log("Stop timer elapsed, requesting immediate stop")
        # Immediate stop.
        self.stop_encoding_immediate()
        self._autotest_dump_tree_state("after_immediate_stop")
        # Wait a bit so stop/save operations can progress.
        self._autotest_log("Waiting 3 seconds before exit")
        self.root.after(3000, self._autotest_exit)
    def _autotest_exit(self):
        """Autotest mode: exit application using normal close path."""
        if getattr(self, '_autotest_exit_started', False):
            return
        self._autotest_exit_started = True
        self._autotest_log("Exiting")
        self._autotest_dump_tree_state("pre_exit")
        close_handler = getattr(self, '_on_closing_handler', None)
        if callable(close_handler):
            try:
                close_handler()
                return
            except Exception as e:
                self._autotest_log(f"Close handler failed, fallback destroy: {e}")
        try:
            self.root.quit()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass
    def _is_video_actively_encoding(self, video_path):
        """
        Ellenőrzi, hogy egy videó jelenleg aktívan kódolás alatt van-e.
        
        Args:
            video_path: A videó elérési útja (Path objektum)
        
        Returns:
            tuple: (is_encoding: bool, task_info: dict or None, queue_type: str or None)
            - is_encoding: True, ha folyamatban van az átkódolás
            - task_info: A feladat adatai (ha queue-ban van vagy aktívan dolgozik rajta)
            - queue_type: 'svt', 'nvenc', vagy None
        """
        # 1. Ellenőrizzük az aktívan dolgozó SVT workereket
        if hasattr(self, 'svt_processing_videos'):
            with self.nvenc_selection_lock:
                if video_path in self.svt_processing_videos:
                    # Keressük meg a feladatot a pending_svt_tasks listában
                    with self.task_list_lock:
                        if hasattr(self, 'pending_svt_tasks'):
                            for task in self.pending_svt_tasks:
                                if task.get('video_path') == video_path and task.get('type') == 'encode':
                                    return (True, task, 'svt')
                    # Ha nincs a listában, akkor egyszerű task dict-et adunk vissza
                    return (True, {'video_path': video_path, 'type': 'encode'}, 'svt')
        
        # 2. Ellenőrizzük az aktívan dolgozó NVENC workereket
        if hasattr(self, 'nvenc_processing_videos'):
            with self.nvenc_selection_lock:
                if video_path in self.nvenc_processing_videos:
                    # NVENC manual tasks listából keresés
                    if hasattr(self, 'manual_nvenc_tasks'):
                        for task in self.manual_nvenc_tasks:
                            if task.get('video_path') == video_path:
                                return (True, task, 'nvenc')
                    return (True, {'video_path': video_path}, 'nvenc')
        
        # 3. Ellenőrizzük VMAF számítást
        # VMAF számítás folyamatban van, de ez NEM blokkolja a manuális újrakódolást
        # (A manuális kódolás csak az encoding feladatokat írja felül)
        # VMAF-ot hagyjuk befejezni, majd a manuális kódolás utána fut
        
        # 4. Ellenőrizzük a queue-kat (még nem indult el a munka)
        # SVT queue
        with self.task_list_lock:
            if hasattr(self, 'pending_svt_tasks'):
                for task in self.pending_svt_tasks:
                    if task.get('video_path') == video_path and task.get('type') == 'encode':
                        return (True, task, 'svt')
        
        # NVENC queue - automata feladatok (pending_nvenc_tasks)
        with self.task_list_lock:
            if hasattr(self, 'pending_nvenc_tasks'):
                for task in self.pending_nvenc_tasks:
                    if task.get('video_path') == video_path:
                        return (True, task, 'nvenc')
        
        # NVENC queue - manuális feladatok (manual_nvenc_tasks)
        if hasattr(self, 'manual_nvenc_tasks'):
            for task in self.manual_nvenc_tasks:
                if task.get('video_path') == video_path:
                    return (True, task, 'nvenc_manual')
        
        return (False, None, None)

    def stop_encoding_for_video(self, video_path, cleanup_denoised_master=False):
        """
        Leállítja az adott videóhoz tartozó encoding folyamatot.

        Args:
            video_path: A videó elérési útja (Path objektum)
            cleanup_denoised_master: Ha True, akkor a zajszűrt mesterfájlt is törli
                                      (alapértelmezetten False, mert újrahasználjuk)

        Returns:
            bool: True, ha sikerült leállítani vagy nem volt mit leállítani
        """
        # 1. Keressük meg a feladatot és állapítsuk meg a queue típusát
        is_encoding, task_info, queue_type = self._is_video_actively_encoding(video_path)

        if not is_encoding:
            return True  # Nincs mit leállítani

        # 2. Videó-specifikus stop event beállítása
        # Ez leállítja a zajszűrő/kódoló folyamatokat, mert azok figyelik ezt az event-et
        # FONTOS: Ha az event még nem létezik (race condition - worker még nem hozta létre),
        # akkor létrehozzuk és beállítjuk, így a worker majd ezt a beállított event-et fogja látni
        # Thread-safe access to video_stop_events
        
        # Track if the video was actively processing (worker thread running)
        was_processing = False
        
        # 2. Videó-specifikus stop event beállítása (flat lock struktúra - nincs nesting)
        with self.video_stop_events_lock:
            if video_path not in self.video_stop_events:
                self.video_stop_events[video_path] = threading.Event()
            self.video_stop_events[video_path].set()

        self.log_status(f"[WARN] Stop signal küldve: {video_path.name}")
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"  [WARN] Video stop event set for: {video_path.name}\n")
                LOG_WRITER.flush()
            except Exception:
                pass

        # 3. Queue-ból eltávolítás
        with self.task_list_lock:
            # SVT queue
            if hasattr(self, 'pending_svt_tasks'):
                original_count = len(self.pending_svt_tasks)
                self.pending_svt_tasks = [
                    task for task in self.pending_svt_tasks
                    if task.get('video_path') != video_path or task.get('type') != 'encode'
                ]
                removed_count = original_count - len(self.pending_svt_tasks)
                if removed_count > 0 and LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"  [OK] SVT queue-ból eltávolítva: {removed_count} feladat ({video_path.name})\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            # NVENC automata feladatok (pending_nvenc_tasks)
            if hasattr(self, 'pending_nvenc_tasks'):
                original_count = len(self.pending_nvenc_tasks)
                self.pending_nvenc_tasks = [
                    task for task in self.pending_nvenc_tasks
                    if task.get('video_path') != video_path
                ]
                removed_count = original_count - len(self.pending_nvenc_tasks)
                if removed_count > 0 and LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"  [OK] NVENC automata queue-ból eltávolítva: {removed_count} feladat ({video_path.name})\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            # NVENC manuális feladatok (manual_nvenc_tasks)
            if hasattr(self, 'manual_nvenc_tasks'):
                original_count = len(self.manual_nvenc_tasks)
                self.manual_nvenc_tasks = [
                    task for task in self.manual_nvenc_tasks
                    if task.get('video_path') != video_path
                ]
                removed_count = original_count - len(self.manual_nvenc_tasks)
                if removed_count > 0 and LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"  [OK] NVENC manuális queue-ból eltávolítva: {removed_count} feladat ({video_path.name})\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

        # 4. Aktív videók listájából eltávolítás
        with self.nvenc_selection_lock:
            if hasattr(self, 'svt_processing_videos') and video_path in self.svt_processing_videos:
                was_processing = True
                self.svt_processing_videos.discard(video_path)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"  [OK] SVT aktív videók listájából eltávolítva: {video_path.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            if hasattr(self, 'nvenc_processing_videos') and video_path in self.nvenc_processing_videos:
                was_processing = True
                self.nvenc_processing_videos.discard(video_path)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"  [OK] NVENC aktív videók listájából eltávolítva: {video_path.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

        # MEMORY LEAK FIX:
        # If the video was NOT in processing set, it means it was only in the Queue.
        # Since we removed it from the Queue, the worker will never see it, and thus
        # the 'finally' block in the worker that cleans up video_stop_events will NEVER run.
        # Therefore, we must clean up the event ourselves here if was_processing is False.
        with self.video_stop_events_lock:
            if not was_processing:
                if video_path in self.video_stop_events:
                    del self.video_stop_events[video_path]
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"  [OK] Video stop event cleaned up (not processing): {video_path.name}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass

        # 5. Cleanup: Részleges fájlok törlése (Lock-on kívül)
        output_file = self.video_to_output.get(video_path)
        if output_file:
            # Töröljük a részleges kimeneti fájlokat
            video_only_file = output_file.parent / (output_file.stem.replace('.av1', '') + '.av1_video_only.mkv')
            if video_only_file.exists():
                try:
                    video_only_file.unlink()
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"  [OK] Részleges fájl törölve: {video_only_file.name}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                except Exception as e:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"  [WARN] Nem sikerült törölni: {video_only_file.name}: {e}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            
            # Zajszűrt mesterfájl kezelése
            if cleanup_denoised_master:
                # Csak akkor töröljük, ha explicit kérés van rá (alapértelmezetten nem)
                denoise_level = self.video_denoise_enabled.get(video_path, 0)
                if denoise_level > 0:
                    # Zajszűrt mesterfájl útvonala
                    master_filename = f"{video_path.stem}_denoised_master.mkv"
                    denoised_path = output_file.parent / master_filename
                    if denoised_path.exists():
                        try:
                            denoised_path.unlink()
                            # Sidecar fájl törlése is
                            denoised_path.with_suffix('.denoise_level').unlink(missing_ok=True)
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"  [OK] Zajszűrt mesterfájl törölve: {denoised_path.name}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except Exception as e:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"  [WARN] Nem sikerült törölni: {denoised_path.name}: {e}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
        
        self.log_status(f"[OK] Leállítás befejezve: {video_path.name}")
        return True

    def _wait_for_video_stop_completion(self, video_path, timeout=20.0, poll_interval=0.05):
        """Wait until a per-video stop event is fully cleaned up by the worker."""
        deadline = time.time() + max(0.0, float(timeout or 0.0))

        while time.time() < deadline:
            try:
                with self.video_stop_events_lock:
                    still_present = video_path in self.video_stop_events
            except Exception:
                still_present = False

            if not still_present:
                return True

            time.sleep(max(0.01, float(poll_interval or 0.05)))

        return False

    def _wait_for_worker_poll(self, timeout=0.1):
        """Pause a worker loop without delaying shutdown responsiveness."""
        wait_timeout = max(0.01, float(timeout or 0.1))

        try:
            return STOP_EVENT.wait(wait_timeout)
        except Exception:
            time.sleep(wait_timeout)
            return STOP_EVENT.is_set()

    def _wait_briefly(self, timeout=0.1):
        """Short interruptible wait usable outside the main worker loop too."""
        return self._wait_for_worker_poll(timeout)
