from .gui_imports import *
from .gui_shared import *

class QueueManagementMixin:
    def _restore_manual_status_suffix(self, item_id, status_text):
        """Re-apply manual CQ suffix for queue/denoise statuses from cached row metadata."""
        status_text = str(status_text or "")
        if not status_text or " (M " in status_text:
            return status_text

        lowered_status = status_text.lower()
        # Do not graft manual queue suffix onto dynamic CRF/VMAF/PSNR states.
        if (
            'crf keres' in lowered_status or
            'crf search' in lowered_status or
            'vmaf számítás' in lowered_status or
            'vmaf calculation' in lowered_status or
            'psnr' in lowered_status
        ):
            return status_text

        status_code = normalize_status_to_code(status_text)
        if status_code not in (
            'svt_queue', 'nvenc_queue', 'denoising', 'encoding',
            'completed', 'completed_svt', 'completed_nvenc', 'completed_copy', 'completed_exists',
            'needs_check', 'needs_check_svt', 'needs_check_nvenc',
        ):
            return status_text

        try:
            row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
        except Exception:
            row_meta = {}

        manual_cq_value = row_meta.get('manual_cq_value')
        manual_quality_check = row_meta.get('manual_quality_check')
        if manual_cq_value is None:
            return status_text

        if status_code in ('svt_queue', 'nvenc_queue') and hasattr(self, '_build_manual_queue_status_text'):
            try:
                return self._build_manual_queue_status_text(status_text, manual_cq_value, manual_quality_check)
            except Exception:
                pass

        if hasattr(self, '_format_manual_status_suffix'):
            try:
                try:
                    cq_for_suffix = float(manual_cq_value)
                    if cq_for_suffix == int(cq_for_suffix):
                        cq_for_suffix = int(cq_for_suffix)
                except (ValueError, TypeError):
                    cq_for_suffix = int(manual_cq_value)
                return status_text + self._format_manual_status_suffix(cq_for_suffix, manual_quality_check)
            except Exception:
                pass

            try:
                cq_value = float(manual_cq_value)
                if cq_value == int(cq_value):
                    cq_value = int(cq_value)
            except (ValueError, TypeError):
                cq_value = None
            if cq_value is None:
                return status_text
            quality_text = str(manual_quality_check or '').strip().upper()
            return f"{status_text} (M CQ:{cq_value}{(' ' + quality_text) if quality_text else ''})"

    def _get_tree_data_snapshot(self, item_id, timeout: float = 1.0) -> tuple[list, tuple]:
        """Return Treeview data safely from either the GUI thread or a worker thread."""
        if threading.current_thread() is threading.main_thread():
            try:
                return list(self.tree.item(item_id, 'values')), self.tree.item(item_id, 'tags') or ()
            except (tk.TclError, KeyError, AttributeError):
                return [], ()

        result = self.request_tree_data_sync(item_id, timeout=timeout)
        if not result:
            return [], ()
        return list(result.get('values', [])), tuple(result.get('tags', ()) or ())

    def _get_nvenc_enabled_snapshot(self) -> bool:
        """Read NVENC enabled state without touching tkinter from worker threads."""
        if threading.current_thread() is threading.main_thread():
            try:
                return bool(self.nvenc_enabled.get())
            except Exception:
                return bool(getattr(self, 'current_nvenc_enabled', False))
        return bool(getattr(self, 'current_nvenc_enabled', False))

    def request_tree_data_sync(self, item_id, timeout: float = 1.0) -> dict | None:
        """Thread-safe synchronous request for tree data from worker threads.

        This method allows worker threads to safely read tree widget data by
        sending a request to the main GUI thread via the encoding queue.

        Args:
            item_id: The Treeview item ID to read data from.
            timeout: Maximum wait time in seconds (default 1.0).

        Returns:
            dict with 'values' (list) and 'tags' (tuple) keys, or None if
            the request timed out or failed.

        Note:
            This should only be called from worker threads, not from the main
            GUI thread (which would deadlock).
        """
        import threading
        result_event = threading.Event()
        result_container: dict = {}
        self.encoding_queue.put(("get_tree_data", item_id, result_event, result_container))
        if result_event.wait(timeout):
            return result_container
        return None

    def has_pending_tasks(self):
        """Check if there are any pending tasks in the queue.
        
        Returns:
            bool: True if there are pending videos, False otherwise.
        """
    
        def log_result(result, reason):
            if LOAD_DEBUG:
                load_debug_log(f"has_pending_tasks -> {result} ({reason}) | items={len(self.video_items)} | videos={len(self.video_files)} | loading={self.is_loading_videos}")
    
        if not VMAF_QUEUE.empty():
            log_result(True, f"VMAF_QUEUE size={VMAF_QUEUE.qsize()}")
            return True
        if not AUDIO_EDIT_QUEUE.empty():
            log_result(True, f"AUDIO_EDIT_QUEUE size={AUDIO_EDIT_QUEUE.qsize()}")
            return True
        # LIST-BASED QUEUE: Check new list-based queue instead of old SVT_QUEUE
        # THREAD-SAFETY FIX: Use task_list_lock when accessing shared lists
        with self.task_list_lock:
            has_svt_pending = len(getattr(self, 'pending_svt_tasks', [])) > 0
            if has_svt_pending:
                log_result(True, f"pending_svt_tasks size={len(self.pending_svt_tasks)}")
                return True
            has_nvenc_pending = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
            if has_nvenc_pending:
                log_result(True, f"pending_nvenc_tasks size={len(self.pending_nvenc_tasks)}")
                return True
            if getattr(self, 'manual_nvenc_tasks', None):
                log_result(True, "manual_nvenc_tasks pending")
                return True
    
        finished_codes = {
            'completed', 'completed_nvenc', 'completed_svt',
            'completed_copy', 'completed_exists',
            'failed', 'source_missing', 'file_missing', 'load_error',
            'needs_check', 'needs_check_nvenc', 'needs_check_svt'
        }
    
        # Végigmegyünk a videókon, de ha találunk egy pending videót, azonnal visszatérünk
        # CRITICAL FIX #6: Thread-safe dict access with snapshot
        with self.video_items_lock:
            video_items_snapshot = list(self.video_items.items())
        
        for video_path, item_id in video_items_snapshot:
            try:
                current_values, tags = self._get_tree_data_snapshot(item_id)
                status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                status_code = normalize_status_to_code(status)

                # Tag-ek ellenőrzése - ha pending/encoding tag van, akkor biztosan van feladat
                # Ezt először ellenőrizzük, mert ez a leggyorsabb
                if any(tag in ('pending', 'encoding_nvenc', 'encoding_svt', 'needs_check', 'needs_check_nvenc', 'needs_check_svt', 'audio_edit') for tag in tags):
                    # Ha a tag pending/encoding, akkor biztosan van feladat, függetlenül a status_code-tól
                    if status_code not in finished_codes:
                        log_result(True, f"pending tag {tags} and status {status_code} for {video_path}")
                        return True
                    # Ha a status_code None, de pending tag van, akkor is van feladat
                    if status_code is None:
                        log_result(True, f"unknown status but pending tag {tags} for {video_path}")
                        return True
    
                # Ha nem tudtuk beazonosítani a kódot, de a tag 'pending'/'encoding', tekintsük feladatnak
                if status_code is None:
                    continue
    
                if status_code not in finished_codes:
                    log_result(True, f"status {status_code} for {video_path}")
                    return True
            except (tk.TclError, KeyError, AttributeError, IndexError) as e:
                # Ha hiba van egy videó ellenőrzésekor, folytatjuk a következővel
                if LOAD_DEBUG:
                    load_debug_log(f"has_pending_tasks: hiba videó ellenőrzésekor ({video_path}): {e}")
                continue
    
        log_result(False, "no pending items")
        return False

    def normalize_queue_statuses(self):
        """NVENC engedély változáskor frissíti a várólisták státuszait."""
        nvenc_on = self.nvenc_enabled.get()
        changed = False
        for video_path, item_id in self.video_items.items():
            values = list(self.tree.item(item_id, 'values'))
            if len(values) <= self.COLUMN_INDEX['status']:
                continue
            status_text = values[self.COLUMN_INDEX['status']]
            status_code = normalize_status_to_code(status_text)
            new_status_code = status_code
            if not nvenc_on and status_code == 'nvenc_queue':
                new_status_code = 'svt_queue'
            elif nvenc_on and status_code == 'svt_queue':
                # When NVENC is enabled, move SVT queue items to NVENC queue
                new_status_code = 'nvenc_queue'
            if new_status_code != status_code:
                row_meta = self.get_tree_item_meta(item_id) if hasattr(self, 'get_tree_item_meta') else {}
                manual_cq_value = row_meta.get('manual_cq_value')
                manual_quality_check = row_meta.get('manual_quality_check')
                base_status = status_code_to_localized(new_status_code)
                if hasattr(self, '_build_manual_queue_status_text'):
                    values[self.COLUMN_INDEX['status']] = self._build_manual_queue_status_text(
                        base_status,
                        manual_cq_value,
                        manual_quality_check
                    )
                else:
                    values[self.COLUMN_INDEX['status']] = base_status
                self.tree.item(item_id, values=tuple(values))
                if hasattr(self, 'set_tree_item_meta'):
                    self.set_tree_item_meta(
                        item_id,
                        status_code=new_status_code,
                        status_display=values[self.COLUMN_INDEX['status']]
                    )
                changed = True
        if changed:
            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
            pass

    def find_next_waiting_video(self):
        """Megkeresi az első videót, ami NVENC queue-ban vár (nem SVT-AV1 queue-ban) - sorszám szerint rendezve"""
        # Ha az NVENC nincs engedélyezve, ne keressünk NVENC queue-ban várakozó videókat
        if not self._get_nvenc_enabled_snapshot():
            return None
        
        # Összegyűjtjük az összes pending videót sorszám szerint rendezve
        pending_videos = []
        
        # Optimalizálás: csak a szükséges videókat ellenőrizzük (gyorsabb)
        with self.video_items_lock:
            video_items_snapshot = list(self.video_items.items())
        for video_path, item_id in video_items_snapshot:
            try:
                # Először gyors ellenőrzések (tree.item hívások)
                current_values, current_tags = self._get_tree_data_snapshot(item_id)
                current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                
                # Kész vagy ellenőrizendő állapotot kihagyjuk (gyors ellenőrzés)
                if is_status_completed(current_status) or "completed" in current_tags or is_status_needs_check(current_status) or "needs_check" in current_tags:
                    continue
                
                # Folyamatban lévő kódolásokat kihagyjuk (gyors ellenőrzés)
                if ("encoding" in current_tags or "encoding_nvenc" in current_tags or "encoding_svt" in current_tags or 
                    "NVENC kódolás" in current_status or "NVENC CRF keresés" in current_status or 
                    "NVENC validálás" in current_status or "SVT-AV1" in current_status):
                    continue
                
                # Csak azokat vesszük figyelembe, amelyek NVENC queue-ban várnak (nem SVT-AV1)
                status_code = normalize_status_to_code(current_status)
                if status_code != 'nvenc_queue' and not ('pending' in current_tags and status_code != 'svt_queue'):
                    continue  # Gyors skip, ha nem NVENC queue
                
                # Ellenőrizzük, hogy a videó már nincs-e feldolgozás alatt (gyors lock ellenőrzés)
                with self.nvenc_selection_lock:
                    if video_path in self.nvenc_processing_videos:
                        continue
                
                # Lassú fájlrendszer műveletek csak akkor, ha már átment a gyors ellenőrzéseken
                # Ellenőrizzük, hogy a videó létezik-e (lassú, de szükséges)
                if not video_path.exists():
                    continue
                
                # Output fájl ellenőrzés (lassú, de szükséges)
                # output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                # if output_file.exists():
                #     # Ellenőrizzük a fájl méretét (gyors ellenőrzés)
                #     try:
                #         file_size = output_file.stat().st_size
                #         if file_size == 0:
                #             # 0 byte fájl, törölni kell, de még nem kész
                #             order_num = self.video_order.get(video_path, 999999)
                #             pending_videos.append((order_num, video_path))
                #             continue
                #     except (OSError, AttributeError):
                #         pass
                #     # Fájl létezik, skip-eljük
                #     continue
                
                # Nincs kész fájl, kódolni kell
                video_order = getattr(self, 'video_order', {})
                order_num = video_order.get(video_path, 999999)
                pending_videos.append((order_num, video_path))
                
                # Optimalizálás: ha már találtunk egy videót, és nincs sorszám követelmény, visszaadhatjuk azonnal
                # De mivel sorszám szerint kell rendezni, folytatjuk a keresést
            except (tk.TclError, KeyError, AttributeError, IndexError) as e:
                # Ha hiba van egy videó ellenőrzésekor, folytatjuk a következővel
                continue
        
        # Sorszám szerint rendezzük és visszaadjuk az elsőt
        if pending_videos:
            pending_videos.sort(key=lambda x: x[0])  # Sorszám szerint rendezés
            return pending_videos[0][1]  # Az első videó
        
        return None

    def check_encoding_queue(self):
        """Process messages from the encoding queue.
        
        Main GUI update loop. Handles log messages, status updates, and debug events
        from worker threads.
        """
        if hasattr(self, 'show_db_update_notification_debounced_pending'):
            try:
                self.show_db_update_notification_debounced_pending()
            except Exception:
                pass

        # Update cached tree order for thread-safe sorting in worker threads
        self._update_cached_tree_order()

        max_messages_per_cycle = 200
        processed_messages = 0
        queue_drained = False
        try:
            while processed_messages < max_messages_per_cycle:
                msg = self.encoding_queue.get_nowait()
                processed_messages += 1

                # Queue loading progress (háttérszálból)
                if msg[0] == "queue_progress":
                    _, current, total = msg
                    self.status_label.config(
                        text=t('status_queue_filling').format(
                            current=f"{current:,}",
                            total=f"{total:,}"
                        )
                    )
                    continue

                # Queue loading befejezve - encoding_worker és extra workerek indítása
                elif msg[0] == "queue_loading_finished":
                    _, svt_queued, nvenc_queued, vmaf_queued = msg
                    self.is_queue_loading = False
                    total_queued = svt_queued + nvenc_queued + vmaf_queued

                    # If triggered by manual re-encode (_manual_start_active), don't reset
                    # encoding state when 0 new tasks were found — the manual task is already running.
                    manual_start = getattr(self, '_manual_start_active', False)
                    self._manual_start_active = False

                    if total_queued == 0 and not manual_start:
                        # Semmi nem került a queue-ba -> reset
                        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.DISABLED)
                        self.load_videos_btn.config(state=tk.NORMAL)
                        with self.encoding_state_lock:
                            self.is_encoding = False
                            self.encoding_worker_running = False
                        self.status_label.config(text="Nincs feldolgozható várólista feladat.")
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(
                                    f"Start skipped: no queueable tasks found "
                                    f"(SVT={svt_queued}, NVENC={nvenc_queued}, VMAF={vmaf_queued}).\n"
                                )
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                    else:
                        # Start gomb átváltás normál stop-ra (queue loading vége)
                        self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                        self.status_label.config(text=t('status_starting'))

                        # NVENC_QUEUE ürítés (régi queue.Queue kompatibilitás)
                        while True:
                            try:
                                NVENC_QUEUE.get_nowait()
                                NVENC_QUEUE.task_done()
                            except queue.Empty:
                                break

                        # VMAF workerek indítása (ha van VMAF task)
                        if not VMAF_QUEUE.empty():
                            if STOP_EVENT.is_set():
                                STOP_EVENT.clear()
                            vmaf_worker_count = self.get_configured_svt_workers()
                            if not hasattr(self, 'vmaf_worker_threads'):
                                self.vmaf_worker_threads = []
                            self.vmaf_worker_threads = [th for th in self.vmaf_worker_threads if th.is_alive()]
                            active_workers = len(self.vmaf_worker_threads)
                            if active_workers < vmaf_worker_count:
                                workers_to_start = vmaf_worker_count - active_workers
                                start_idx = active_workers
                                for i in range(workers_to_start):
                                    worker_idx = start_idx + i
                                    vmaf_thread = threading.Thread(target=self.vmaf_worker, args=(worker_idx,), daemon=True)
                                    vmaf_thread.start()
                                    self.vmaf_worker_threads.append(vmaf_thread)

                        # Audio edit worker (ha kell)
                        if not AUDIO_EDIT_QUEUE.empty():
                            if not getattr(self, 'audio_edit_thread', None) or not self.audio_edit_thread.is_alive():
                                self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
                                self.audio_edit_thread.start()

                        # encoding_worker indítás
                        if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                            self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                            self.encoding_worker_thread.start()

                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(
                                    f"Queue loaded: SVT={svt_queued}, NVENC={nvenc_queued}, VMAF={vmaf_queued}\n"
                                )
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                    continue

                # Queue loading leállítva (felhasználó megszakította)
                elif msg[0] == "queue_loading_stopped":
                    _, svt_queued, nvenc_queued, vmaf_queued = msg
                    self.is_queue_loading = False
                    total_queued = svt_queued + nvenc_queued + vmaf_queued

                    if total_queued > 0:
                        # Van már betöltött feladat - workerek már futnak
                        self.status_label.config(
                            text=f"Queue feltöltés leállítva ({total_queued:,} feladat betöltve)"
                        )
                        # Gomb átváltás normál stop-ra
                        self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)

                        # encoding_worker indítás a részlegesen betöltött queue-val
                        if not (hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive()):
                            self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
                            self.encoding_worker_thread.start()

                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(
                                    f"Queue loading stopped by user: SVT={svt_queued}, NVENC={nvenc_queued}, VMAF={vmaf_queued}\n"
                                )
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                    else:
                        # Semmi nem töltődött be -> teljes reset
                        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.DISABLED)
                        self.load_videos_btn.config(state=tk.NORMAL)
                        with self.encoding_state_lock:
                            self.is_encoding = False
                            self.encoding_worker_running = False
                        self.status_label.config(text="Queue feltöltés leállítva")
                    continue

                if msg[0] == "nvenc_log":
                    # Üzenet formátum: ("nvenc_log", worker_idx, logger_idx, log_msg, video_path) vagy régi formátum
                    if len(msg) == 5:
                        _, worker_idx, logger_idx, log_msg, video_path = msg
                    elif len(msg) == 4:
                        _, worker_idx, logger_idx, log_msg = msg
                        video_path = None
                    elif len(msg) == 3:
                        _, worker_idx, log_msg = msg
                        logger_idx = worker_idx  # Régi formátum: logger_idx = worker_idx
                        video_path = None
                    else:
                        _, log_msg = msg
                        worker_idx = 0
                        logger_idx = 0
                        video_path = None
                    
                    # Store log entry for video if video_path is available
                    if video_path and hasattr(self, 'video_logs'):
                        from datetime import datetime
                        video_log_key = make_video_log_key(video_path)
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        
                        if video_log_key:
                            # THREAD-SAFETY FIX: Lock video_logs access
                            with self.video_logs_lock:
                                if video_log_key not in self.video_logs:
                                    self.video_logs[video_log_key] = []
                                self.video_logs[video_log_key].append((timestamp, "NVENC", log_msg))
                    
                    target_console = None
                    if hasattr(self, 'nvenc_consoles') and self.nvenc_consoles:
                        if logger_idx is None or logger_idx < 0:
                            logger_idx = 0
                        # Logger index alapján választunk (nem worker_index!), hogy elkerüljük a race condition-t
                        if len(self.nvenc_consoles) > 0:
                            console_idx = logger_idx % len(self.nvenc_consoles)
                            target_console = self.nvenc_consoles[console_idx]
                    if target_console is None and hasattr(self, 'nvenc_console'):
                        target_console = self.nvenc_console
                    if target_console is not None:
                        target_console.config(state=tk.NORMAL)
                        target_console.insert(tk.END, log_msg)
                        target_console.see(tk.END)
                        target_console.config(state=tk.DISABLED)
                    continue
                elif msg[0] == "svt_log":
                    # Üzenet formátum: ("svt_log", worker_idx, logger_idx, log_msg, video_path) vagy régi formátum
                    if len(msg) == 5:
                        _, worker_idx, logger_idx, log_msg, video_path = msg
                    elif len(msg) == 4:
                        _, worker_idx, logger_idx, log_msg = msg
                        video_path = None
                    elif len(msg) == 3:
                        _, log_msg, video_path = msg
                        worker_idx = 0
                        logger_idx = 0
                    else:
                        _, log_msg = msg
                        worker_idx = 0
                        logger_idx = 0
                        video_path = None
                    
                    # Store log entry for video if video_path is available
                    if video_path and hasattr(self, 'video_logs'):
                        from datetime import datetime
                        video_log_key = make_video_log_key(video_path)
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        
                        if video_log_key:
                            # THREAD-SAFETY FIX: Lock video_logs access
                            with self.video_logs_lock:
                                if video_log_key not in self.video_logs:
                                    self.video_logs[video_log_key] = []
                                self.video_logs[video_log_key].append((timestamp, "SVT", log_msg))
                    
                    target_console = None
                    if hasattr(self, 'svt_consoles') and self.svt_consoles:
                        if logger_idx is None or logger_idx < 0:
                            logger_idx = 0
                        # Logger index alapján választunk
                        if len(self.svt_consoles) > 0:
                            console_idx = logger_idx % len(self.svt_consoles)
                            target_console = self.svt_consoles[console_idx]
                    if target_console is None and hasattr(self, 'svt_console'):
                        target_console = self.svt_console
                    if target_console is not None:
                        target_console.config(state=tk.NORMAL)
                        target_console.insert(tk.END, log_msg)
                        target_console.see(tk.END)
                        target_console.config(state=tk.DISABLED)
                    continue
    
                if msg[0] == "debug_pause":
                    if len(msg) != 5:
                        print(f"HIBA: debug_pause üzenet nem 5 paramétert tartalmaz: {len(msg)} paraméter")
                        continue
                    _, current_step, next_step, file_info, continue_event = msg
                    self.show_debug_dialog(current_step, next_step, file_info, continue_event)
                    continue
    
                if msg[0] == "revert_status_if_not_done":
                    if len(msg) != 4:
                        print(f"HIBA: revert_status_if_not_done üzenet nem 4 paramétert tartalmaz: {len(msg)} paraméter")
                        continue
                    _, item_id, target_status, orig_size_str = msg
                    try:
                        target_status = self._restore_manual_status_suffix(item_id, target_status)
                        current_values = self.tree.item(item_id, 'values')
                        if not current_values:
                            continue
                        
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        # Használjuk a helper függvényeket a státusz ellenőrzéséhez
                        if not is_status_completed(status) and "needs_check" not in tags and not is_status_needs_check(status):
                            # Megőrizzük a többi értéket
                            denoise = current_values[self.COLUMN_INDEX['denoise']] if len(current_values) > self.COLUMN_INDEX['denoise'] else ""
                            hard_rotate_str = current_values[self.COLUMN_INDEX['hard_rotate']] if len(current_values) > self.COLUMN_INDEX['hard_rotate'] else "0"
                            video_name = current_values[self.COLUMN_INDEX['video_name']] if len(current_values) > self.COLUMN_INDEX['video_name'] else ""
                            duration = current_values[self.COLUMN_INDEX['duration']] if len(current_values) > self.COLUMN_INDEX['duration'] else "-"
                            frames = current_values[self.COLUMN_INDEX['frames']] if len(current_values) > self.COLUMN_INDEX['frames'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            
                            # Frissítjük a sort
                            self.tree.item(item_id, values=(denoise, hard_rotate_str, video_name, target_status, "-", "-", "-", "-", orig_size_str, "-", "-", duration, frames, completed_date), tags=("pending",))
                    except Exception as e:
                        print(t('log_status_revert_error').format(error=e))
                    continue
    
                if msg[0] == "update_info":
                    raw_meta = None
                    if len(msg) == 4:
                        _, item_id, duration_str, frames_str = msg
                    elif len(msg) == 5 and isinstance(msg[4], dict):
                        _, item_id, duration_str, frames_str, raw_meta = msg
                    else:
                        continue

                    try:
                        if hasattr(self, 'get_tree_values'):
                            current_values = list(self.get_tree_values(item_id, min_length=len(self.COLUMN_INDEX)))
                            changed = False
                            if duration_str is not None:
                                current_values[self.COLUMN_INDEX['duration']] = duration_str
                                changed = True
                            if frames_str is not None:
                                current_values[self.COLUMN_INDEX['frames']] = frames_str
                                changed = True
                            if changed:
                                self.tree.item(item_id, values=tuple(current_values))

                        display_cache = {}
                        if duration_str is not None:
                            display_cache['duration_display'] = duration_str
                        if frames_str is not None:
                            display_cache['frames_display'] = frames_str
                        if display_cache:
                            self.set_tree_item_meta(item_id, **display_cache)

                        if raw_meta:
                            self.merge_tree_item_meta(item_id, raw_meta)
                        else:
                            parsed_duration_seconds = parse_duration_cell_source_seconds(duration_str)
                            parsed_frame_count = parse_frames_cell_source_count(frames_str)
                            meta_update = {}
                            if parsed_duration_seconds is not None and parsed_duration_seconds > 0:
                                meta_update['source_duration_seconds'] = parsed_duration_seconds
                            if parsed_frame_count is not None and parsed_frame_count > 0:
                                meta_update['source_frame_count'] = parsed_frame_count
                            if parsed_duration_seconds and parsed_frame_count:
                                meta_update['source_fps'] = parsed_frame_count / parsed_duration_seconds
                            if meta_update:
                                self.set_tree_item_meta(item_id, **meta_update)
                    except Exception as e:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"HIBA: update_info hiba: {e}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                    continue

                if msg[0] == "update":
                    duration_str = None
                    frames_str = None
                    raw_meta = None

                    if len(msg) == 14 and isinstance(msg[-1], dict):
                        _, item_id, status, cq, vmaf, psnr, progress, orig_size, new_size, change, duration_str, frames_str, completed_date, raw_meta = msg
                    elif len(msg) == 13:
                        _, item_id, status, cq, vmaf, psnr, progress, orig_size, new_size, change, duration_str, frames_str, completed_date = msg
                    elif len(msg) == 12 and isinstance(msg[-1], dict):
                        _, item_id, status, cq, vmaf, psnr, progress, orig_size, new_size, change, completed_date, raw_meta = msg
                    elif len(msg) == 11:
                        _, item_id, status, cq, vmaf, psnr, progress, orig_size, new_size, change, completed_date = msg
                    else:
                        if len(msg) == 4 and msg[2] == "new_size":
                            _, item_id, _, new_size_str = msg
                            self.encoding_queue.put(("update_new_size", item_id, new_size_str))
                            continue
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"HIBA: update uzenet nem 11/12/13/14 parameteres: {len(msg)}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        continue

                    try:
                        current_selection = self.tree.selection()
                        current_values = self.tree.item(item_id, 'values')
                        denoise_str = current_values[self.COLUMN_INDEX['denoise']] if len(current_values) > self.COLUMN_INDEX['denoise'] else ""
                        hard_rotate_str = current_values[self.COLUMN_INDEX['hard_rotate']] if len(current_values) > self.COLUMN_INDEX['hard_rotate'] else "0"
                        video_name = current_values[self.COLUMN_INDEX['video_name']] if len(current_values) > self.COLUMN_INDEX['video_name'] else ""

                        if not video_name:
                            for video_path, vid_item_id in self.video_items.items():
                                if vid_item_id == item_id:
                                    try:
                                        video_name = self.format_relative_name(video_path)
                                    except Exception as e:
                                        log_error = f"[ERROR] Relativ utvonal hiba (queue update): {video_path} -> {e}"
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(log_error + "\n")
                                                LOG_WRITER.flush()
                                            except Exception:
                                                pass
                                        video_name = video_path.name
                                    break

                        if duration_str is None:
                            duration_str = current_values[self.COLUMN_INDEX['duration']] if len(current_values) > self.COLUMN_INDEX['duration'] else "-"
                        if frames_str is None:
                            frames_str = current_values[self.COLUMN_INDEX['frames']] if len(current_values) > self.COLUMN_INDEX['frames'] else "-"

                        video_path = None
                        for vp, vid in self.video_items.items():
                            if vid == item_id:
                                video_path = vp
                                break

                        status_code = normalize_status_to_code(status)
                        status = self._restore_manual_status_suffix(item_id, status)
                        status_code = normalize_status_to_code(status)
                        needs_output_enrichment = status_code in (
                            'completed',
                            'completed_nvenc',
                            'completed_svt',
                            'completed_copy',
                            'completed_exists',
                            'needs_check',
                            'needs_check_nvenc',
                            'needs_check_svt',
                        )
                        if needs_output_enrichment:
                            target_duration_seconds = parse_duration_cell_target_seconds(duration_str)
                            target_frame_count = parse_frames_cell_target_count(frames_str)

                            if target_duration_seconds is None or target_frame_count is None:
                                source_duration_seconds = self.get_tree_item_meta(item_id, 'source_duration_seconds')
                                source_frame_count = self.get_tree_item_meta(item_id, 'source_frame_count')
                                if source_duration_seconds is None:
                                    source_duration_seconds = parse_duration_cell_source_seconds(duration_str)
                                if source_frame_count is None:
                                    source_frame_count = parse_frames_cell_source_count(frames_str)

                                cached_output_duration = self.get_tree_item_meta(item_id, 'output_duration_seconds')
                                cached_output_frames = self.get_tree_item_meta(item_id, 'output_frame_count')
                                if target_duration_seconds is None:
                                    target_duration_seconds = cached_output_duration
                                if target_frame_count is None:
                                    target_frame_count = cached_output_frames

                                if (target_duration_seconds is None or target_frame_count is None) and video_path:
                                    output_file = self.video_to_output.get(video_path)
                                    if not output_file:
                                        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                                    if output_file and output_file.exists():
                                        try:
                                            (
                                                _,
                                                _,
                                                _,
                                                probed_frame_count,
                                                _,
                                                _,
                                                _,
                                                _,
                                                probed_duration_seconds,
                                                _,
                                            ) = get_output_file_info(output_file)
                                            if target_duration_seconds is None and probed_duration_seconds is not None:
                                                target_duration_seconds = probed_duration_seconds
                                            if target_frame_count is None and probed_frame_count is not None:
                                                target_frame_count = probed_frame_count
                                        except Exception:
                                            pass

                                if target_duration_seconds is not None or target_frame_count is not None:
                                    duration_str, frames_str = self._build_duration_frames_display(
                                        source_duration_seconds=source_duration_seconds,
                                        source_frame_count=source_frame_count,
                                        target_duration_seconds=target_duration_seconds,
                                        target_frame_count=target_frame_count,
                                        show_target=True
                                    )

                                    if isinstance(raw_meta, dict):
                                        if target_duration_seconds is not None:
                                            raw_meta['output_duration_seconds'] = target_duration_seconds
                                        if target_frame_count is not None:
                                            raw_meta['output_frame_count'] = target_frame_count
                                        if target_duration_seconds and target_frame_count and not raw_meta.get('output_fps'):
                                            try:
                                                raw_meta['output_fps'] = target_frame_count / target_duration_seconds
                                            except (ValueError, TypeError, ZeroDivisionError):
                                                pass

                        self.tree.item(item_id, values=(denoise_str, hard_rotate_str, video_name, status, cq, vmaf, psnr, progress, orig_size, new_size, change, duration_str, frames_str, completed_date))

                        self.set_tree_item_meta(
                            item_id,
                            status_display=status,
                            progress_display=progress,
                            orig_size_display=orig_size,
                            new_size_display=new_size,
                            size_change_display=change,
                            duration_display=duration_str,
                            frames_display=frames_str,
                            completed_date=completed_date,
                            denoise_display=denoise_str,
                            hard_rotate_display=hard_rotate_str,
                            status_code=normalize_status_to_code(status)
                        )

                        if video_path:
                            cached_stat = self.video_stat_cache.get(video_path, {})
                            cached_source_size = cached_stat.get('source_size_bytes')
                            cached_source_mtime = cached_stat.get('source_modified_timestamp')
                            if cached_source_size is not None:
                                self.set_tree_item_meta(item_id, source_size_bytes=cached_source_size)
                            elif orig_size != "-" and "MB" in str(orig_size):
                                try:
                                    parsed_source_size = parse_size_to_bytes(orig_size)
                                    if parsed_source_size is not None:
                                        self.set_tree_item_meta(item_id, source_size_bytes=parsed_source_size)
                                except (ValueError, TypeError):
                                    pass
                            if cached_source_mtime is not None:
                                self.set_tree_item_meta(item_id, source_modified_timestamp=cached_source_mtime)

                        if raw_meta:
                            self.merge_tree_item_meta(item_id, raw_meta)
                        else:
                            parsed_duration_seconds = parse_duration_cell_source_seconds(duration_str)
                            parsed_frame_count = parse_frames_cell_source_count(frames_str)
                            parsed_output_duration_seconds = parse_duration_cell_target_seconds(duration_str)
                            parsed_output_frame_count = parse_frames_cell_target_count(frames_str)
                            if parsed_duration_seconds is not None and parsed_duration_seconds > 0:
                                self.set_tree_item_meta(item_id, source_duration_seconds=parsed_duration_seconds)
                            if parsed_frame_count is not None and parsed_frame_count > 0:
                                self.set_tree_item_meta(item_id, source_frame_count=parsed_frame_count)
                            if parsed_duration_seconds and parsed_frame_count:
                                self.set_tree_item_meta(item_id, source_fps=(parsed_frame_count / parsed_duration_seconds))
                            if parsed_output_duration_seconds is not None and parsed_output_duration_seconds > 0:
                                self.set_tree_item_meta(item_id, output_duration_seconds=parsed_output_duration_seconds)
                            if parsed_output_frame_count is not None and parsed_output_frame_count > 0:
                                self.set_tree_item_meta(item_id, output_frame_count=parsed_output_frame_count)
                            if parsed_output_duration_seconds and parsed_output_frame_count:
                                self.set_tree_item_meta(item_id, output_fps=(parsed_output_frame_count / parsed_output_duration_seconds))

                            if cq != "-":
                                try:
                                    cq_val = float(cq.replace(",", ".")) if isinstance(cq, str) else float(cq)
                                    self.set_tree_item_meta(item_id, cq=cq_val)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                self.remove_tree_item_meta_keys(item_id, 'cq')

                            if vmaf != "-":
                                try:
                                    vmaf_clean = vmaf.split('[')[0].strip() if isinstance(vmaf, str) else vmaf
                                    vmaf_val = float(vmaf_clean.replace(",", ".")) if isinstance(vmaf_clean, str) else float(vmaf_clean)
                                    self.set_tree_item_meta(item_id, vmaf=vmaf_val)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                self.remove_tree_item_meta_keys(item_id, 'vmaf')

                            if psnr != "-":
                                try:
                                    psnr_val = float(psnr.replace(",", ".")) if isinstance(psnr, str) else float(psnr)
                                    self.set_tree_item_meta(item_id, psnr=psnr_val)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                self.remove_tree_item_meta_keys(item_id, 'psnr')

                            if new_size != "-" and "MB" in str(new_size):
                                try:
                                    new_size_bytes = parse_size_to_bytes(new_size)
                                    if new_size_bytes:
                                        self.set_tree_item_meta(item_id, output_size_bytes=new_size_bytes)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                self.remove_tree_item_meta_keys(item_id, 'output_size_bytes')

                        temp_status_code = normalize_status_to_code(status)
                        if temp_status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                            video_path = None
                            for vp, vid in self.video_items.items():
                                if vid == item_id:
                                    video_path = vp
                                    break

                            if video_path:
                                output_file = self.video_to_output.get(video_path)
                                if not output_file:
                                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                                if output_file and output_file.exists():
                                    try:
                                        out_stat = output_file.stat()
                                        self.set_tree_item_meta(
                                            item_id,
                                            output_modified_timestamp=out_stat.st_mtime,
                                            output_size_bytes=out_stat.st_size
                                        )
                                    except Exception:
                                        pass

                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError):
                        continue

                    try:
                        status_code = normalize_status_to_code(status)
                        if status_code in (
                            'nvenc_queue',
                            'svt_queue',
                            'vmaf_waiting',
                            'psnr_waiting',
                            'vmaf_psnr_waiting',
                            'audio_edit_queue'
                        ):
                            self.clear_encoding_times(item_id)
                        if self.hide_completed.get():
                            if is_status_completed(status):
                                if item_id not in self.hidden_items:
                                    parent = self.tree.parent(item_id)
                                    if parent == "" and item_id in self.tree.get_children():
                                        self.tree.detach(item_id)
                                        self.hidden_items.add(item_id)
                            else:
                                self._show_hidden_item_if_needed(item_id)
                    except (tk.TclError, KeyError, AttributeError):
                        pass
                elif msg[0] == "tag":
                    _, item_id, tag = msg
                    try:
                        if tag == "completed":
                            current_values = self.tree.item(item_id, 'values')
                            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            if normalize_status_to_code(current_status) == 'completed_copy':
                                tag = "completed_copy"
                        self.tree.item(item_id, tags=(tag,))
                    except (tk.TclError, KeyError, AttributeError):
                        # Item már nem létezik - skip
                        pass
                elif msg[0] == "update_partial":
                    # Thread-safe partial column update from worker threads
                    # Format: ("update_partial", item_id, {column_name: value, ...})
                    _, item_id, column_updates = msg
                    try:
                        current_values = list(self.tree.item(item_id, 'values'))
                        for col_name, value in column_updates.items():
                            if col_name in self.COLUMN_INDEX:
                                idx = self.COLUMN_INDEX[col_name]
                                if idx < len(current_values):
                                    current_values[idx] = value
                        self.tree.item(item_id, values=tuple(current_values))
                    except (tk.TclError, KeyError, AttributeError, IndexError):
                        # Item no longer exists or invalid update - skip
                        pass
                elif msg[0] == "get_tree_data":
                    # Thread-safe synchronous tree data request from worker threads
                    # Format: ("get_tree_data", item_id, result_event, result_container)
                    _, item_id, result_event, result_container = msg
                    try:
                        result_container['values'] = list(self.tree.item(item_id, 'values'))
                        result_container['tags'] = self.tree.item(item_id, 'tags')
                    except (tk.TclError, KeyError, AttributeError):
                        result_container['values'] = []
                        result_container['tags'] = ()
                    result_event.set()
                elif msg[0] == "schedule_hide_completed":
                    # THREAD-SAFETY FIX: Worker thread sends this instead of root.after()
                    # Schedule the hide operation with delays to ensure update messages are processed first
                    if len(msg) > 1:
                        _sc_item_id = msg[1]
                        self.root.after(50, lambda iid=_sc_item_id: self.encoding_queue.put(("hide_completed", iid)))
                        self.root.after(200, self.toggle_hide_completed)
                elif msg[0] == "ensure_svt_workers":
                    # THREAD-SAFETY FIX: Worker thread sends this instead of root.after()
                    self._ensure_svt_workers_running()
                elif msg[0] == "hide_completed":
                    # Hide all completed items - call toggle_hide_completed to refresh all
                    if len(msg) > 1:
                        # If item_id provided, hide just that one
                        _, item_id = msg
                        # Check if item is actually completed before hiding
                        try:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"DEBUG hide_completed message: item_id={item_id}, status='{status}', is_completed={is_status_completed(status)}, already_hidden={item_id in self.hidden_items}\n")
                                    LOG_WRITER.flush()
                                except (OSError, IOError, AttributeError, ValueError):
                                    # Log writer closed/unavailable - non-critical
                                    pass
                            if is_status_completed(status) and item_id not in self.hidden_items:
                                try:
                                    parent = self.tree.parent(item_id)
                                    if parent == "" and item_id in self.tree.get_children():
                                        self.tree.detach(item_id)
                                        self.hidden_items.add(item_id)
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(f"DEBUG hide_completed: Successfully hid item {item_id}\n")
                                                LOG_WRITER.flush()
                                            except (OSError, IOError, AttributeError, ValueError):
                                                # Log writer closed/unavailable - non-critical
                                                pass
                                except (tk.TclError, KeyError, AttributeError) as e:
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"DEBUG hide_completed: Error hiding item {item_id}: {e}\n")
                                            LOG_WRITER.flush()
                                        except (OSError, IOError, AttributeError, ValueError):
                                            # Log writer closed/unavailable - non-critical
                                            pass
                        except (tk.TclError, KeyError, AttributeError) as e:
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"DEBUG hide_completed: Error accessing item {item_id}: {e}\n")
                                    LOG_WRITER.flush()
                                except (OSError, IOError, AttributeError, ValueError):
                                    # Log writer closed/unavailable - non-critical
                                    pass
                    else:
                        # No item_id - refresh all completed items
                        self.toggle_hide_completed()
                elif msg[0] == "progress":
                    _, item_id, progress_msg = msg
                    try:
                        # Mentjük a jelenlegi kijelölést, hogy ne sárgásítsa a sort automatikusan
                        current_selection = self.tree.selection()
                        
                        current_values = self.get_tree_values(item_id)
                        current_values[self.COLUMN_INDEX['progress']] = progress_msg
                        self.tree.item(item_id, values=tuple(current_values))
                        
                        # Visszaállítjuk a kijelölést (vagy töröljük, ha üres volt)
                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError, IndexError):
                        # Item már nem létezik - skip
                        pass
                elif msg[0] == "status_only":
                    _, item_id, status_text = msg
                    try:
                        # Mentjük a jelenlegi kijelölést, hogy ne sárgásítsa a sort automatikusan
                        current_selection = self.tree.selection()
                        
                        current_values = self.get_tree_values(item_id)
                        current_values[self.COLUMN_INDEX['status']] = status_text
                        self.tree.item(item_id, values=tuple(current_values))
                        
                        if hasattr(self, 'set_tree_item_meta'):
                            self.set_tree_item_meta(item_id, status_display=status_text, status_code=normalize_status_to_code(status_text))
                        
                        # Visszaállítjuk a kijelölést (vagy töröljük, ha üres volt)
                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError):
                        pass
                elif msg[0] == "update_new_size":
                    # Csak az "Új méret" oszlop frissítése (manuális encoding méretbecslés)
                    _, item_id, new_size_str = msg
                    try:
                        # Mentjük a jelenlegi kijelölést
                        current_selection = self.tree.selection()
                        
                        current_values = list(self.get_tree_values(item_id))
                        current_values[self.COLUMN_INDEX['new_size']] = new_size_str
                        self.tree.item(item_id, values=tuple(current_values))
                        
                        # Visszaállítjuk a kijelölést
                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError):
                        pass
                elif msg[0] == "save_json":
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    pass
                elif msg[0] == "db_update_saved":
                    self.show_db_update_notification_debounced_pending()
                elif msg[0] == "db_saved":
                    self.show_db_notification()
                elif msg[0] == "progress_bar":
                    _, value = msg
                    # Dinamikusan számoljuk ki a befejezett videók számát a tree-ből
                    completed_count = 0
                    for item_id in self.video_items.values():
                        try:
                            tags = self.tree.item(item_id, 'tags')
                            if 'completed' in tags or 'completed_copy' in tags or 'failed' in tags or 'needs_check' in tags:
                                completed_count += 1
                        except (tk.TclError, KeyError, AttributeError):
                            # Item már nem létezik - skip
                            pass
                    total = len(self.video_files) if self.video_files else 1
                    # CRITICAL: Keep progressbar scale aligned with video count.
                    # Copy progress can temporarily change maximum; restore it here.
                    self.progress_bar['maximum'] = total if total > 0 else 1
                    self.progress_bar['value'] = min(completed_count, total)
                    percent = int((completed_count / total) * 100) if total > 0 else 0
                    remaining = total - completed_count
                    self.status_label.config(text=t('status_encoding_progress').format(completed=completed_count, total=total, percent=percent, remaining=remaining))
                elif msg[0] == "copy_progress":
                    # Tuple formátum: (típus, total, current, message)
                    _, copy_data = msg
                    if isinstance(copy_data, tuple) and len(copy_data) >= 1:
                        copy_type = copy_data[0]
                        if copy_type == "copy_error":
                            message = copy_data[1] if len(copy_data) > 1 else "Error copying non-video files."
                            self.status_label.config(text=message)
                            continue
                        if len(copy_data) < 4:
                            continue
                        total, current, message = copy_data[1], copy_data[2], copy_data[3]
                        
                        if copy_type == "copy_start":
                            # Másolás kezdése
                            self.progress_bar['maximum'] = total
                            self.progress_bar['value'] = 0
                            self.status_label.config(text=message)
                        elif copy_type == "copy_progress":
                            # Másolás folyamatban
                            if total <= 0 or current <= 0:
                                # Search/index phase: avoid misleading permanent "0%" display.
                                self.progress_bar['maximum'] = total if total > 0 else 1
                                self.progress_bar['value'] = 0
                                self.status_label.config(text=message)
                            else:
                                self.progress_bar['maximum'] = total
                                self.progress_bar['value'] = current
                                percent = int((current / total) * 100) if total > 0 else 0
                                self.status_label.config(text=f"{message} ({percent}%)")
                        elif copy_type == "copy_done":
                            # Másolás befejezve
                            self.progress_bar['maximum'] = total if total > 0 else 1
                            self.progress_bar['value'] = current if current > 0 else total
                            self.status_label.config(text=message)
                        elif copy_type == "copy_error":
                            # Hiba
                            self.status_label.config(text=message)
                elif msg[0] == "copy_status":
                    _, msg_text = msg
                    msg_text_str = str(msg_text)
                    if "database" in msg_text_str.lower():
                        if hasattr(self, 'db_notification_label'):
                            self.db_notification_label.config(text=msg_text_str, foreground="blue")
                            self.root.after(3000, self.hide_db_notification)
                    else:
                        self.status_label.config(text=msg_text_str)
                elif msg[0] == "db_progress":
                    _, msg_text = msg
                    # db_progress üzeneteket a notification label-ban jelenítjük meg, ne a status_label-ban
                    if hasattr(self, 'db_notification_label'):
                        self.root.after(0, lambda m=msg_text: self.db_notification_label.config(text=m, foreground="blue"))
                elif msg[0] == "db_error":
                    _, msg_text = msg
                    # Aszinkron DB hibák külön (piros) notification-ban jelenjenek meg
                    if hasattr(self, 'db_notification_label'):
                        self.root.after(0, lambda m=msg_text: self.db_notification_label.config(text=m, foreground="red"))
                elif msg[0] == "update_summary":
                    self.update_summary_row()
                elif msg[0] == "finished":
                    _, completed, failed, needs_check = msg
                    
                    # KRITIKUS JAVÍTÁS: Ha van még feladat a queue-ban vagy pending státuszú videó,
                    # akkor a "finished" üzenet téves (pl. race condition miatt), ezért figyelmen kívül hagyjuk.
                    has_pending = self.has_pending_tasks()
                    # LIST-BASED QUEUE: Check if there are tasks in the new list-based queues
                    with self.task_list_lock:
                        has_svt_queue = len(getattr(self, 'pending_svt_tasks', [])) > 0
                        has_nvenc_queue = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
                    has_vmaf_queue = not VMAF_QUEUE.empty()  # VMAF still uses old queue
                    has_queue = has_svt_queue or has_nvenc_queue or has_vmaf_queue
                    
                    if has_pending or has_queue:
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"[WARN] 'finished' üzenet figyelmen kívül hagyva: pending={has_pending}, queue={has_queue}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        continue

                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(encoding_worker_running=False)
                    has_vmaf_work = (not VMAF_QUEUE.empty()) or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive())
                    # Csak akkor jelenítjük meg a sikeres számot, ha vannak problémák is
                    parts = []
                    if needs_check > 0 or failed > 0:
                        # Ha van probléma, akkor a sikeres számot is megjelenítjük
                        parts.append(t('msg_encoding_ok').format(count=completed))
                    if needs_check > 0:
                        parts.append(t('msg_encoding_needs_check').format(count=needs_check))
                    if failed > 0:
                        parts.append(t('summary_failed').format(count=failed))

                    if parts:
                        summary_text = ", ".join(parts)
                    else:
                        # Nincs probléma, egyszerű üzenet
                        summary_text = t('msg_processing_done')
                    # A kódolás befejeződött, így is_encoding-et False-ra állítjuk
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(is_encoding=False, graceful_stop_requested=False)
                    if has_vmaf_work:
                        self.status_label.config(text=f"{t('status_vmaf_calculating')} - {summary_text}")
                        # VMAF munkálat esetén a gomb "Leállítás"-ként maradjon
                        self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.NORMAL)
                    else:
                        self.status_label.config(text=f"{t('status_completed')} {summary_text}")
                        # Start gomb visszaállítása "Start"-ra - EXPLICIT módon, hogy biztosan működjön
                        # Késleltetett hívás is, hogy biztosan működjön, ha valami más felülírja
                        self.root.after(0, lambda: self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL))
                        self.root.after(0, lambda: self.immediate_stop_button.config(state=tk.DISABLED))
                        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.DISABLED)
                    # Videók betöltése gomb aktívvá tétele
                    self.load_videos_btn.config(state=tk.NORMAL)
                    # Biztosítjuk, hogy a gomb állapota helyesen legyen beállítva (VMAF ellenőrzés miatt)
                    # De csak akkor, ha nincs VMAF munka, mert akkor már explicit beállítottuk
                    if not has_vmaf_work:
                        # Késleltetett hívás is, hogy biztosan működjön
                        self.root.after(100, self.update_start_button_state)
                    # Összefoglaló messagebox megjelenítése
                    msg_parts = []
                    # Csak akkor jelenítjük meg a sikeres számot, ha vannak problémák is
                    if needs_check > 0 or failed > 0:
                        msg_parts.append(f"{t('status_processing_finished')}\n")
                        msg_parts.append(t('msg_encoding_ok').format(count=completed))
                    else:
                        # Nincs probléma, egyszerű üzenet
                        msg_parts.append(f"{t('status_processing_finished')}\n")
                        msg_parts.append(t('msg_processing_done'))
                    
                    if needs_check > 0:
                        msg_parts.append(t('msg_encoding_needs_check').format(count=needs_check))
                    if failed > 0:
                        msg_parts.append(t('msg_encoding_failed').format(count=failed))
                    if has_vmaf_work:
                        msg_parts.append(f"\n{t('status_vmaf_calculating')}")
                    # Messagebox csak akkor jelenik meg, ha nincs VMAF munka (különben zavaró lenne)
                    if not has_vmaf_work:
                        self.root.after(100, lambda: messagebox.showinfo(t('msg_done'), '\n'.join(msg_parts)))
                    # THREAD-SAFETY FIX: Pre-cache settings on GUI thread before DB thread
                    _stop_settings_snapshot = self._build_settings_snapshot()
                    # Adatbázis mentés leállítás után
                    def save_db_after_stop():
                        try:
                            self.save_state_to_db(settings_snapshot=_stop_settings_snapshot)
                            try:
                                self.encoding_queue.put_nowait(("db_saved",))
                            except Exception:
                                pass
                        except Exception as e:
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_db after stop] Hiba: {e}")
                    self._start_db_thread(save_db_after_stop, name="SaveDBAfterStop")
                    
                    # Update summary row final time
                    self.update_summary_row()
                elif msg[0] == "paused":
                    _, completed, failed, needs_check = msg
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(
                        is_encoding=False,
                        encoding_worker_running=False,
                        graceful_stop_requested=False
                    )
                    # Start gomb visszaállítása "Start"-ra
                    self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                    self.immediate_stop_button.config(state=tk.DISABLED)
                    # Videók betöltése gomb aktívvá tétele
                    self.load_videos_btn.config(state=tk.NORMAL)
                    # Csak akkor jelenítjük meg a részleteket, ha van valami értékes információ
                    parts = []
                    if completed > 0:
                        parts.append(f"OK: {completed}")
                    if needs_check > 0:
                        parts.append(t('msg_encoding_needs_check').format(count=needs_check))
                    if failed > 0:
                        parts.append(t('msg_encoding_failed').format(count=failed))
                    # Ha van részlet, hozzáadjuk, különben csak a leállítás üzenet
                    if parts:
                        self.status_label.config(text=t('status_paused').format(start_btn=t('btn_start')) + ' – ' + ', '.join(parts))
                    else:
                        self.status_label.config(text=t('status_paused').format(start_btn=t('btn_start')))
        except queue.Empty:
            queue_drained = True

        queue_has_backlog = not self.encoding_queue.empty()

        # If queue is under heavy load, skip expensive button-state recalculation on every cycle.
        if queue_drained or processed_messages < max_messages_per_cycle:
            self.update_start_button_state()
        
        # Periodically refresh console tabs to hide tabs of stopped workers
        if hasattr(self, 'refresh_nvenc_console_tabs') and hasattr(self, 'nvenc_worker_count'):
            try:
                self.refresh_nvenc_console_tabs(self.nvenc_worker_count.get())
            except (AttributeError, tk.TclError):
                pass
        if hasattr(self, 'refresh_svt_console_tabs') and hasattr(self, 'svt_worker_count'):
            try:
                self.refresh_svt_console_tabs(self.svt_worker_count.get())
            except (AttributeError, tk.TclError):
                pass
    
        # Mindig folytatjuk a queue ellenőrzést, még akkor is, ha nincs aktív kódolás
        # (pl. nem-videó fájlok másolása közben is frissüljön a GUI)
        copy_thread_alive = False
        if hasattr(self, 'copy_thread') and self.copy_thread:
            try:
                copy_thread_alive = self.copy_thread.is_alive()
            except (AttributeError, RuntimeError):
                copy_thread_alive = False
        
        # Check if any worker threads are still alive (even if is_encoding is False)
        has_active_workers = False
        if hasattr(self, 'svt_worker_threads'):
            has_active_workers = has_active_workers or any(t.is_alive() for t in self.svt_worker_threads if t)
        if hasattr(self, 'nvenc_worker_threads'):
            has_active_workers = has_active_workers or any(t.is_alive() for t in self.nvenc_worker_threads if t)
        if getattr(self, 'rebuild_running', False):
            has_active_workers = True
        
        next_delay_ms = 30 if queue_has_backlog and not queue_drained else 100
        if self.is_encoding or self.is_queue_loading or copy_thread_alive or has_active_workers:
            self.root.after(next_delay_ms, self.check_encoding_queue)
        elif queue_has_backlog:
            # Ha van üzenet a queue-ban, akkor is folytatjuk
            self.root.after(next_delay_ms, self.check_encoding_queue)

    def mark_encoding_completed(self, item_id, status_text, cq_str, vmaf_str, psnr_str, orig_size_str, new_size_mb, change_percent, completed_date=None, manual_quality_check=None, manual_cq_value=None, vmaf_reference_path=None, denoised_master_path=None):
        """Kódolás befejezésének jelölése és státusz frissítése

        Args:
            manual_quality_check: 'vmaf', 'psnr', 'both', vagy None - manuális kódolásnál megadott minőség ellenőrzés
            manual_cq_value: Manuális CQ érték (pl. 10, 15) - manuális kódolásnál használt CQ érték
            vmaf_reference_path: Original source file for VMAF comparison (for denoised videos)
            denoised_master_path: Denoised master file to cleanup AFTER VMAF finishes
        """
        if completed_date is None:
            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Determine if quality check will be queued BEFORE sending status update
        # This prevents the inline hide logic from hiding the row during the
        # gap between "completed" and "VMAF pending" status updates
        will_queue_quality_check = False
        quality_check_type = None
        if manual_quality_check and manual_quality_check != 'none':
            will_queue_quality_check = True
            quality_check_type = manual_quality_check
        elif getattr(self, 'current_auto_vmaf_psnr', False):
            will_queue_quality_check = True
            quality_check_type = 'both'

        # Resolve the output before switching the row to a pending VMAF/PSNR
        # status. If the output is gone, no quality task can run.
        video_path = None
        with self.video_items_lock:
            for vp, vid in self.video_items.items():
                if vid == item_id:
                    video_path = vp
                    break

        quality_check_output_file = None
        if will_queue_quality_check and video_path:
            with self.video_items_lock:
                quality_check_output_file = self.video_to_output.get(video_path)
            if not quality_check_output_file:
                quality_check_output_file = get_output_filename(video_path, self.source_path, self.dest_path)

        if will_queue_quality_check and not (
            quality_check_output_file and quality_check_output_file.exists()
        ):
            will_queue_quality_check = False

        # If quality check will be queued, use pending status immediately
        # to prevent the row from being hidden by the inline hide logic
        original_completed_status = None
        quality_check_queued = False
        if will_queue_quality_check:
            original_completed_status = status_text
            if manual_cq_value is not None and hasattr(self, '_rebuild_manual_status_text'):
                original_completed_status = self._rebuild_manual_status_text(status_text, manual_cq_value, manual_quality_check)
            if quality_check_type == 'vmaf':
                status_text = t('status_vmaf_pending')
            elif quality_check_type == 'psnr':
                status_text = t('status_psnr_pending')
            else:
                status_text = t('status_vmaf_psnr_pending')
        else:
            if manual_cq_value is not None and hasattr(self, '_rebuild_manual_status_text'):
                status_text = self._rebuild_manual_status_text(status_text, manual_cq_value, manual_quality_check)

        status_code = normalize_status_to_code(status_text)
        self.clear_encoding_times(item_id)
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
        change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"

        raw_meta = {'status_code': normalize_status_to_code(status_text)}
        if manual_cq_value is not None:
            raw_meta['manual_cq_value'] = manual_cq_value
            raw_meta['manual_cq_range'] = 'single'
        if manual_quality_check is not None:
            raw_meta['manual_quality_check'] = manual_quality_check
        cached_output_duration = self.get_tree_item_meta(item_id, 'output_duration_seconds')
        cached_output_frames = self.get_tree_item_meta(item_id, 'output_frame_count')
        cached_output_fps = self.get_tree_item_meta(item_id, 'output_fps')
        if cached_output_duration is not None:
            raw_meta['output_duration_seconds'] = cached_output_duration
        if cached_output_frames is not None:
            raw_meta['output_frame_count'] = cached_output_frames
        if cached_output_fps is not None:
            raw_meta['output_fps'] = cached_output_fps
        if new_size_mb is not None:
            try:
                raw_meta['output_size_bytes'] = int(float(new_size_mb) * 1024 * 1024)
            except (ValueError, TypeError):
                pass
        if cq_str not in (None, '-', ''):
            try:
                raw_meta['cq'] = float(str(cq_str).replace(',', '.'))
            except (ValueError, TypeError):
                pass
        if vmaf_str not in (None, '-', ''):
            try:
                raw_meta['vmaf'] = float(str(vmaf_str).split('[')[0].strip().replace(',', '.'))
            except (ValueError, TypeError):
                pass
        if psnr_str not in (None, '-', ''):
            try:
                raw_meta['psnr'] = float(str(psnr_str).replace(',', '.'))
            except (ValueError, TypeError):
                pass

        # Build "source / target" format for duration and frames columns
        cached_source_duration = self.get_tree_item_meta(item_id, 'source_duration_seconds')
        cached_source_frames = self.get_tree_item_meta(item_id, 'source_frame_count')
        duration_str, frames_str = self._build_duration_frames_display(
            source_duration_seconds=cached_source_duration,
            source_frame_count=cached_source_frames,
            target_duration_seconds=cached_output_duration,
            target_frame_count=cached_output_frames,
            show_target=True
        )

        self.encoding_queue.put((
            "update", item_id, status_text, cq_str, vmaf_str, psnr_str, "100%",
            orig_size_str, new_size_str, change_percent_str, duration_str, frames_str, completed_date, raw_meta
        ))
        # Tag: if quality check pending, use "pending" to avoid green checkmark
        if will_queue_quality_check:
            tag = "pending"
        elif is_status_needs_check(status_text):
            tag = "needs_check"
        elif status_code == 'completed_copy':
            tag = "completed_copy"
        else:
            tag = "completed"
        self.encoding_queue.put(("tag", item_id, tag))
        self.encoding_queue.put(("progress_bar", 0))
        self.encoding_queue.put(("update_summary",))

        # Adatbázis frissítése minden egyes fájl feldolgozása után
        # THREAD-SAFETY FIX: video_items_lock snapshot
        if video_path is None:
            with self.video_items_lock:
                for vp, vid in self.video_items.items():
                    if vid == item_id:
                        video_path = vp
                        break
        
        if video_path:
            # Háttérszálban frissítjük az adatbázist, hogy ne blokkolja az encoding folyamatot
            def update_db_in_thread():
                try:
                    denoise_enabled_val = self.get_tree_item_meta(item_id, 'denoise_enabled')

                    # Save the original completed status to DB (not the pending status shown in tree)
                    db_status_text = original_completed_status if original_completed_status else status_text

                    self.update_single_video_in_db(
                        video_path, item_id, db_status_text, cq_str, vmaf_str, psnr_str,
                        orig_size_str, new_size_mb, change_percent, completed_date,
                        manual_cq_value=manual_cq_value,
                        manual_quality_check=manual_quality_check,
                        denoise_enabled=denoise_enabled_val
                    )

                    db_status_code = normalize_status_to_code(db_status_text)
                    completed_codes = ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists')
                    if db_status_code in completed_codes:
                        output_file = self.video_to_output.get(video_path)
                        if output_file and output_file.exists():
                            self.schedule_track_editor_cache_build(video_path, output_file)
                except Exception as e:
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[WARN] [mark_encoding_completed] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            
            self._start_db_thread(update_db_in_thread, name="MarkEncodingCompletedDB")
        
        if will_queue_quality_check:
            if not video_path:
                # THREAD-SAFETY FIX: video_items_lock snapshot
                with self.video_items_lock:
                    for vp, vid in self.video_items.items():
                        if vid == item_id:
                            video_path = vp
                            break

            if video_path:
                # THREAD-SAFETY FIX: video_to_output access with lock
                output_file = quality_check_output_file

                if output_file and output_file.exists():
                    # Use the original completed status for VMAF task context
                    # (not the pending status we already sent to the tree)
                    completed_status_for_vmaf = original_completed_status or status_text
                    final_status_code_for_vmaf = normalize_status_to_code(completed_status_for_vmaf)

                    check_vmaf = quality_check_type in ('vmaf', 'both')
                    check_psnr = quality_check_type in ('psnr', 'both')

                    added_to_quality_queue = self.add_to_svt_queue(
                        video_path=video_path,
                        item_id=item_id,
                        task_type='vmaf',
                        is_manual=bool(manual_quality_check and manual_quality_check != 'none'),
                        output_file=output_file,
                        orig_size_str=orig_size_str,
                        check_vmaf=check_vmaf,
                        check_psnr=check_psnr,
                        current_cq_str=cq_str,
                        current_vmaf_str=vmaf_str,
                        current_psnr_str="-",
                        current_new_size_str=new_size_str,
                        current_size_change=change_percent_str,
                        current_completed_date=completed_date if completed_date else "",
                        current_status=completed_status_for_vmaf,
                        quality_check_type=quality_check_type,
                        final_status_code=final_status_code_for_vmaf if final_status_code_for_vmaf else None,
                        vmaf_reference_path=vmaf_reference_path,
                        denoised_master_path=denoised_master_path,
                        queue_front=True,
                        reason='auto_vmaf_psnr'
                    )
                    quality_check_queued = bool(added_to_quality_queue)
                    if not quality_check_queued:
                        with self.task_list_lock:
                            quality_check_queued = any(
                                queued_task.get('video_path') == video_path
                                and queued_task.get('type') == 'vmaf'
                                for queued_task in self.pending_svt_tasks
                            )
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(
                                f"[INFO] Post-encode VMAF task prioritized at queue front: "
                                f"{Path(video_path).name} | quality_check={quality_check_type}\n"
                            )
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                else:
                    if original_completed_status and is_status_completed(original_completed_status):
                        self.encoding_queue.put(("schedule_hide_completed", item_id))
            else:
                if original_completed_status and is_status_completed(original_completed_status):
                    self.encoding_queue.put(("schedule_hide_completed", item_id))
        else:
            if is_status_completed(status_text):
                self.encoding_queue.put(("schedule_hide_completed", item_id))
        return quality_check_queued
          
