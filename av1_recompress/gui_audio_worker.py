from .gui_imports import *
from .gui_shared import *

class AudioWorkerMixin:
    def audio_edit_worker(self):
        """Background worker for audio manipulation tasks.
        
        Handles removing audio tracks and converting to stereo.
        """
    
        set_low_priority()
        task_in_progress = False
        while True:
            if STOP_EVENT.is_set():
                self._reset_audio_tasks_pending()
                break
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            _, _, graceful_stop = self.get_encoding_state()
            if graceful_stop and not task_in_progress:
                break
            try:
                task = AUDIO_EDIT_QUEUE.get(timeout=1)
            except queue.Empty:
                if STOP_EVENT.is_set():
                    break
                _, _, graceful_stop = self.get_encoding_state()
                if graceful_stop:
                    self._reset_audio_tasks_pending()
                    break
                continue
    
            item_id = task.get('item_id')
            original = task.get('original') or self.audio_edit_task_info.get(item_id)
            task_in_progress = True
            try:
                self._process_audio_edit_task(task)
            except EncodingStopped:
                self._restore_audio_task_state(item_id, original)
                self.audio_edit_task_info.pop(item_id, None)
                AUDIO_EDIT_QUEUE.task_done()
                task_in_progress = False
                break
            except Exception as e:
                with console_redirect(self.svt_logger):
                    print(f"\n[ERROR] Hangsáv eltávolítás hiba: {e}\n")
                if original:
                    failure_status = t('status_audio_edit_failed')
                    self.encoding_queue.put(("update", item_id, failure_status, original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                self.audio_edit_task_info.pop(item_id, None)
                AUDIO_EDIT_QUEUE.task_done()
                task_in_progress = False
                continue
    
            self.audio_edit_task_info.pop(item_id, None)
            AUDIO_EDIT_QUEUE.task_done()
            task_in_progress = False
    
        # FIX #3: Guard against root destruction during app shutdown
        if hasattr(self, 'root'):
            try:
                if not is_app_closing() and self.root.winfo_exists():
                    self.root.after(0, self._on_audio_edit_worker_finished)
            except tk.TclError:
                pass

    def _process_audio_edit_task(self, task):
        """Audio feladat feldolgozása (eltávolítás vagy konverzió)."""
        action = (task.get('action') or 'remove').lower()
        if action == 'convert':
            self._process_audio_conversion_task(task)
        else:
            self._process_audio_removal_task(task)

    def _process_audio_removal_task(self, task):
        """Egyetlen hangsáv eltávolításának végrehajtása."""
        video_path = task['video_path']
        output_file = task['output_file']
        item_id = task['item_id']
        track_info = task['track_info']
        original = task['original']
    
        # KRITIKUS: Állítsuk be a current_video_path-ot a logger-ben, hogy a naplóbejegyzések bekerüljenek a video_logs-ba
        if hasattr(self.svt_logger, 'set_current_video_path'):
            self.svt_logger.set_current_video_path(video_path)
    
        self.encoding_queue.put(("update", item_id, t('status_audio_editing'), original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, "audio_edit"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
    
        try:
            with CPU_WORKER_LOCK:
                with console_redirect(self.svt_logger):
                    print(f"\n{'='*80}\nHANGSÁV ELTÁVOLÍTÁS: {output_file.name}\n{track_info.get('description', '')}\n{'='*80}\n")
                remove_audio_track_from_file(output_file, track_info['ffmpeg_audio_index'], logger=self.svt_logger, stop_event=STOP_EVENT)
        finally:
            # Clear current video path in logger
            if hasattr(self.svt_logger, 'set_current_video_path'):
                self.svt_logger.set_current_video_path(None)
    
        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB" if orig_size_mb else original['orig_size']
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
        change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%" if orig_size_mb else original['change']
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
        self.encoding_queue.put(("update", item_id, t('status_audio_edit_done'), original['cq'], original['vmaf'], original['psnr'], "100%", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "completed"))
        self.encoding_queue.put(("update_summary",))
        
        # Adatbázis frissítése hangsáv módosítás befejezése után
        if video_path:
            def update_db_after_audio_edit():
                try:
                    self.update_single_video_in_db(
                        video_path, item_id, t('status_audio_edit_done'), 
                        original['cq'], original['vmaf'], original['psnr'], 
                        orig_size_str, new_size_mb, change_percent, completed_date
                    )
                except Exception as e:
                    # Csendes hiba - ne zavarjuk meg az audio edit folyamatot
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[WARN] [audio_edit] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except (OSError, IOError, ValueError, AttributeError):
                            # File or calculation error - non-critical
                            pass
            
            self._start_db_thread(update_db_after_audio_edit, name="AudioEditDB", daemon=True)
    
        with console_redirect(self.svt_logger):
            print(f"\n[OK] Hangsáv eltávolítva: {output_file.name} -> {track_info.get('description', '')}\n")

    def _process_audio_conversion_task(self, task):
        """Új 2.0 hangsáv létrehozása egy kiválasztott térhatású sávból."""
        video_path = task['video_path']
        output_file = task['output_file']
        item_id = task['item_id']
        track_info = task['track_info']
        original = task['original']
        method_key = task.get('conversion_method', 'fast')
        if not output_file.exists():
            raise FileNotFoundError("Kimeneti fájl nem található a hangsáv konverzióhoz.")
    
        # KRITIKUS: Állítsuk be a current_video_path-ot a logger-ben, hogy a naplóbejegyzések bekerüljenek a video_logs-ba
        if hasattr(self.svt_logger, 'set_current_video_path'):
            self.svt_logger.set_current_video_path(video_path)
    
        self.encoding_queue.put(("update", item_id, t('status_audio_editing'), original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, "audio_edit"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
    
        try:
            audio_index = track_info['ffmpeg_audio_index']
            language_norm = track_info.get('language_normalized')
            with console_redirect(self.svt_logger):
                with CPU_WORKER_LOCK:
                    convert_audio_track_to_stereo(output_file, audio_index, method=method_key, language_code=language_norm, logger=self.svt_logger, stop_event=STOP_EVENT)
        finally:
            # Clear current video path in logger
            if hasattr(self.svt_logger, 'set_current_video_path'):
                self.svt_logger.set_current_video_path(None)
    
        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB" if orig_size_mb else original['orig_size']
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB" if new_size_mb else original['new_size']
        change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%" if orig_size_mb else original['change']
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
        self.encoding_queue.put(("update", item_id, t('status_audio_edit_done'), original['cq'], original['vmaf'], original['psnr'], "100%", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "completed"))
        self.encoding_queue.put(("update_summary",))
        
        # Adatbázis frissítése hangsáv konverzió befejezése után
        if video_path:
            def update_db_after_audio_conversion():
                try:
                    self.update_single_video_in_db(
                        video_path, item_id, t('status_audio_edit_done'), 
                        original['cq'], original['vmaf'], original['psnr'], 
                        orig_size_str, new_size_mb, change_percent, completed_date
                    )
                except Exception as e:
                    # Csendes hiba - ne zavarjuk meg az audio conversion folyamatot
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"[WARN] [audio_conversion] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except (OSError, IOError, ValueError, AttributeError):
                            # File or calculation error - non-critical
                            pass
            
            self._start_db_thread(update_db_after_audio_conversion, name="AudioConversionDB", daemon=True)
    
        method_label = self._get_audio_method_label(method_key)
        with console_redirect(self.svt_logger):
            print(f"\n[OK] Új 2.0 hangsáv hozzáadva ({method_label}): {output_file.name} - {track_info.get('description', '')}\n")

    def _restore_audio_task_state(self, item_id, original):
        """Visszaállítja az eredeti státuszt (stop vagy hiba esetén)."""
        if not original:
            return
        status = original.get('status', t('status_ready'))
        tag = original.get('tag', 'pending')
        progress = original.get('progress', "-")
        self.encoding_queue.put(("update", item_id, status, original['cq'], original['vmaf'], original['psnr'], progress, original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, tag))

    def _reset_audio_tasks_pending(self):
        """Az audio queue-ban váró feladatok státuszát visszaállítja (stop esetén)."""
        while True:
            try:
                task = AUDIO_EDIT_QUEUE.get_nowait()
            except queue.Empty:
                break
            item_id = task.get('item_id')
            original = task.get('original')
            self._restore_audio_task_state(item_id, original)
            self.audio_edit_task_info.pop(item_id, None)
            AUDIO_EDIT_QUEUE.task_done()

    def _on_audio_edit_worker_finished(self):
        """Audio worker leállásakor visszaállítja a gombokat, ha csak az futott."""
        self.audio_edit_thread = None
        if self.audio_edit_only_mode:
            self.audio_edit_only_mode = False
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            self.set_encoding_state(is_encoding=False)
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.DISABLED)
            self.load_videos_btn.config(state=tk.NORMAL)
            self.status_label.config(text=t('status_ready'))
        self.update_start_button_state()
        self._reset_encoding_ui_if_idle()

    def confirm_audio_track_removal(self, video_path, output_file, item_id, track_info):
        """Hangsáv eltávolításának megerősítése és ütemezése."""
        if not output_file or not output_file.exists():
            messagebox.showerror(t('msg_error'), t('msg_output_not_found'))
            return
    
        description = track_info.get('description', 'Audio')
        confirmation = messagebox.askyesno(
            t('menu_audio_tracks'),
            f"{t('menu_audio_remove_confirm')}\n\n{description}"
        )
        if not confirmation:
            return
    
        self.schedule_audio_track_removal(video_path, output_file, item_id, track_info)

    def _capture_audio_task_state(self, item_id):
        """Visszaadja a kiválasztott sor eredeti állapotát a hangsáv műveletekhez."""
        current_values = self.get_tree_values(item_id, min_length=len(self.COLUMN_INDEX))
        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
        cq_str = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
        psnr_str = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
        progress_str = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else "-"
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        new_size_str = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
        change_str = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        return {
            'status': status,
            'cq': cq_str,
            'vmaf': vmaf_str,
            'psnr': psnr_str,
            'progress': progress_str,
            'orig_size': orig_size_str,
            'new_size': new_size_str,
            'change': change_str,
            'completed_date': completed_date,
            'tag': 'completed' if is_status_completed(status) else 'pending'
        }

    def _get_audio_method_label(self, method_key):
        method = (method_key or 'fast').lower()
        return t('audio_compression_dialogue') if method == 'dialogue' else t('audio_compression_fast')

    def schedule_audio_track_removal(self, video_path, output_file, item_id, track_info):
        """Feladat ütemezése a CPU workerre egy hangsáv eltávolításához."""
        video_path = Path(video_path)
        output_file = Path(output_file)
        original_info = self._capture_audio_task_state(item_id)
        cq_str = original_info['cq']
        vmaf_str = original_info['vmaf']
        psnr_str = original_info['psnr']
        orig_size_str = original_info['orig_size']
        new_size_str = original_info['new_size']
        change_str = original_info['change']
        completed_date = original_info['completed_date']
    
        self.audio_edit_task_info[item_id] = original_info
    
        task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'track_info': track_info,
            'original': original_info,
            'action': 'remove'
        }
        AUDIO_EDIT_QUEUE.put(task)
    
        self.encoding_queue.put(("update", item_id, t('status_audio_edit_queue'), cq_str, vmaf_str, psnr_str, "-", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
    
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        is_enc, _, _ = self.get_encoding_state()
        if not is_enc:
            self.set_encoding_state(is_encoding=True)
            self.audio_edit_only_mode = True
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
            self.root.after(100, self.check_encoding_queue)

        if not self.audio_edit_thread or not self.audio_edit_thread.is_alive():
            self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
            self.audio_edit_thread.start()

        self.update_start_button_state()

    def schedule_audio_track_conversion(self, video_path, output_file, item_id, track_info, method_key):
        """Feladat ütemezése térhatású hangsáv 2.0 konverziójára."""
        video_path = Path(video_path)
        output_file = Path(output_file)
        original_info = self._capture_audio_task_state(item_id)
        cq_str = original_info['cq']
        vmaf_str = original_info['vmaf']
        psnr_str = original_info['psnr']
        orig_size_str = original_info['orig_size']
        new_size_str = original_info['new_size']
        change_str = original_info['change']
        completed_date = original_info['completed_date']
    
        self.audio_edit_task_info[item_id] = original_info
    
        task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'track_info': track_info,
            'original': original_info,
            'action': 'convert',
            'conversion_method': method_key
        }
        AUDIO_EDIT_QUEUE.put(task)
    
        self.encoding_queue.put(("update", item_id, t('status_audio_edit_queue'), cq_str, vmaf_str, psnr_str, "-", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
    
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        is_enc, _, _ = self.get_encoding_state()
        if not is_enc:
            self.set_encoding_state(is_encoding=True)
            self.audio_edit_only_mode = True
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
            self.root.after(100, self.check_encoding_queue)

        if not self.audio_edit_thread or not self.audio_edit_thread.is_alive():
            self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
            self.audio_edit_thread.start()

        self.update_start_button_state()
