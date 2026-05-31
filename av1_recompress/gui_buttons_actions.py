from .gui_imports import *
from .gui_shared import *

class ButtonsActionsMixin:
    def browse_source(self):
        """Open directory browser for source folder selection."""
        folder = filedialog.askdirectory(title=t('dialog_select_source_folder'))
        if folder:
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, folder)

    def browse_dest(self):
        """Open directory browser for destination folder selection."""
        folder = filedialog.askdirectory(title=t('dialog_select_dest_folder'))
        if folder:
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, folder)

    def browse_ffmpeg(self):
        """Open file browser for FFmpeg executable selection."""
        file_path = filedialog.askopenfilename(
            title=t('ffmpeg_path'),
            filetypes=[("FFmpeg", "ffmpeg.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.ffmpeg_path.set(file_path)

    def browse_virtualdub(self):
        """Open file browser for VirtualDub2 executable selection."""
        file_path = filedialog.askopenfilename(
            title=t('virtualdub_path'),
            filetypes=[("VirtualDub2", "vdub64.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.virtualdub_path.set(file_path)

    def browse_abav1(self):
        """Open file browser for ab-av1 executable selection."""
        file_path = filedialog.askopenfilename(
            title=t('abav1_path'),
            filetypes=[("ab-av1", "ab-av1.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.abav1_path.set(file_path)

    def browse_hybrid(self):
        """Open directory browser for Hybrid encoder folder selection."""
        from .gui_db_and_state_load import save_program_path
        from .core_paths_tools_logging import _get_preferred_search_paths
        
        # Determine initial directory
        initial_dir = self.hybrid_path.get()
        if not initial_dir:
            # Try preferred locations
            preferred_paths = _get_preferred_search_paths('hybrid')
            for path in preferred_paths:
                if path.exists():
                    initial_dir = str(path)
                    break
            if not initial_dir:
                initial_dir = str(Path.home())
        
        folder = filedialog.askdirectory(
            title=t('select_hybrid_folder'),
            initialdir=initial_dir
        )
        if folder:
            self.hybrid_path.set(folder)
            # Save to save.db
            save_program_path('hybrid', folder)
            self._save_settings_debounced()

    def toggle_debug_mode(self):
        """Toggle the global debug mode based on the checkbox state."""
        DEBUG_MODE.set(self.debug_mode.get())

    def toggle_hide_completed(self):
        """Hide/Show completed items."""
        hide = self.hide_completed.get()
        
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"DEBUG toggle_hide_completed: hide={hide}, total_videos={len(self.video_items)}\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError, ValueError):
                # Log writer closed/unavailable - non-critical
                pass
        
        # Item ID -> video path for quick lookup
        id_to_video_path = {item_id: video_path for video_path, item_id in self.video_items.items()}
    
        hidden_count = 0
        shown_count = 0
        completed_count = 0
        
        # Iterate through all videos
        for video_path, item_id in list(self.video_items.items()):
            # Handle only existing item_ids
            try:
                current_values = self.tree.item(item_id, 'values')
                status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                
                # If status is completed
                if is_status_completed(status):
                    completed_count += 1
                    if hide:
                        # Hide - only if not already hidden
                        if item_id not in self.hidden_items:
                            try:
                                # Check if visible
                                parent = self.tree.parent(item_id)
                                if parent == "" and item_id in self.tree.get_children():
                                    self.tree.detach(item_id)
                                    self.hidden_items.add(item_id)
                                    hidden_count += 1
                            except (tk.TclError, KeyError, AttributeError) as e:
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"DEBUG toggle_hide_completed: Error hiding {item_id}: {e}\n")
                                        LOG_WRITER.flush()
                                    except (OSError, IOError, AttributeError, ValueError):
                                        # Log writer closed/unavailable - non-critical
                                        pass
                    else:
                        # Show
                        if item_id in self.hidden_items:
                            try:
                                children = list(self.tree.get_children(""))
                                video_order = getattr(self, 'video_order', {})
                                target_order = video_order.get(video_path, float('inf'))
                                insert_index = len(children)

                                for idx, child_id in enumerate(children):
                                    child_video_path = id_to_video_path.get(child_id)
                                    if not child_video_path:
                                        continue
                                    child_order = video_order.get(child_video_path, float('inf'))
                                    if target_order < child_order:
                                        insert_index = idx
                                        break

                                insert_pos = tk.END if insert_index >= len(children) else insert_index

                                reattach = getattr(self.tree, "reattach", None)
                                if callable(reattach):
                                    reattach(item_id, "", insert_pos)
                                else:
                                    self.tree.move(item_id, "", insert_pos)
                                shown_count += 1
                            except (tk.TclError, KeyError, AttributeError, ValueError) as e:
                                # If failed, keep hidden to retry later
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"DEBUG toggle_hide_completed: Error showing {item_id}: {e}\n")
                                        LOG_WRITER.flush()
                                    except (OSError, IOError, AttributeError, ValueError):
                                        # Log writer closed/unavailable - non-critical
                                        pass
                                continue
                            else:
                                self.hidden_items.discard(item_id)
            except (tk.TclError, KeyError, AttributeError) as e:
                # Ha az item_id már nem létezik, kihagyjuk
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"DEBUG toggle_hide_completed: Error accessing item {item_id}: {e}\n")
                        LOG_WRITER.flush()
                    except (OSError, IOError, AttributeError, ValueError):
                        # Log writer closed/unavailable - non-critical
                        pass
                continue
        
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"DEBUG toggle_hide_completed: completed={completed_count}, hidden={hidden_count}, shown={shown_count}, hidden_items_set_size={len(self.hidden_items)}\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError, ValueError):
                # Log writer closed/unavailable - non-critical
                pass
        
        # FIX: Update tree widget after showing
        # This ensures all items are visible, including the first one
        self.tree.update_idletasks()
        
        # Extra safety layer: check if everything is truly visible
        if not hide:
            # If shown, ensure tree sees the items
            self.tree.event_generate("<<TreeviewSelect>>")
            self.tree.update()

    def toggle_resize_slider(self):
        """Show or hide resize slider based on checkbox state."""
        new_value = self.resize_enabled.get()
        old_value = getattr(self, '_last_resize_enabled_value', new_value)
        if not hasattr(self, '_last_resize_enabled_value'):
            self._last_resize_enabled_value = new_value
        
        # Ha kódolás fut és az érték változott
        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.resize_enabled, new_value, old_value):
                self.resize_enabled.set(old_value)
                return
        
        # Változtatás elfogadva
        self._last_resize_enabled_value = new_value
        
        if new_value:
            self.resize_slider.pack(side=tk.LEFT, padx=5)
            self.resize_value_label.pack(side=tk.LEFT, padx=5)
        else:
            self.resize_slider.pack_forget()
            self.resize_value_label.pack_forget()
        self._save_settings_debounced()  # Automatic save with debounce

    def show_debug_dialog(self, current_step, next_step, file_info, continue_event):
        """Debug dialog with countdown timer."""
        dialog = tk.Toplevel(self.root)
        dialog.title(t('debug_dialog_title'))
        dialog.transient(self.root)
        dialog.grab_set()
        
        canvas = tk.Canvas(dialog, width=600, height=400)
        scrollbar = ttk.Scrollbar(dialog, orient=tk.VERTICAL, command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        content_frame = ttk.Frame(scrollable_frame, padding="20")
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(content_frame, text=t('debug_dialog_header'), font=("Arial", 16, "bold")).pack(pady=10)
        
        # Countdown timer label
        countdown_label = ttk.Label(
            content_frame, 
            text=t('debug_dialog_auto_continue').format(seconds=60), 
            font=("Arial", 11, "bold"), 
            foreground="orange"
        )
        countdown_label.pack(pady=(0, 10))
        
        ttk.Label(content_frame, text=t('debug_dialog_current'), font=("Arial", 10, "bold")).pack(anchor=tk.W)
        ttk.Label(content_frame, text=current_step, wraplength=550, foreground="blue").pack(anchor=tk.W, pady=5)
        
        ttk.Label(content_frame, text=t('debug_dialog_next'), font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(10,0))
        ttk.Label(content_frame, text=next_step, wraplength=550, foreground="green").pack(anchor=tk.W, pady=5)
        
        if file_info:
            ttk.Label(content_frame, text=t('debug_dialog_info'), font=("Arial", 9, "italic")).pack(anchor=tk.W, pady=(10,0))
            ttk.Label(content_frame, text=file_info, wraplength=550, foreground="gray").pack(anchor=tk.W, pady=5)
        
        button_frame = ttk.Frame(dialog, padding="10")
        button_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        # State for countdown
        countdown_state = {'remaining': 60, 'timer_id': None}
        
        def on_continue():
            # Cancel the timer if it's running
            if countdown_state['timer_id'] is not None:
                try:
                    dialog.after_cancel(countdown_state['timer_id'])
                except (tk.TclError, RuntimeError):
                    pass
            try:
                dialog.destroy()
            except (tk.TclError, RuntimeError):
                pass
            continue_event.set()
        
        def update_countdown():
            """Update countdown timer every second."""
            try:
                countdown_state['remaining'] -= 1
                remaining = countdown_state['remaining']
                
                if remaining <= 0:
                    # Timeout reached - auto-continue
                    countdown_label.config(
                        text=t('debug_dialog_timeout'),
                        foreground="red"
                    )
                    dialog.update_idletasks()
                    # Schedule auto-continue after brief delay to show the message
                    dialog.after(500, on_continue)
                else:
                    # Update countdown display
                    countdown_label.config(
                        text=t('debug_dialog_auto_continue').format(seconds=remaining),
                        foreground="orange" if remaining > 10 else "red"
                    )
                    # Schedule next update
                    countdown_state['timer_id'] = dialog.after(1000, update_countdown)
            except (tk.TclError, RuntimeError):
                # Dialog was destroyed - stop updating
                pass
        
        ttk.Button(button_frame, text=t('debug_dialog_continue'), command=on_continue, width=20).pack()
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        dialog.update_idletasks()
        required_height = min(scrollable_frame.winfo_reqheight() + 100, 600)
        dialog.geometry(f"620x{required_height}")
        
        x = (dialog.winfo_screenwidth() // 2) - (620 // 2)
        y = (dialog.winfo_screenheight() // 2) - (required_height // 2)
        dialog.geometry(f"+{x}+{y}")
        
        # Start countdown timer
        countdown_state['timer_id'] = dialog.after(1000, update_countdown)

    def schedule_auto_encode(self, video_path, item_id, encoder='auto', prompt=True):
        """Re-encode with NVENC (or automatic decision)."""
        use_nvenc = self.nvenc_enabled.get()
        if encoder == 'nvenc':
            if not self.nvenc_enabled.get():
                if prompt:
                    messagebox.showwarning(t('msg_warning'), t('msg_nvenc_disabled'))
                return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt, reason='auto_reencode')
            use_nvenc = True
        elif encoder == 'svt':
            return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt, reason='auto_reencode')
        elif encoder == 'auto':
            if not self.nvenc_enabled.get():
                return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt, reason='auto_reencode')
    
        # Újrakódolás során mindig .av1.mkv kiterjesztésű output fájlt használunk
        # (nem az eredeti kiterjesztésű másolt fájlt)
        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
        # Frissítjük a mapping-et is
        self.video_to_output[video_path] = output_file

        # GUI FREEZE FIX: A fájltörlés és feliratkeresés átkerült az NVENC workerbe
        # (_reencode_pre_cleanup), mert hálózati megosztáson ezek percekig blokkolhatják a GUI szálat.
        orig_values = self.tree.item(item_id, 'values')
        orig_size_str = orig_values[self.COLUMN_INDEX['orig_size']] if len(orig_values) > self.COLUMN_INDEX['orig_size'] else "-"
        status_text = t('status_nvenc_queue')
        completed_date = ""

        new_values = list(orig_values)
        if len(new_values) < len(self.COLUMN_INDEX):
            new_values.extend([''] * (len(self.COLUMN_INDEX) - len(new_values)))
        new_values[self.COLUMN_INDEX['status']] = status_text
        new_values[self.COLUMN_INDEX['cq']] = "-"
        new_values[self.COLUMN_INDEX['vmaf']] = "-"
        new_values[self.COLUMN_INDEX['psnr']] = "-"
        new_values[self.COLUMN_INDEX['progress']] = "-"
        new_values[self.COLUMN_INDEX['new_size']] = "-"
        new_values[self.COLUMN_INDEX['size_change']] = "-"
        # Keep duration and frames values
        new_values[self.COLUMN_INDEX['completed_date']] = completed_date
        self.tree.item(item_id, values=tuple(new_values))

        self.encoding_queue.put(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))

        self.update_summary_row()

        # LIST-BASED QUEUE: Add auto re-encode task
        # subtitle_files=None jelzi a workernek, hogy a feliratkeresés még nem történt meg
        self.add_to_nvenc_queue(
            video_path=video_path, item_id=item_id, is_manual=True,
            output_file=output_file, subtitle_files=None,
            invalid_subtitles=None, orig_size_str=orig_size_str,
            initial_min_vmaf=self.min_vmaf.get(), vmaf_step=self.vmaf_step.get(),
            max_encoded=self.max_encoded_percent.get(),
            resize_enabled=self.resize_enabled.get(), resize_height=self.resize_height.get(),
            audio_compression_enabled=self.audio_compression_enabled.get(),
            audio_compression_method=self.audio_compression_method.get(),
            reason='auto_reencode',
            needs_pre_cleanup=True
        )
        
        # Beállítjuk az encoding állapotot és indítjuk a worker-eket, ha még nem futnak
        is_enc, _, _ = self.get_encoding_state()
        if not is_enc:
            # ThreadSafeEvent provides built-in atomicity, no external lock needed
            STOP_EVENT.clear()
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            self.set_encoding_state(is_encoding=True, encoding_worker_running=True)
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
        
        # NVENC worker-ek indítása, ha még nem futnak
        if not hasattr(self, 'nvenc_worker_threads') or not self.nvenc_worker_threads or not any(t.is_alive() for t in self.nvenc_worker_threads):
            nvenc_worker_count = self.get_configured_nvenc_workers()
            self.nvenc_worker_threads = []
            for worker_idx in range(nvenc_worker_count):
                nvenc_thread = threading.Thread(target=self.nvenc_worker, args=(worker_idx,), daemon=True)
                nvenc_thread.worker_index = worker_idx
                nvenc_thread.start()
                self.nvenc_worker_threads.append(nvenc_thread)
        
        # Encoding worker indítása, ha még nem fut
        if not hasattr(self, 'encoding_worker_thread') or not self.encoding_worker_thread or not self.encoding_worker_thread.is_alive():
            if hasattr(self, '_refresh_encoding_worker_snapshot'):
                self._refresh_encoding_worker_snapshot()
            self.encoding_worker_thread = threading.Thread(target=self.encoding_worker, daemon=True)
            self.encoding_worker_thread.start()
            # THREAD-SAFETY FIX: Use helper method for lock-protected state access
            self.set_encoding_state(encoding_worker_running=True)
        
        # Queue ellenőrzés indítása
        self.root.after(100, self.check_encoding_queue)
        
        if prompt:
            messagebox.showinfo(t('msg_started'), f"{t('msg_reencode_added').format(encoder='NVENC')}\n{video_path.name}")
        else:
            self.log_status(f"[OK] NVENC queued: {video_path.name}")
        self.update_start_button_state()
        return True

    def bulk_schedule_auto(self, item_ids, encoder='auto'):
        """Schedule multiple items for auto-encoding.
        
        Args:
            item_ids: List of tree item IDs.
            encoder: 'auto', 'nvenc', or 'svt'.
        """
        processed = 0
        for item_id in item_ids:
            video_path = self._get_video_path_by_item(item_id)
            if not video_path:
                continue
            if encoder == 'svt':
                if self.reencode_with_svt_av1(video_path, item_id, prompt=False, reason='auto_reencode'):
                    processed += 1
            else:
                if self.schedule_auto_encode(video_path, item_id, encoder=encoder, prompt=False):
                    processed += 1
        if processed:
            self.log_status(f"[OK] {processed} videos queued ({encoder}).")
            self.update_start_button_state()

    def bulk_manual_reencode(self, item_ids, target_cq, encoder_type, quality_check, cq_range):
        """Schedule multiple completed items for manual quality re-encoding.
        
        Args:
            item_ids: List of tree item IDs.
            target_cq: Target CQ/CRF value.
            encoder_type: 'NVENC' or 'SVT-AV1'.
            quality_check: 'vmaf', 'psnr', 'both', 'none'.
            cq_range: CQ range string (e.g., "10-24").
        """
        processed = 0
        encoder_type_str = encoder_type if encoder_type else "SVT-AV1"
        quality_check_display = {
            'vmaf': t('manual_quality_vmaf'),
            'psnr': t('manual_quality_psnr'),
            'both': t('manual_quality_both'),
            'none': t('manual_quality_none')
        }.get(quality_check, quality_check)
        
        # Confirmation for bulk operation
        result = messagebox.askyesno(
            t('context_multi_manual_reencode_menu'),
            f"{t('msg_reencode_confirm_bulk').format(count=len(item_ids))}\n\n"
            f"{t('label_encoder')}: {encoder_type_str}\n"
            f"CQ/CRF: {target_cq} ({cq_range})\n"
            f"{t('label_quality_check')}: {quality_check_display}\n\n"
            f"{t('msg_reencode_confirm_bulk_note')}"
        )
        
        if not result:
            return
        
        for item_id in item_ids:
            video_path = self._get_video_path_by_item(item_id)
            if not video_path:
                continue
            # Call reencode_with_manual_config for each video (skip confirmation - already confirmed once)
            if self.reencode_with_manual_config(video_path, item_id, target_cq, encoder_type_str, quality_check, cq_range, skip_confirmation=True):
                processed += 1
        
        if processed:
            self.log_status(f"[OK] {processed} videos queued for manual re-encoding (CQ={target_cq}, {encoder_type_str}).")
            self.update_start_button_state()

        # Update summary row to reflect status changes
        self.update_summary_row()
