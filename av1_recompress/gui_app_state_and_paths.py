from .gui_imports import *
from .gui_shared import *

class AppStateAndPathsMixin:
    def check_and_offer_state_load(self):
        """Check for saved database state and offer to offer to load it.
        
        If a saved state exists in the database, prompts the user to load it.
        """
        print("DEBUG: check_and_offer_state_load called")
    
        # Check if tree and source_entry are initialized
        if not hasattr(self, 'tree') or self.tree is None or not hasattr(self, 'source_entry') or self.source_entry is None:
            print("DEBUG: components not initialized, retrying in 100ms")
            self.root.after(100, self.check_and_offer_state_load)
            return
            
        print("DEBUG: Initialization check passed, calling load_state_from_db()")
        try:
            saved_state = self.load_state_from_db()
        except Exception as e:
            print(f"DEBUG: Exception in load_state_from_db: {e}")
            saved_state = None

        if saved_state:
            print(f"DEBUG: saved_state loaded. Keys: {list(saved_state.keys())}")
            saved_source = saved_state.get('source_path')
            saved_dest = saved_state.get('dest_path')
            print(f"DEBUG: saved_source: '{saved_source}' (type: {type(saved_source)})")
            
            if saved_source or (saved_state.get('videos') and len(saved_state['videos']) > 0):
                print(f"DEBUG: State found (source={bool(saved_source)}, videos={len(saved_state.get('videos', []))}), showing message box")
                
                # Autotest mode: automatic load without asking
                if self.autotest_mode:
                    if LOG_WRITER:
                        LOG_WRITER.write("🤖 AUTOTEST: Automatic state load from save.db without prompt\n")
                        LOG_WRITER.flush()
                    result = True  # Automatically confirm
                else:
                    # Normal mode: ask user
                    result = messagebox.askyesno(
                        t('msg_state_load_title'),
                        t('msg_load_state_content').format(saved_source, saved_dest or 'None')
                    )
                print(f"DEBUG: message box result: {result}")
                
                if result:
                    # Setting source folder
                    if saved_source:
                        self.source_entry.delete(0, tk.END)
                        self.source_entry.insert(0, saved_source)
                    elif saved_state.get('videos') and len(saved_state['videos']) > 0:
                        # Attempt to derive source from first video if missing
                        try:
                            from pathlib import Path # Import Path here to avoid circular dependency or global import if not needed elsewhere
                            first_video_path = Path(saved_state['videos'][0]['video_path'])
                            derived_source = str(first_video_path.parent)
                            print(f"DEBUG: Derived source path from video: {derived_source}")
                            self.source_entry.delete(0, tk.END)
                            self.source_entry.insert(0, derived_source)
                        except Exception as e:
                            print(f"DEBUG: Failed to derive source path: {e}")
                    
                    # Setting destination folder
                    if saved_dest:
                        self.dest_entry.delete(0, tk.END)
                        self.dest_entry.insert(0, saved_dest)
                    
                    # Restoring VMAF settings
                    if 'min_vmaf' in saved_state:
                        self.update_vmaf_label(saved_state['min_vmaf'])
                    if 'vmaf_step' in saved_state:
                        self.update_vmaf_step_label(saved_state['vmaf_step'])
                    if 'max_encoded_percent' in saved_state:
                        self.update_max_encoded_label(saved_state['max_encoded_percent'])
                    if 'max_encoded_mode' in saved_state:
                        mode_val = saved_state['max_encoded_mode']
                        if mode_val in ('full', 'video'):
                            self.max_encoded_mode.set(mode_val)
                            # Update combobox display if it exists
                            if hasattr(self, 'max_encoded_mode_combo') and hasattr(self, '_max_encoded_mode_reverse_map'):
                                display_text = self._max_encoded_mode_reverse_map.get(mode_val, t('max_encoded_mode_full'))
                                self.max_encoded_mode_combo.set(display_text)
                    if 'resize_enabled' in saved_state:
                        self.resize_enabled.set(saved_state['resize_enabled'])
                        self.toggle_resize_slider()
                    if 'resize_height' in saved_state:
                        self.update_resize_label(saved_state['resize_height'])
                    if 'deband_enabled' in saved_state:
                        self.deband_enabled.set(saved_state['deband_enabled'])
                        self.current_deband_enabled = bool(saved_state['deband_enabled'])
                    if 'force_8bit_denoised_master' in saved_state:
                        self.force_8bit_denoised_master.set(saved_state['force_8bit_denoised_master'])
                        self.current_force_8bit_denoised_master = bool(saved_state['force_8bit_denoised_master'])
                    if 'auto_vmaf_psnr' in saved_state:
                        self.auto_vmaf_psnr.set(saved_state['auto_vmaf_psnr'])
                    if 'nvenc_worker_count' in saved_state:
                        self.nvenc_worker_count.set(int(saved_state['nvenc_worker_count']))
                        self.update_nvenc_workers_label(saved_state['nvenc_worker_count'])
                    if 'svt_worker_count' in saved_state:
                        self.svt_worker_count.set(int(saved_state['svt_worker_count']))
                        if hasattr(self, 'update_svt_workers_label'):
                            self.update_svt_workers_label(saved_state['svt_worker_count'])
                    if 'svt_preset' in saved_state:
                        svt_preset_val = int(saved_state['svt_preset']) if saved_state.get('svt_preset') is not None else 2
                        self.svt_preset.set(svt_preset_val)
                        self.svt_preset_value_label.config(text=str(svt_preset_val))
                    if 'crf_increment' in saved_state:
                        crf_increment_val = int(saved_state['crf_increment']) if saved_state['crf_increment'] else 1
                        self.crf_increment.set(crf_increment_val)
                        if hasattr(self, 'crf_increment_value_label'):
                            self.crf_increment_value_label.config(text=str(crf_increment_val))
                    if 'nvenc_enabled' in saved_state:
                        self.nvenc_enabled.set(saved_state['nvenc_enabled'])
                        if hasattr(self, 'nvenc_checkbutton'):
                            self._update_nvenc_checkbox_text()
                        if hasattr(self, 'refresh_nvenc_console_tabs'):
                            self.refresh_nvenc_console_tabs(self.nvenc_worker_count.get())
                    if 'vdub_validation_disabled' in saved_state:
                        self.vdub_validation_disabled.set(saved_state['vdub_validation_disabled'])
                    if 'max_cq_limit' in saved_state:
                        self.max_cq_limit.set(saved_state['max_cq_limit'])
                    if 'hybrid_path' in saved_state and saved_state['hybrid_path']:
                        self.hybrid_path.set(saved_state['hybrid_path'])

                    # Loading videos with state
                    self.load_videos()
        else:
            print("DEBUG: saved_state is empty or None")

    def apply_tool_paths_from_gui(self):
        """Apply external tool paths from GUI input fields to the core configuration.
        
        Updates the global configuration for FFmpeg, ab-av1, and VirtualDub2 paths
        based on the current values in the GUI entry fields.
        """
        apply_external_tool_paths(
            (self.ffmpeg_path.get().strip() or None),
            (self.abav1_path.get().strip() or None),
            (self.virtualdub_path.get().strip() or None)
        )

    def _on_tool_path_change(self, *args):
        """Handle changes in tool path variables.
        
        Args:
            *args: Variable length argument list (unused, required by trace callback).
        """
        self.apply_tool_paths_from_gui()

