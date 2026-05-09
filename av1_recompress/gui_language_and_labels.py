from .gui_imports import *
from .gui_shared import *
from . import i18n

class LanguageAndLabelsMixin:
    def change_language(self, event=None):
        """Switch the application language.
        
        Updates all GUI elements (labels, buttons, headers) to the selected language.
        
        Args:
            event: Optional event argument (unused, for binding).
        """
        # Look up language code using i18n module
        selected_display = self.language_var.get()
        if selected_display == t('hungarian'):
            new_lang = 'hu'
        elif selected_display == t('english'):
            new_lang = 'en'
        else:
            return
        
        if new_lang != i18n.CURRENT_LANGUAGE:
            i18n.CURRENT_LANGUAGE = new_lang
            # Update GUI
            self.root.title(t('app_title'))
            
            # Update top labels and buttons
            self.language_label.config(text=t('language'), width=18, anchor=tk.W)
            # Update Hybrid label
            if hasattr(self, 'hybrid_label'):
                self.hybrid_label.config(text=t('hybrid_path'), width=20, anchor=tk.W)
            if hasattr(self, 'hybrid_browse_btn'):
                self.hybrid_browse_btn.config(text=t('browse'))

            self.ffmpeg_label.config(text=t('ffmpeg_path'), width=18, anchor=tk.W)
            self.vdub_label.config(text=t('virtualdub_path'), width=18, anchor=tk.W)
            self.abav1_label.config(text=t('abav1_path'), width=18, anchor=tk.W)
            self.ffmpeg_browse_btn.config(text=t('browse'))
            self.vdub_browse_btn.config(text=t('browse'))
            self.abav1_browse_btn.config(text=t('browse'))
            self.source_label.config(text=t('source'), width=12, anchor=tk.W)
            self.dest_label.config(text=t('dest'), width=12, anchor=tk.W)
            self.source_browse_btn.config(text=t('browse'))
            self.dest_browse_btn.config(text=t('browse'))

            # Update HTTP Server GUI
            if hasattr(self, 'http_enabled_checkbutton'):
                self.http_enabled_checkbutton.config(text=t('http_server_enabled'))
            if hasattr(self, 'http_port_label'):
                self.http_port_label.config(text=t('http_port'))
            
            # Maintain field widths
            self.source_entry.config(width=50)
            self.dest_entry.config(width=50)
            self.debug_checkbutton.config(text=t('debug_mode'))
            if hasattr(self, 'auto_vmaf_psnr_checkbutton'):
                self.auto_vmaf_psnr_checkbutton.config(text=t('auto_vmaf_psnr'))
            self.load_videos_btn.config(text=t('load_videos'))
            
            # Update right side labels (fixed width - all labels 20 chars wide so sliders start from same position)
            self.min_vmaf_label.config(text=t('min_vmaf'), width=20, anchor=tk.W)
            self.vmaf_fallback_label.config(text=t('vmaf_fallback'), width=20, anchor=tk.W)
            self.max_encoded_label.config(text=t('max_encoded'), width=20, anchor=tk.W)
            self.resize_checkbox.config(text=t('resize_height'))
            if hasattr(self, 'deband_checkbutton'):
                self.deband_checkbutton.config(text=t('deband_enabled'))
            if hasattr(self, 'denoised_master_8bit_checkbutton'):
                self.denoised_master_8bit_checkbutton.config(text=t('denoised_master_8bit_test'))
            self.skip_av1_checkbutton.config(text=t('skip_av1'))
            if hasattr(self, 'nvenc_workers_label'):
                self.nvenc_workers_label.config(text=t('nvenc_workers'), width=20, anchor=tk.W)
            if hasattr(self, 'crf_increment_label'):
                self.crf_increment_label.config(text=t('crf_increment'), width=20, anchor=tk.W)
            
            # Update slider VALUE labels (decimal separator: HU = comma, EN = point)
            if hasattr(self, 'vmaf_value_label') and hasattr(self, 'min_vmaf'):
                self.vmaf_value_label.config(text=format_localized_number(self.min_vmaf.get(), decimals=1))
            if hasattr(self, 'vmaf_step_value_label') and hasattr(self, 'vmaf_step'):
                self.vmaf_step_value_label.config(text=format_localized_number(self.vmaf_step.get(), decimals=2))
            
            # Update audio dynamic compression
            if hasattr(self, 'audio_compression_checkbutton'):
                self.audio_compression_checkbutton.config(text=t('audio_compression'))
                
            # Update max encoded mode combobox
            if hasattr(self, 'max_encoded_mode_combo'):
                # Update map and reverse map for new language
                self._max_encoded_mode_map = {
                    t('max_encoded_mode_full'): 'full',
                    t('max_encoded_mode_video'): 'video'
                }
                self._max_encoded_mode_reverse_map = {v: k for k, v in self._max_encoded_mode_map.items()}
                
                # Update combobox values
                self.max_encoded_mode_combo['values'] = [t('max_encoded_mode_full'), t('max_encoded_mode_video')]
                
                # Update current displayed value
                current_mode = self.max_encoded_mode.get()  # 'full' or 'video'
                display_text = self._max_encoded_mode_reverse_map.get(current_mode, t('max_encoded_mode_full'))
                self.max_encoded_mode_combo.set(display_text)

            if hasattr(self, 'audio_compression_combo'):
                self.audio_compression_combo['values'] = [t('audio_compression_fast'), t('audio_compression_dialogue')]
                # Update current value (variable value is 'fast' or 'dialogue')
                current_value = self.audio_compression_method.get()
                if current_value == 'fast':
                    self.audio_compression_combo.set(t('audio_compression_fast'))
                elif current_value == 'dialogue':
                    self.audio_compression_combo.set(t('audio_compression_dialogue'))
                else:
                    # If unknown value, default to 'fast'
                    self.audio_compression_method.set('fast')
                    self.audio_compression_combo.set(t('audio_compression_fast'))
            
            # Update NVENC checkbox
            nvenc_text = t('nvenc_enabled')
            if hasattr(self, 'detected_gpu_name') and self.detected_gpu_name:
                nvenc_text += f" ({self.detected_gpu_name})"
            if hasattr(self, 'nvenc_checkbutton'):
                self.nvenc_checkbutton.config(text=nvenc_text)
            
            # Update Notebook labels
            if hasattr(self, 'videos_tab'):
                try:
                    self.notebook.tab(self.videos_tab, text=t('videos_tab'))
                except tk.TclError:
                    pass
            if hasattr(self, 'svt_tab'):
                try:
                    self.notebook.tab(self.svt_tab, text=t('svt_console'))
                except tk.TclError:
                    pass
            self._refresh_nvenc_console_tab_titles()
            
            # Update table headers
            self.tree.heading("#0", text=t('column_order'))
            self.tree.heading("denoise", text=t('column_denoise'))
            self.tree.heading("hard_rotate", text=t('column_hard_rotate'))
            self.tree.heading("video_name", text=t('column_video'))
            self.tree.heading("status", text=t('column_status'))
            self.tree.heading("cq", text=t('column_cq'))
            self.tree.heading("vmaf", text=t('column_vmaf'))
            self.tree.heading("psnr", text=t('column_psnr'))
            self.tree.heading("progress", text=t('column_progress'))
            self.tree.heading("orig_size", text=t('column_orig_size'))
            self.tree.heading("new_size", text=t('column_new_size'))
            self.tree.heading("size_change", text=t('column_size_change'))
            self.tree.heading("duration", text=t('column_duration'))
            self.tree.heading("frames", text=t('column_frames'))
            self.tree.heading("completed_date", text=t('column_completed'))
            
            # Update bottom buttons
            self.start_button.config(text=t('btn_start'))
            self.immediate_stop_button.config(text=t('btn_immediate_stop'))
            self.clear_table_btn.config(text=t('btn_clear_table'))
            self.hide_completed_checkbutton.config(text=t('btn_hide_completed'))
            
            # Update status label
            self.status_label.config(text=t('status_ready'))
            
            # Update table status messages
            for video_path, item_id in self.video_items.items():
                current_values = list(self.tree.item(item_id, 'values'))
                if len(current_values) > self.COLUMN_INDEX['status']:
                    current_status = current_values[self.COLUMN_INDEX['status']]
                    # Handle CRF search statuses with VMAF value
                    import re
                    crf_match = re.search(r'(NVENC|SVT-AV1)\s+CRF\s+(?:keresés|search)\s*\(VMAF(?:\s+fallback)?:\s*([\d.]+)\)', current_status)
                    if crf_match:
                        encoder = crf_match.group(1)
                        vmaf_value = crf_match.group(2)
                        is_fallback = 'fallback' in crf_match.group(0)
                        if encoder == 'NVENC':
                            base_status = t('status_nvenc_crf_search')
                        else:
                            base_status = t('status_svt_crf_search')
                        # Remove "..." suffix if present
                        base_status = base_status.rstrip('...')
                        if is_fallback:
                            new_status = f"{base_status} (VMAF fallback: {vmaf_value})..."
                        else:
                            new_status = f"{base_status} (VMAF: {vmaf_value})..."
                        current_values[self.COLUMN_INDEX['status']] = new_status
                        self.tree.item(item_id, values=tuple(current_values))
                        continue
                    
                    # Normalize and re-translate status code
                    status_code = normalize_status_to_code(current_status)
                    if status_code:
                        # If status code exists, translate to new language
                        new_status = status_code_to_localized(status_code)
                        current_values[self.COLUMN_INDEX['status']] = new_status
                        self.tree.item(item_id, values=tuple(current_values))
                    elif current_status:
                        # If no status code, try translating with translate_status
                        translated_status = translate_status(current_status)
                        if translated_status != current_status:
                            current_values[self.COLUMN_INDEX['status']] = translated_status
                            self.tree.item(item_id, values=tuple(current_values))
            
            # Update language selector
            lang_display = {'hu': t('hungarian'), 'en': t('english')}
            self.lang_combo['values'] = [lang_display['hu'], lang_display['en']]
            self.lang_combo.set(lang_display.get(i18n.CURRENT_LANGUAGE, i18n.CURRENT_LANGUAGE))
