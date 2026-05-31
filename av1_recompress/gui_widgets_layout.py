from .gui_imports import *
from .gui_shared import *

class WidgetsLayoutMixin:
    def setup_ui(self):
        """Initialize and arrange the User Interface components.
        
        Sets up the main window layout, including:
        - Top control panel (language, paths, settings)
        - Video list Treeview
        - Bottom control panel (buttons, status, progress)
        """
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X)
        
        # Setting uniform row spacing in top_frame
        for i in range(9):
            top_frame.grid_rowconfigure(i, uniform='rows', weight=1)

        # Language selector in top right corner (approx 1cm = 37px offset from browse button)
        lang_frame = ttk.Frame(top_frame)
        lang_frame.grid(row=0, column=4, rowspan=9, sticky=tk.N, padx=(37, 5), pady=5)

        # Setting uniform row spacing in lang_frame as well
        for i in range(9):
            lang_frame.grid_rowconfigure(i, uniform='rows', weight=1)
        
        # Configure column 1 to expand (for sticky=tk.EW to work)
        lang_frame.grid_columnconfigure(1, weight=1)
        
        # Hybrid encoder path (row 0 - for SMDegrain denoising)
        self.hybrid_label = ttk.Label(lang_frame, text=t('hybrid_path'), width=20, anchor=tk.W)
        self.hybrid_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.hybrid_entry = ttk.Entry(lang_frame, textvariable=self.hybrid_path, width=24)
        self.hybrid_entry.grid(row=0, column=1, sticky=tk.EW, padx=2, pady=(5, 5))
        self.hybrid_browse_btn = ttk.Button(lang_frame, text=t('browse'), width=10, command=self.browse_hybrid)
        self.hybrid_browse_btn.grid(row=0, column=2, padx=2, pady=(5, 5))
        
        # FFmpeg path
        self.ffmpeg_label = ttk.Label(lang_frame, text=t('ffmpeg_path'), width=20, anchor=tk.W)
        self.ffmpeg_label.grid(row=1, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.ffmpeg_entry = ttk.Entry(lang_frame, textvariable=self.ffmpeg_path, width=24)
        self.ffmpeg_entry.grid(row=1, column=1, sticky=tk.W, padx=2, pady=(5, 5))
        self.ffmpeg_browse_btn = ttk.Button(lang_frame, text=t('browse'), width=10, command=self.browse_ffmpeg)
        self.ffmpeg_browse_btn.grid(row=1, column=2, padx=2, pady=(5, 5))
        
        # VirtualDub2 path
        self.vdub_label = ttk.Label(lang_frame, text=t('virtualdub_path'), width=20, anchor=tk.W)
        self.vdub_label.grid(row=2, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.vdub_entry = ttk.Entry(lang_frame, textvariable=self.virtualdub_path, width=24)
        self.vdub_entry.grid(row=2, column=1, sticky=tk.W, padx=2, pady=(5, 5))
        self.vdub_browse_btn = ttk.Button(lang_frame, text=t('browse'), width=10, command=self.browse_virtualdub)
        self.vdub_browse_btn.grid(row=2, column=2, padx=2, pady=(5, 5))

        # VirtualDub2 validation disable checkbox
        self.vdub_validation_checkbutton = ttk.Checkbutton(
            lang_frame,
            text=t('vdub_validation_disabled'),
            variable=self.vdub_validation_disabled,
            command=self._save_settings_debounced
        )
        self.vdub_validation_checkbutton.grid(row=3, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(2, 5))

        # ab-av1 path
        self.abav1_label = ttk.Label(lang_frame, text=t('abav1_path'), width=20, anchor=tk.W)
        self.abav1_label.grid(row=4, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.abav1_entry = ttk.Entry(lang_frame, textvariable=self.abav1_path, width=24)
        self.abav1_entry.grid(row=4, column=1, sticky=tk.W, padx=2, pady=(5, 5))
        self.abav1_browse_btn = ttk.Button(lang_frame, text=t('browse'), width=10, command=self.browse_abav1)
        self.abav1_browse_btn.grid(row=4, column=2, padx=2, pady=(5, 5))

        # NVENC enable checkbox
        nvenc_text = t('nvenc_enabled')
        if self.detected_gpu_name:
            nvenc_text += f" ({self.detected_gpu_name})"
        self.nvenc_checkbutton = ttk.Checkbutton(
            lang_frame,
            text=nvenc_text,
            variable=self.nvenc_enabled
        )
        self.nvenc_checkbutton.grid(row=5, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.nvenc_enabled.trace_add('write', self._on_nvenc_toggle)
        self._update_nvenc_checkbox_text()

        # SVT-AV1 preset slider
        svt_preset_frame = ttk.Frame(lang_frame)
        svt_preset_frame.grid(row=6, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))

        self.svt_preset_label = ttk.Label(svt_preset_frame, text=t('svt_preset'), width=20, anchor=tk.W)
        self.svt_preset_label.pack(side=tk.LEFT, padx=(0, 2))

        self.svt_preset_slider = ttk.Scale(
            svt_preset_frame,
            from_=1,
            to=12,
            orient=tk.HORIZONTAL,
            variable=self.svt_preset,
            length=150,
            command=self.update_svt_preset_label
        )
        self.svt_preset_slider.pack(side=tk.LEFT, padx=2)

        self.svt_preset_value_label = ttk.Label(svt_preset_frame, text="2", font=("Arial", 10, "bold"))
        self.svt_preset_value_label.pack(side=tk.LEFT, padx=2)

        # Workers frame: NVENC and SVT-AV1 side by side (below SVT preset)
        workers_frame = ttk.Frame(lang_frame)
        workers_frame.grid(row=7, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))
    
        # NVENC workers (left side)
        self.nvenc_workers_label = ttk.Label(workers_frame, text=t('nvenc_workers_short'), width=8, anchor=tk.W)
        self.nvenc_workers_label.pack(side=tk.LEFT, padx=(0, 2))
    
        self.nvenc_workers_slider = ttk.Scale(
            workers_frame,
            from_=1,
            to=3,
            orient=tk.HORIZONTAL,
            variable=self.nvenc_worker_count,
            length=70,
            command=self.update_nvenc_workers_label
        )
        try:
            self.nvenc_workers_slider.configure(resolution=1)
        except (tk.TclError, AttributeError):
            pass
        self.nvenc_workers_slider.pack(side=tk.LEFT, padx=2)
    
        self.nvenc_workers_value_label = ttk.Label(
            workers_frame,
            text=str(int(self.nvenc_worker_count.get())),
            font=("Arial", 10, "bold"),
            width=2
        )
        self.nvenc_workers_value_label.pack(side=tk.LEFT, padx=(0, 15))
        self.update_nvenc_workers_label(self.nvenc_worker_count.get())
        
        # SVT-AV1 workers (right side)
        self.svt_workers_label = ttk.Label(workers_frame, text=t('svt_workers_short'), width=5, anchor=tk.W)
        self.svt_workers_label.pack(side=tk.LEFT, padx=(0, 2))
    
        self.svt_workers_slider = ttk.Scale(
            workers_frame,
            from_=1,
            to=3,
            orient=tk.HORIZONTAL,
            variable=self.svt_worker_count,
            length=70,
            command=self.update_svt_workers_label
        )
        try:
            self.svt_workers_slider.configure(resolution=1)
        except (tk.TclError, AttributeError):
            pass
        self.svt_workers_slider.pack(side=tk.LEFT, padx=2)
    
        self.svt_workers_value_label = ttk.Label(
            workers_frame,
            text=str(int(self.svt_worker_count.get())),
            font=("Arial", 10, "bold"),
            width=2
        )
        self.svt_workers_value_label.pack(side=tk.LEFT, padx=2)
        self.update_svt_workers_label(self.svt_worker_count.get())
        
        # Left side: Source, Dest, Debug, Load Videos
        # Labels with fixed width to prevent layout shift on language change
        self.source_label = ttk.Label(top_frame, text=t('source'), width=12, anchor=tk.W)
        self.source_label.grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.source_entry = ttk.Entry(top_frame, width=50)
        self.source_entry.grid(row=0, column=1, padx=5, pady=2)
        self.source_browse_btn = ttk.Button(top_frame, text=t('browse'), command=self.browse_source)
        self.source_browse_btn.grid(row=0, column=2, padx=5, pady=2)
        
        self.dest_label = ttk.Label(top_frame, text=t('dest'), width=12, anchor=tk.W)
        self.dest_label.grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.dest_entry = ttk.Entry(top_frame, width=50)
        self.dest_entry.grid(row=1, column=1, padx=5, pady=2)
        self.dest_browse_btn = ttk.Button(top_frame, text=t('browse'), command=self.browse_dest)
        self.dest_browse_btn.grid(row=1, column=2, padx=5, pady=2)
        
        # Debug checkbox on left side
        self.debug_checkbutton = ttk.Checkbutton(
            top_frame,
            text=t('debug_mode'),
            variable=self.debug_mode,
            command=self.toggle_debug_mode
        )
        self.debug_checkbutton.grid(row=2, column=0, columnspan=3, sticky=tk.W, padx=5, pady=2)
        
        # Automatic VMAF/PSNR calculation checkbox
        self.auto_vmaf_psnr_checkbutton = ttk.Checkbutton(
            top_frame,
            text=t('auto_vmaf_psnr'),
            variable=self.auto_vmaf_psnr,
            command=self._on_auto_vmaf_psnr_change
        )
        self.auto_vmaf_psnr_checkbutton.grid(row=3, column=0, columnspan=3, sticky=tk.W, padx=5, pady=2)
        
        # HTTP Server control (checkbox + port entry)
        http_frame = ttk.Frame(top_frame)
        http_frame.grid(row=4, column=0, columnspan=3, sticky=tk.W, padx=5, pady=2)
        
        self.http_enabled_checkbutton = ttk.Checkbutton(
            http_frame,
            text=t('http_server_enabled'),
            variable=self.http_enabled_var,
            command=self._on_http_toggle
        )
        self.http_enabled_checkbutton.pack(side=tk.LEFT)
        
        self.http_port_label = ttk.Label(http_frame, text=t('http_port'))
        self.http_port_label.pack(side=tk.LEFT, padx=(10, 2))
        
        self.http_port_entry = ttk.Entry(http_frame, textvariable=self.http_port_var, width=6)
        self.http_port_entry.pack(side=tk.LEFT)
        self.http_port_entry.bind('<FocusOut>', self._on_http_port_change)
        self.http_port_entry.bind('<Return>', self._on_http_port_change)
        
        
        # Language selector on left side (row 5)
        lang_frame_row5 = ttk.Frame(top_frame)
        lang_frame_row5.grid(row=5, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        self.language_label = ttk.Label(lang_frame_row5, text=t('language'), anchor=tk.W)
        self.language_label.pack(side=tk.LEFT, padx=(0, 5))
        self.language_var = tk.StringVar()
        lang_display = {'hu': t('hungarian'), 'en': t('english')}
        self.lang_combo = ttk.Combobox(lang_frame_row5, textvariable=self.language_var, 
                                      values=[lang_display['hu'], lang_display['en']], 
                                      state='readonly', width=12)
        self.lang_combo.pack(side=tk.LEFT)
        self.lang_combo.bind('<<ComboboxSelected>>', self.change_language)
        
        # Displaying language
        self.lang_combo.set(lang_display.get(CURRENT_LANGUAGE, CURRENT_LANGUAGE))
        
        # Load videos button BELOW language selector (row 6)
        self.load_videos_btn = ttk.Button(top_frame, text=t('load_videos'), command=self.load_videos)
        self.load_videos_btn.grid(row=6, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Right side: Sliders (approx 1cm = 37px offset from browse button)
        # Min VMAF slider to the right
        vmaf_frame = ttk.Frame(top_frame)
        vmaf_frame.grid(row=0, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.min_vmaf_label = ttk.Label(vmaf_frame, text=t('min_vmaf'), width=20, anchor=tk.W)
        self.min_vmaf_label.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_slider = ttk.Scale(
            vmaf_frame,
            from_=85.0,
            to=99.9,
            orient=tk.HORIZONTAL,
            variable=self.min_vmaf,
            length=160,
            command=self.update_vmaf_label
        )
        self.vmaf_slider.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_value_label = ttk.Label(vmaf_frame, text="97.50", font=("Arial", 10, "bold"))
        self.vmaf_value_label.pack(side=tk.LEFT, padx=5)
        
        # VMAF Fallback slider to the right
        vmaf_step_frame = ttk.Frame(top_frame)
        vmaf_step_frame.grid(row=1, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.vmaf_fallback_label = ttk.Label(vmaf_step_frame, text=t('vmaf_fallback'), width=20, anchor=tk.W)
        self.vmaf_fallback_label.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_step_slider = ttk.Scale(
            vmaf_step_frame,
            from_=0.1,
            to=5.0,
            orient=tk.HORIZONTAL,
            variable=self.vmaf_step,
            length=160,
            command=self.update_vmaf_step_label
        )
        self.vmaf_step_slider.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_step_value_label = ttk.Label(vmaf_step_frame, text="0.25", font=("Arial", 10, "bold"))
        self.vmaf_step_value_label.pack(side=tk.LEFT, padx=5)
        
        # Max Encoded slider to the right
        max_encoded_frame = ttk.Frame(top_frame)
        max_encoded_frame.grid(row=2, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.max_encoded_label = ttk.Label(max_encoded_frame, text=t('max_encoded'), width=20, anchor=tk.W)
        self.max_encoded_label.pack(side=tk.LEFT, padx=(5, 2))
        
        # Mode selector combobox (full video / video track only)
        # Note: We don't use textvariable because the StringVar stores 'full'/'video' 
        # while the Combobox displays localized text
        self.max_encoded_mode_combo = ttk.Combobox(
            max_encoded_frame,
            values=[t('max_encoded_mode_full'), t('max_encoded_mode_video')],
            state='readonly',
            width=12
        )
        self.max_encoded_mode_combo.pack(side=tk.LEFT, padx=2)
        # Map display values to internal values
        self._max_encoded_mode_map = {
            t('max_encoded_mode_full'): 'full',
            t('max_encoded_mode_video'): 'video'
        }
        self._max_encoded_mode_reverse_map = {v: k for k, v in self._max_encoded_mode_map.items()}
        # Set initial display value
        self.max_encoded_mode_combo.set(self._max_encoded_mode_reverse_map.get(self.max_encoded_mode.get(), t('max_encoded_mode_full')))
        # Bind selection event
        self.max_encoded_mode_combo.bind('<<ComboboxSelected>>', self._on_max_encoded_mode_change)
        
        # Container for slider to force fixed width (length param might be ignored by theme)
        slider_container = tk.Frame(max_encoded_frame, width=108, height=30)
        slider_container.pack(side=tk.LEFT, padx=2)
        slider_container.pack_propagate(False)  # Don't let slider expand the frame
        
        self.max_encoded_slider = ttk.Scale(
            slider_container,
            from_=1,
            to=100,
            orient=tk.HORIZONTAL,
            variable=self.max_encoded_percent,
            command=self.update_max_encoded_label
        )
        self.max_encoded_slider.pack(fill=tk.X, expand=True) # Center vertically in container
        
        self.max_encoded_value_label = ttk.Label(max_encoded_frame, text="75%", font=("Arial", 10, "bold"))
        self.max_encoded_value_label.pack(side=tk.LEFT, padx=5)
        
        # Resize checkbox and slider
        resize_frame = ttk.Frame(top_frame)
        resize_frame.grid(row=3, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.resize_checkbox = ttk.Checkbutton(
            resize_frame,
            text=t('resize_height'),
            variable=self.resize_enabled,
            command=lambda: (self.toggle_resize_slider(), self._save_settings_debounced())
        )
        self.resize_checkbox.pack(side=tk.LEFT, padx=5)
        
        self.resize_slider = ttk.Scale(
            resize_frame,
            from_=360,
            to=2160,
            orient=tk.HORIZONTAL,
            variable=self.resize_height,
            length=160,
            command=self.update_resize_label
        )
        try:
            self.resize_slider.configure(resolution=10)
        except (tk.TclError, AttributeError):
            pass
        
        self.resize_slider.pack(side=tk.LEFT, padx=5)
        
        self.resize_value_label = ttk.Label(resize_frame, text="1080p", font=("Arial", 10, "bold"))
        self.resize_value_label.pack(side=tk.LEFT, padx=5)
    
        # Initially hide the slider
        self.resize_slider.pack_forget()
        self.resize_value_label.pack_forget()
        
        # Skip AV1 files checkbox below resize height
        skip_av1_frame = ttk.Frame(top_frame)
        skip_av1_frame.grid(row=4, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.skip_av1_checkbutton = ttk.Checkbutton(
            skip_av1_frame,
            text=t('skip_av1'),
            variable=self.skip_av1_files,
            command=self._on_skip_av1_change
        )
        self.skip_av1_checkbutton.pack(side=tk.LEFT, padx=5)
        
        # Audio dynamic compression checkbox and method selector
        audio_compression_frame = ttk.Frame(top_frame)
        audio_compression_frame.grid(row=5, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.audio_compression_checkbutton = ttk.Checkbutton(
            audio_compression_frame,
            text=t('audio_compression'),
            variable=self.audio_compression_enabled,
            command=self._on_audio_compression_change
        )
        self.audio_compression_checkbutton.pack(side=tk.LEFT, padx=5)

        deband_frame = ttk.Frame(top_frame)
        deband_frame.grid(row=7, column=3, padx=(20, 5), pady=2, sticky=tk.W)

        self.deband_checkbutton = ttk.Checkbutton(
            deband_frame,
            text=t('deband_enabled'),
            variable=self.deband_enabled,
            onvalue=False,
            offvalue=True,
            command=self._on_deband_change
        )
        self.deband_checkbutton.pack(side=tk.LEFT, padx=5)

        self.denoised_master_8bit_checkbutton = ttk.Checkbutton(
            deband_frame,
            text=t('denoised_master_8bit_test'),
            variable=self.force_8bit_denoised_master,
            command=self._on_force_8bit_denoised_master_change
        )
        self.denoised_master_8bit_checkbutton.pack(side=tk.LEFT, padx=(20, 5))

        self.audio_compression_combo = ttk.Combobox(
            audio_compression_frame,
            values=[t('audio_compression_fast'), t('audio_compression_dialogue')],
            state='readonly',
            width=16
        )
        self.audio_compression_combo.pack(side=tk.LEFT, padx=5)
        self.audio_compression_combo.set(t('audio_compression_fast'))
        # Event handler for combobox change
        def on_audio_method_change(event=None):
            old_value = getattr(self, '_last_audio_method_value', self.audio_compression_method.get())
            selected = self.audio_compression_combo.get()
            new_value = 'fast' if selected == t('audio_compression_fast') else 'dialogue'
            
            if not hasattr(self, '_last_audio_method_value'):
                self._last_audio_method_value = new_value
            
            # Ha kódolás fut és az érték változott
            if new_value != old_value and getattr(self, 'is_encoding', False):
                if not self._confirm_setting_change_during_encoding(self.audio_compression_method, new_value, old_value):
                    # Mégse: visszaállítás
                    if old_value == 'fast':
                        self.audio_compression_combo.set(t('audio_compression_fast'))
                    else:
                        self.audio_compression_combo.set(t('audio_compression_dialogue'))
                    return
            
            self._last_audio_method_value = new_value
            self.audio_compression_method.set(new_value)
            self._save_settings_debounced()  # Automatic save with debounce
        self.audio_compression_combo.bind('<<ComboboxSelected>>', on_audio_method_change)
        # Setting initial value
        self.audio_compression_method.set('fast')
        
        # CRF increment slider (row=6, column=3)
        crf_increment_frame = ttk.Frame(top_frame)
        crf_increment_frame.grid(row=6, column=3, padx=(20, 5), pady=2, sticky=tk.W)
        
        self.crf_increment_label = ttk.Label(crf_increment_frame, text=t('crf_increment'), width=20, anchor=tk.W)
        self.crf_increment_label.pack(side=tk.LEFT, padx=5)
        
        self.crf_increment_slider = ttk.Scale(
            crf_increment_frame,
            from_=1,
            to=5,
            orient=tk.HORIZONTAL,
            variable=self.crf_increment,
            length=160,
            command=self.update_crf_increment_label
        )
        try:
            self.crf_increment_slider.configure(resolution=1)
        except (tk.TclError, AttributeError):
            pass
        self.crf_increment_slider.pack(side=tk.LEFT, padx=5)
        
        self.crf_increment_value_label = ttk.Label(crf_increment_frame, text="1", font=("Arial", 10, "bold"))
        self.crf_increment_value_label.pack(side=tk.LEFT, padx=5)
        
        # Max CQ limit (row=8 in lang_frame, below workers)
        max_cq_frame = ttk.Frame(lang_frame)
        max_cq_frame.grid(row=8, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        
        self.max_cq_label = ttk.Label(max_cq_frame, text=t('max_cq_limit'), width=14, anchor=tk.W)
        self.max_cq_label.pack(side=tk.LEFT, padx=(0, 2))
        
        self.max_cq_spinbox = ttk.Spinbox(
            max_cq_frame,
            from_=0,
            to=63,
            width=4,
            textvariable=self.max_cq_limit
        )
        self.max_cq_spinbox.pack(side=tk.LEFT, padx=2)
        
        self.max_cq_hint_label = ttk.Label(max_cq_frame, text=t('max_cq_auto_hint'), font=("Arial", 8))
        self.max_cq_hint_label.pack(side=tk.LEFT, padx=2)
        
        # === NOTEBOOK (multiple tabs) ===
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 5))
        
        # TAB 1: Videos (TreeView)
        self.videos_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.videos_tab, text=t('videos_tab'))
        
        # NVENC console tabs (top-level, with dynamic display)
        self.max_nvenc_consoles = 3
        self.nvenc_console_frames = []
        self.nvenc_consoles = []
        self.nvenc_loggers = []
        self.nvenc_log_files = []
    
        # SVT-AV1 console tabs (top-level, with dynamic display)
        self.max_svt_consoles = 3
        self.svt_console_frames = []
        self.svt_consoles = []
        self.svt_loggers = []
        self.svt_log_files = []
    
        # Creating NVENC and SVT-AV1 log files (delete and recreate)
        # Use the same directory as av1_recompress.log
        # APP_ROOT should be a directory (parent of .pyz file if running as .pyz)
        # But check if it's actually a file and use parent if needed
        from .core_preamble_and_imports import APP_ROOT
        
        if APP_ROOT:
            # Check if APP_ROOT is actually a file (shouldn't happen, but handle it)
            # This can happen if APP_ROOT was set incorrectly
            try:
                if APP_ROOT.exists() and APP_ROOT.is_file():
                    log_dir = APP_ROOT.parent
                else:
                    log_dir = APP_ROOT
            except (OSError, AttributeError):
                # If check fails, assume it's a directory
                log_dir = APP_ROOT
        else:
            # Fallback: use current working directory
            log_dir = Path.cwd()
        
        nvenc_log_paths = [log_dir / f"nvenc_console_{idx + 1}.log" for idx in range(self.max_nvenc_consoles)]
        svt_log_paths = [log_dir / f"svt_console_{idx + 1}.log" for idx in range(self.max_svt_consoles)]
    
        # Deleting log files if they exist
        for nvenc_log_path in nvenc_log_paths:
            if nvenc_log_path.exists():
                try:
                    nvenc_log_path.unlink()
                except (OSError, PermissionError, FileNotFoundError):
                    pass
    
        for svt_log_path in svt_log_paths:
            if svt_log_path.exists():
                try:
                    svt_log_path.unlink()
                except (OSError, PermissionError, FileNotFoundError):
                    pass
    
        # Store log file paths for later reopening if needed
        self.nvenc_log_paths = nvenc_log_paths
        self.svt_log_paths = svt_log_paths
        
        # Creating log files - keep them open
        for idx, nvenc_log_path in enumerate(nvenc_log_paths):
            try:
                log_file = open(nvenc_log_path, "w", encoding="utf-8")
                header_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                log_file.write(f"[{header_ts}] === NVENC CONSOLE LOG #{idx + 1} ===\n")
                log_file.write(f"[{header_ts}] Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"[{header_ts}] {'='*80}\n\n")
                log_file.flush()
            except (OSError, IOError, PermissionError):
                log_file = None
            self.nvenc_log_files.append(log_file)
    
        for idx, svt_log_path in enumerate(svt_log_paths):
            try:
                log_file = open(svt_log_path, "w", encoding="utf-8")
                header_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                log_file.write(f"[{header_ts}] === SVT-AV1 CONSOLE LOG #{idx + 1} ===\n")
                log_file.write(f"[{header_ts}] Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"[{header_ts}] {'='*80}\n\n")
                log_file.flush()
            except (OSError, IOError, PermissionError):
                log_file = None
            self.svt_log_files.append(log_file)
    
        # Creating NVENC console tabs and loggers
        for idx in range(self.max_nvenc_consoles):
            frame = ttk.Frame(self.notebook)
            self.notebook.add(frame, text=f"{t('nvenc_console')} {idx + 1}")
            console_widget = scrolledtext.ScrolledText(
                frame,
                wrap=tk.WORD,
                width=120,
                height=30,
                font=("Consolas", 9),
                bg="#1e1e1e",
                fg="#d4d4d4",
                state=tk.DISABLED
            )
            console_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            self.nvenc_console_frames.append(frame)
            self.nvenc_consoles.append(console_widget)
            log_file = self.nvenc_log_files[idx] if idx < len(self.nvenc_log_files) else None
            # Passing log_files_list and logger_index so it can choose file based on logger_index
            # logger_index does not change, avoiding race condition
            nvenc_logger = ConsoleLogger(console_widget, self.encoding_queue, log_file=log_file, log_files_list=self.nvenc_log_files, logger_index=idx)
            nvenc_logger.set_encoder_type('nvenc')
            nvenc_logger.set_worker_index(idx)  # Initially same as logger_index
            # Store log file paths for reopening if needed
            nvenc_logger.nvenc_log_paths = self.nvenc_log_paths
            self.nvenc_loggers.append(nvenc_logger)
        
        if self.nvenc_consoles:
            self.nvenc_console = self.nvenc_consoles[0]
        else:
            self.nvenc_console = None
        if self.nvenc_loggers:
            self.nvenc_logger = self.nvenc_loggers[0]
        else:
            self.nvenc_logger = None
    
        self.refresh_nvenc_console_tabs(self.get_configured_nvenc_workers())
        
        # Creating SVT-AV1 console tabs and loggers (dynamic 1-3)
        for idx in range(self.max_svt_consoles):
            frame = ttk.Frame(self.notebook)
            self.notebook.add(frame, text=f"{t('svt_console')} {idx + 1}")
            console_widget = scrolledtext.ScrolledText(
                frame,
                wrap=tk.WORD,
                width=120,
                height=30,
                font=("Consolas", 9),
                bg="#1e1e1e",
                fg="#d4d4d4",
                state=tk.DISABLED
            )
            console_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            self.svt_console_frames.append(frame)
            self.svt_consoles.append(console_widget)
            log_file = self.svt_log_files[idx] if idx < len(self.svt_log_files) else None
            # Passing log_files_list and logger_index so it can choose file based on logger_index
            # logger_index does not change, avoiding race condition
            svt_logger = ConsoleLogger(console_widget, self.encoding_queue, log_file=log_file, log_files_list=self.svt_log_files, logger_index=idx)
            svt_logger.set_encoder_type('svt')
            svt_logger.set_worker_index(idx)  # Initially same as logger_index
            # Store log file paths for reopening if needed
            svt_logger.svt_log_paths = self.svt_log_paths
            self.svt_loggers.append(svt_logger)
        
        if self.svt_consoles:
            self.svt_console = self.svt_consoles[0]
        else:
            self.svt_console = None
        if self.svt_loggers:
            self.svt_logger = self.svt_loggers[0]
        else:
            self.svt_logger = None
    
        self.refresh_svt_console_tabs(self.get_configured_svt_workers())
    
        # Closing log files when application closes
        def on_closing():
            # Signal all threads that application is closing - FIRST THING!
            from .gui_shared import set_app_closing
            set_app_closing()
            
            # Use dynamic log writer getter to avoid None errors
            from .core_paths_tools_logging import get_log_writer
            
            def log_debug(msg):
                """Safe logging wrapper that handles None LOG_WRITER."""
                try:
                    writer = get_log_writer()
                    if writer:
                        writer.write(msg)
                        writer.flush()
                except (AttributeError, OSError, IOError):
                    pass  # Silently ignore if log writer is not available
            
            log_debug("[DEBUG] on_closing() called - X button pressed\n")
            
            # Check if encoding is ACTUALLY in progress
            # Not just the is_encoding flag, but also if there are active workers
            is_encoding_flag = getattr(self, 'is_encoding', False)
            log_debug(f"[DEBUG] is_encoding_flag: {is_encoding_flag}\n")
            
            # Check if any worker threads are actually running
            has_active_workers = False
            if is_encoding_flag:
                log_debug("[DEBUG] Checking for active workers...\n")
                # Check NVENC workers
                if hasattr(self, 'nvenc_worker_threads') and self.nvenc_worker_threads:
                    log_debug(f"[DEBUG] Checking {len(self.nvenc_worker_threads)} NVENC worker threads\n")
                    for i, thread in enumerate(self.nvenc_worker_threads):
                        if thread and thread.is_alive():
                            log_debug(f"[DEBUG] NVENC worker {i} is alive\n")
                            has_active_workers = True
                            break
                
                # Check SVT worker
                if not has_active_workers and hasattr(self, 'svt_worker_threads') and self.svt_worker_threads:
                    log_debug(f"[DEBUG] Checking {len(self.svt_worker_threads)} SVT worker threads\n")
                    for i, thread in enumerate(self.svt_worker_threads):
                        if thread and thread.is_alive():
                            log_debug(f"[DEBUG] SVT worker {i} is alive\n")
                            has_active_workers = True
                            break
                
                # Check encoding worker
                if not has_active_workers and hasattr(self, 'encoding_worker_thread') and self.encoding_worker_thread and self.encoding_worker_thread.is_alive():
                    log_debug("[DEBUG] encoding_worker_thread is alive\n")
                    has_active_workers = True
                
                # Check queues
                if not has_active_workers:
                    log_debug("[DEBUG] Checking pending task queues\n")
                    try:
                        # LIST-BASED QUEUE: Check list-based queues
                        has_svt_queue = len(getattr(self, 'pending_svt_tasks', [])) > 0
                        has_nvenc_queue = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
                        log_debug(f"[DEBUG] pending_svt_tasks: {len(getattr(self, 'pending_svt_tasks', []))} items\n")
                        log_debug(f"[DEBUG] pending_nvenc_tasks: {len(getattr(self, 'pending_nvenc_tasks', []))} items\n")
                        if has_svt_queue or has_nvenc_queue:
                            has_active_workers = True
                    except Exception as e:
                        log_debug(f"[DEBUG] Exception checking queues: {e}\n")
                        pass
            
            log_debug(f"[DEBUG] has_active_workers: {has_active_workers}\n")
            
            # Only ask for confirmation if there are ACTUAL active workers
            if is_encoding_flag and has_active_workers:
                log_debug("[DEBUG] Active workers detected, showing confirmation dialog\n")
                if not messagebox.askyesno(t('confirm_exit_title'), t('confirm_exit_listening')):
                    log_debug("[DEBUG] User cancelled exit\n")
                    return
                log_debug("[DEBUG] User confirmed exit, calling stop_encoding_immediate()\n")
                # User confirmed exit -> Kill everything immediately!
                self.stop_encoding_immediate()
                log_debug("[DEBUG] stop_encoding_immediate() completed\n")
                # Give it a moment to kill processes and start DB save
                if hasattr(self, 'last_stop_thread') and self.last_stop_thread.is_alive():
                    log_debug("[DEBUG] Waiting for last_stop_thread to finish (max 5s)...\n")
                    # Wait maximum 5 seconds for the stop thread to finish (it will start the DB thread)
                    # Reduced from 60s to 5s to avoid long waits during exit
                    self.last_stop_thread.join(timeout=5.0)
                    log_debug("[DEBUG] last_stop_thread join completed\n")
            else:
                log_debug("[DEBUG] No active workers, proceeding with cleanup\n")
                # If not encoding (or encoding flag is stale), we must save the state manually before destroy
                # Reset the stale is_encoding flag if needed
                if is_encoding_flag and not has_active_workers:
                    log_debug("[DEBUG] Resetting stale is_encoding flag\n")
                    # THREAD-SAFETY FIX: Use helper method for lock-protected state access
                    self.set_encoding_state(is_encoding=False, graceful_stop_requested=False)

                # But only if we have something to save (video items exist)
                if hasattr(self, 'video_items') and self.video_items:
                    log_debug(f"[DEBUG] Saving state to DB ({len(self.video_items)} video items) on main thread...\n")
                    save_started = time.time()
                    try:
                        # IMPORTANT: avoid background-thread Tk variable access during close.
                        # save_state_to_db reads Tk vars (.get()), which can deadlock when the
                        # main thread is waiting in join() inside WM_DELETE handler.
                        self.save_state_to_db()
                    except Exception as e:
                        log_debug(f"[DEBUG] Exception in save_state_to_db(): {e}\n")
                    finally:
                        save_elapsed = time.time() - save_started
                        if save_elapsed > 10.0:
                            log_debug(f"[DEBUG] WARNING: save_state_to_db() took {save_elapsed:.2f}s (>10s)\n")
                        else:
                            log_debug(f"[DEBUG] save_state_to_db() completed in {save_elapsed:.2f}s\n")

            if getattr(self, 'http_server_running', False):
                log_debug("[DEBUG] Stopping HTTP server...\n")
                try:
                    self.stop_http_server()
                except Exception as e:
                    log_debug(f"[DEBUG] Exception while stopping HTTP server: {e}\n")
                http_thread = getattr(self, 'http_thread', None)
                if http_thread and http_thread.is_alive():
                    try:
                        http_thread.join(timeout=2.0)
                        log_debug("[DEBUG] HTTP server thread join completed\n")
                    except Exception as e:
                        log_debug(f"[DEBUG] Exception joining HTTP server thread: {e}\n")

            log_debug("[DEBUG] Closing NVENC log files...\n")
            if hasattr(self, 'nvenc_log_files'):
                for i, log_file in enumerate(self.nvenc_log_files):
                    if log_file:
                        try:
                            log_file.close()
                            log_debug(f"[DEBUG] Closed NVENC log file {i}\n")
                        except (OSError, IOError, AttributeError) as e:
                            log_debug(f"[DEBUG] Exception closing NVENC log file {i}: {e}\n")
                            pass
            
            log_debug("[DEBUG] Closing SVT log files...\n")
            if hasattr(self, 'svt_log_files'):
                for i, log_file in enumerate(self.svt_log_files):
                    if log_file:
                        try:
                            log_file.close()
                            log_debug(f"[DEBUG] Closed SVT log file {i}\n")
                        except (OSError, IOError, AttributeError) as e:
                            log_debug(f"[DEBUG] Exception closing SVT log file {i}: {e}\n")
                            pass
            
            log_debug("[DEBUG] Waiting for DB threads...\n")
            try:
                # Reduced timeout from 30s to 5s to avoid long waits during exit
                self._wait_for_db_threads(timeout=5.0)
                log_debug("[DEBUG] _wait_for_db_threads() completed\n")
            except Exception as e:
                log_debug(f"[DEBUG] Exception in _wait_for_db_threads(): {e}\n")
                pass
            
            log_debug("[DEBUG] Cleaning up database (WAL checkpoint)...\n")
            try:
                # CRITICAL: Cleanup database to remove .db-shm and .db-wal files
                self._cleanup_database()
                log_debug("[DEBUG] _cleanup_database() completed\n")
            except Exception as e:
                log_debug(f"[DEBUG] Exception in _cleanup_database(): {e}\n")
                pass
            
            log_debug("[DEBUG] Calling root.destroy()...\n")
            self.root.destroy()
            log_debug("[DEBUG] root.destroy() completed - application should exit now\n")
        
        self._on_closing_handler = on_closing
        self.root.protocol("WM_DELETE_WINDOW", on_closing)
        
        # Initializing TreeView
        self._setup_treeview()

