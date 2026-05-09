from .gui_imports import *
from .gui_shared import *

class TreeSetupMixin:
    _DECORATED_TREE_COLUMNS = ("denoise", "status", "progress")
    _TREE_HOVER_TOOLTIP_DELAY_MS = 2000
    _TREE_HOVER_FALLBACK_TEXT = {
        'hover_meta_title': 'Forrás videó metaadatok',
        'hover_meta_loading': 'Metaadatok betöltése...',
        'hover_meta_unavailable': 'A metaadatok nem olvashatók ki.',
        'hover_meta_resolution': 'Felbontás',
        'hover_meta_pix_fmt': 'Pixel formátum',
        'hover_meta_color_space': 'Színtér',
        'hover_meta_color_primaries': 'Színprimerek',
        'hover_meta_color_transfer': 'Transzferfüggvény',
        'hover_meta_color_range': 'Színtartomány',
        'hover_meta_sar': 'SAR',
        'hover_meta_dar': 'DAR',
        'hover_meta_rotation': 'Metaadat rotáció',
        'hover_meta_file_size': 'Méret',
        'hover_meta_bitrate': 'Bitráta',
        'hover_meta_source_section': 'Forrás',
        'hover_meta_output_section': 'Célfájl',
        'hover_meta_output_missing': 'Célfájl metaadat nem érhető el.',
    }

    def _tree_hover_text(self, key):
        localized = t(key)
        if localized and localized != key:
            return localized
        return self._TREE_HOVER_FALLBACK_TEXT.get(key, key)

    def _decorate_tree_column_value(self, column_name, value):
        if value is None:
            return value
        if column_name in self._DECORATED_TREE_COLUMNS:
            return to_decorative_text(value)
        return value

    def _undecorate_tree_column_value(self, column_name, value):
        if value is None:
            return value
        if column_name in self._DECORATED_TREE_COLUMNS:
            return from_decorative_text(value)
        return value

    def _decorate_tree_values_for_display(self, values):
        if values is None:
            return values
        vals = list(values)
        for column_name in self._DECORATED_TREE_COLUMNS:
            idx = self.COLUMN_INDEX.get(column_name)
            if idx is not None and idx < len(vals):
                vals[idx] = self._decorate_tree_column_value(column_name, vals[idx])
        return tuple(vals)

    def _undecorate_tree_values_for_logic(self, values):
        if values is None:
            return values
        vals = list(values)
        for column_name in self._DECORATED_TREE_COLUMNS:
            idx = self.COLUMN_INDEX.get(column_name)
            if idx is not None and idx < len(vals):
                vals[idx] = self._undecorate_tree_column_value(column_name, vals[idx])
        return tuple(vals)

    def _install_tree_text_parser(self):
        """Install display parser wrappers for Treeview values."""
        if getattr(self, "_tree_text_parser_installed", False):
            return
        if not hasattr(self, "tree") or self.tree is None:
            return

        raw_insert = self.tree.insert
        raw_set = self.tree.set
        raw_item = self.tree.item

        def insert_wrapper(parent, index, iid=None, **kw):
            if "values" in kw and kw["values"] is not None:
                kw["values"] = self._decorate_tree_values_for_display(kw["values"])
            if iid is None:
                return raw_insert(parent, index, **kw)
            return raw_insert(parent, index, iid=iid, **kw)

        def set_wrapper(item, column=None, value=None):
            if value is None:
                result = raw_set(item, column)
                if column in self._DECORATED_TREE_COLUMNS:
                    return self._undecorate_tree_column_value(column, result)
                return result
            decorated_value = self._decorate_tree_column_value(column, value)
            return raw_set(item, column, decorated_value)

        def item_wrapper(item, option=None, **kw):
            if "values" in kw and kw["values"] is not None:
                kw["values"] = self._decorate_tree_values_for_display(kw["values"])

            if option is None:
                result = raw_item(item, **kw)
            else:
                result = raw_item(item, option, **kw)

            if kw:
                return result

            if option == "values":
                return self._undecorate_tree_values_for_logic(result)

            if option is None and isinstance(result, dict) and "values" in result:
                out = dict(result)
                out["values"] = self._undecorate_tree_values_for_logic(out.get("values"))
                return out

            return result

        self.tree.insert = insert_wrapper
        self.tree.set = set_wrapper
        self.tree.item = item_wrapper
        self._tree_text_parser_installed = True

    def _setup_treeview(self):
        """Initialize the TreeView widget.
        
        Sets up columns, headings, and scrollbars for the video list.
        """
        # Move TreeView to the first tab
        tree_frame = ttk.Frame(self.videos_tab, padding="10")
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ("denoise", "hard_rotate", "video_name", "status", "cq", "vmaf", "psnr", "progress", "orig_size", "new_size", "size_change", "duration", "frames", "completed_date")
        # Column index table - prevents needing to fix in thousand places if modified
        self.COLUMN_INDEX = {col: idx for idx, col in enumerate(columns)}
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", height=15, displaycolumns=columns)
        self._install_tree_text_parser()
        
        self.tree.heading("#0", text=t('column_order'), command=lambda: self.sort_by_column("#0"))
        self.tree.heading("denoise", text=t('column_denoise'), command=lambda: self.sort_by_column("denoise"))
        self.tree.heading("hard_rotate", text=t('column_hard_rotate'), command=lambda: self.sort_by_column("hard_rotate"))
        self.tree.heading("video_name", text=t('column_video'), command=lambda: self.sort_by_column("video_name"))
        self.tree.heading("status", text=t('column_status'), command=lambda: self.sort_by_column("status"))
        self.tree.heading("cq", text=t('column_cq'), command=lambda: self.sort_by_column("cq"))
        self.tree.heading("vmaf", text=t('column_vmaf'), command=lambda: self.sort_by_column("vmaf"))
        self.tree.heading("psnr", text=t('column_psnr'), command=lambda: self.sort_by_column("psnr"))
        self.tree.heading("progress", text=t('column_progress'), command=lambda: self.sort_by_column("progress"))
        self.tree.heading("orig_size", text=t('column_orig_size'), command=lambda: self.sort_by_column("orig_size"))
        self.tree.heading("new_size", text=t('column_new_size'), command=lambda: self.sort_by_column("new_size"))
        self.tree.heading("size_change", text=t('column_size_change'), command=lambda: self.sort_by_column("size_change"))
        self.tree.heading("duration", text=t('column_duration'), command=lambda: self.sort_by_column("duration"))
        self.tree.heading("frames", text=t('column_frames'), command=lambda: self.sort_by_column("frames"))
        self.tree.heading("completed_date", text=t('column_completed'), command=lambda: self.sort_by_column("completed_date"))
        
        # Set #0 column (tree column) width first - needed for expand/collapse icons
        # A korábbi verzióban 40 volt, de most 50, hogy biztosan látható legyen a plusz ikon
        if '#0' in self.col_widths:
            # Explicit beállítás, hogy biztosan látható legyen a plusz ikon
            self.tree.column('#0', width=self.col_widths['#0'], minwidth=40, stretch=False)
        
        for col, width in self.col_widths.items():
            # Skip #0 as it's already set above
            if col == '#0':
                continue
            # Denoise column - narrow, center aligned
            if col == "denoise":
                self.tree.column(col, width=width, anchor=tk.CENTER, stretch=False)
            elif col == "hard_rotate":
                self.tree.column(col, width=width, anchor=tk.CENTER, stretch=False)
            # Left align for file size columns (better readability for numbers)
            elif col in ("orig_size", "new_size", "size_change", "duration", "frames"):
                self.tree.column(col, width=width, anchor=tk.W)
            elif col in ("vmaf", "psnr", "cq"):
                # VMAF, PSNR and CQ columns also left aligned (numbers)
                self.tree.column(col, width=width, anchor=tk.W)
            else:
                self.tree.column(col, width=width)
        
        self.tree.bind('<B1-Motion>', self.on_column_resize)
        self.tree.bind('<Double-Button-1>', self.on_double_click)
        self.tree.bind('<Button-3>', self.on_right_click)
        self._init_tree_hover_metadata_support()
        
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.summary_frame = ttk.Frame(self.videos_tab, padding="10")
        # Hidden by default, only shown if there is a summary
        # self.summary_frame.pack(fill=tk.X)
        
        self.summary_tree = ttk.Treeview(self.summary_frame, columns=columns, show="tree", height=1, displaycolumns=columns)
        for col, width in self.col_widths.items():
            try:
                # Left align for file size columns (better readability for numbers)
                if col in ("orig_size", "new_size", "size_change"):
                    self.summary_tree.column(col, width=width, anchor=tk.W)
                else:
                    self.summary_tree.column(col, width=width)
            except (tk.TclError, AttributeError):
                pass
        self.summary_tree.pack(fill=tk.X)
        
        self.tree.tag_configure("pending", foreground="blue")
        self.tree.tag_configure("encoding", foreground="orange")
        self.tree.tag_configure("encoding_nvenc", foreground="orange")
        self.tree.tag_configure("encoding_svt", foreground="purple")
        self.tree.tag_configure("audio_edit", foreground="#008080")
        self.tree.tag_configure("completed", foreground="green")
        self.tree.tag_configure("completed_copy", foreground="#2E8B57")
        self.tree.tag_configure("needs_check", foreground="darkorange")
        self.tree.tag_configure("failed", foreground="red")
        self.tree.tag_configure("subtitle", foreground="gray")
        self.tree.tag_configure("invalid_subtitle", foreground="red")
        self.summary_tree.tag_configure("summary", background="#e8e8e8", font=("Arial", 10, "bold"))
        
        bottom_frame = ttk.Frame(self.root, padding="10")
        bottom_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        button_frame = ttk.Frame(bottom_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.start_button = ttk.Button(button_frame, text=t('btn_start'), command=self.start_encoding)
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # stop_button removed - start_button will act as "Stop" button during execution
        self.stop_button = None  # No separate stop button anymore
    
        self.immediate_stop_button = ttk.Button(button_frame, text=t('btn_immediate_stop'), command=self.stop_encoding_immediate, state=tk.DISABLED)
        self.immediate_stop_button.pack(side=tk.LEFT, padx=5)
        
        self.clear_table_btn = ttk.Button(button_frame, text=t('btn_clear_table'), command=self.clear_table)
        self.clear_table_btn.pack(side=tk.LEFT, padx=5)
        
        self.hide_completed_checkbutton = ttk.Checkbutton(
            button_frame,
            text=t('btn_hide_completed'),
            variable=self.hide_completed,
            command=self.toggle_hide_completed
        )
        self.hide_completed_checkbutton.pack(side=tk.LEFT, padx=5)
        
        # Storing hidden item_ids (only for display, not needed for database save)
        
        # Starting timer to update estimated completion time (every 10 seconds)
        self.start_estimated_end_timer()
        self.hidden_items = set()
        
        # Status row Frame (status_label on left, notification on right)
        status_frame = ttk.Frame(bottom_frame)
        status_frame.pack(fill=tk.X)
        
        self.status_label = ttk.Label(status_frame, text=t('status_ready'), font=("Arial", 11, "bold"))
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Notification label (for Database save indication)
        self.db_notification_label = ttk.Label(status_frame, text="", font=("Arial", 10), foreground="green")
        self.db_notification_label.pack(side=tk.RIGHT, padx=10)
        
        self.progress_bar = ttk.Progressbar(bottom_frame, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=5)

    def _init_tree_hover_metadata_support(self):
        """Enable delayed hover popup with additional source metadata."""
        if getattr(self, "_tree_hover_support_initialized", False):
            return

        self._tree_hover_support_initialized = True
        self._tree_hover_after_id = None
        self._tree_hover_item_id = None
        self._tree_hover_token = 0
        self._tree_hover_popup = None
        self._tree_hover_popup_label = None
        self._tree_hover_popup_item_id = None
        self._tree_hover_pointer = (0, 0)
        self._tree_hover_metadata_queue = queue.Queue()
        self._tree_hover_probe_inflight = set()

        self.tree.bind('<Motion>', self._on_tree_hover_motion, add='+')
        self.tree.bind('<Leave>', self._on_tree_hover_leave, add='+')
        self.tree.bind('<ButtonPress>', self._on_tree_hover_cancel, add='+')
        self.tree.bind('<MouseWheel>', self._on_tree_hover_cancel, add='+')

        self.root.after(150, self._poll_tree_hover_metadata_queue)

    def _cancel_tree_hover_timer(self):
        after_id = getattr(self, '_tree_hover_after_id', None)
        if after_id:
            try:
                self.root.after_cancel(after_id)
            except (tk.TclError, ValueError):
                pass
        self._tree_hover_after_id = None

    def _destroy_tree_hover_popup(self):
        popup = getattr(self, '_tree_hover_popup', None)
        if popup is not None:
            try:
                popup.destroy()
            except tk.TclError:
                pass
        self._tree_hover_popup = None
        self._tree_hover_popup_label = None
        self._tree_hover_popup_item_id = None

    def _hide_tree_hover_popup(self, clear_item=False):
        self._cancel_tree_hover_timer()
        self._destroy_tree_hover_popup()
        if clear_item:
            self._tree_hover_item_id = None

    def _on_tree_hover_motion(self, event):
        try:
            item_id = self.tree.identify_row(event.y)
        except tk.TclError:
            return

        self._tree_hover_pointer = (event.x_root, event.y_root)

        if not item_id:
            self._hide_tree_hover_popup(clear_item=True)
            return

        try:
            tags = self.tree.item(item_id, 'tags') or ()
        except tk.TclError:
            self._hide_tree_hover_popup(clear_item=True)
            return

        if 'subtitle' in tags:
            self._hide_tree_hover_popup(clear_item=True)
            return

        if item_id == getattr(self, '_tree_hover_item_id', None):
            if getattr(self, '_tree_hover_popup_item_id', None) == item_id:
                self._position_tree_hover_popup()
            return

        self._hide_tree_hover_popup(clear_item=False)
        self._tree_hover_item_id = item_id
        self._tree_hover_token += 1
        hover_token = self._tree_hover_token
        self._tree_hover_after_id = self.root.after(
            self._TREE_HOVER_TOOLTIP_DELAY_MS,
            lambda iid=item_id, token=hover_token: self._show_tree_hover_popup(iid, token)
        )

    def _on_tree_hover_leave(self, _event=None):
        self._hide_tree_hover_popup(clear_item=True)

    def _on_tree_hover_cancel(self, _event=None):
        self._hide_tree_hover_popup(clear_item=True)

    def _position_tree_hover_popup(self):
        popup = getattr(self, '_tree_hover_popup', None)
        if popup is None:
            return
        x_root, y_root = getattr(self, '_tree_hover_pointer', (0, 0))
        try:
            popup.geometry(f"+{int(x_root) + 18}+{int(y_root) + 22}")
        except (tk.TclError, TypeError, ValueError):
            pass

    def _ensure_tree_hover_popup(self, item_id, text):
        popup = getattr(self, '_tree_hover_popup', None)
        label = getattr(self, '_tree_hover_popup_label', None)

        if popup is None or label is None:
            try:
                popup = tk.Toplevel(self.root)
                popup.withdraw()
                popup.overrideredirect(True)
                popup.attributes('-topmost', True)
                popup.configure(bg="#fff8d6", padx=1, pady=1)
                label = tk.Label(
                    popup,
                    text=text,
                    justify=tk.LEFT,
                    anchor=tk.W,
                    background="#fff8d6",
                    foreground="#202020",
                    relief=tk.SOLID,
                    borderwidth=1,
                    padx=8,
                    pady=6,
                    font=("Consolas", 9),
                    wraplength=0
                )
                label.pack(fill=tk.BOTH, expand=True)
                self._tree_hover_popup = popup
                self._tree_hover_popup_label = label
            except tk.TclError:
                self._destroy_tree_hover_popup()
                return
        else:
            try:
                label.config(text=text)
            except tk.TclError:
                self._destroy_tree_hover_popup()
                return

        self._tree_hover_popup_item_id = item_id
        self._position_tree_hover_popup()
        try:
            popup.deiconify()
            popup.lift()
        except tk.TclError:
            self._destroy_tree_hover_popup()

    def _get_tree_hover_cached_metadata(self, item_id):
        return {
            'source': self.get_tree_item_meta(item_id, 'source_extra_metadata'),
            'output': self.get_tree_item_meta(item_id, 'output_extra_metadata'),
        }

    def _format_color_range_display(self, color_metadata):
        if not isinstance(color_metadata, dict):
            return "-"
        try:
            from .core_audio_video_ops import get_processing_color_range_with_reason
            interpreted_range, raw_range, _reason = get_processing_color_range_with_reason(color_metadata)
        except Exception:
            raw_range = str(color_metadata.get('color_range') or '').strip().lower()
            interpreted_range = 'full' if raw_range == 'pc' else 'limited' if raw_range == 'tv' else None

        raw_display = raw_range or '-'
        if interpreted_range == 'full':
            return f"{raw_display} / full"
        if interpreted_range == 'limited':
            return f"{raw_display} / limited"
        return raw_display

    def _format_single_hover_metadata_section(self, section_title, hover_metadata):
        if not isinstance(hover_metadata, dict):
            return {
                'section': section_title,
                'rows': [
                    (self._tree_hover_text('hover_meta_resolution'), '-'),
                    (self._tree_hover_text('hover_meta_pix_fmt'), '-'),
                    (self._tree_hover_text('hover_meta_color_space'), '-'),
                    (self._tree_hover_text('hover_meta_color_primaries'), '-'),
                    (self._tree_hover_text('hover_meta_color_transfer'), '-'),
                    (self._tree_hover_text('hover_meta_color_range'), self._tree_hover_text('hover_meta_unavailable')),
                    (self._tree_hover_text('hover_meta_sar'), '-'),
                    (self._tree_hover_text('hover_meta_dar'), '-'),
                    (self._tree_hover_text('hover_meta_rotation'), '-'),
                    (self._tree_hover_text('hover_meta_file_size'), '-'),
                    (self._tree_hover_text('hover_meta_bitrate'), '-'),
                ]
            }
        color_metadata = hover_metadata.get('color_metadata') or {}
        width = hover_metadata.get('width')
        height = hover_metadata.get('height')
        resolution_text = f"{width}x{height}" if width and height else "-"

        sar = color_metadata.get('sample_aspect_ratio') or "-"
        dar = color_metadata.get('display_aspect_ratio') or "-"
        rotation = hover_metadata.get('rotation')
        rotation_text = f"{rotation}\N{DEGREE SIGN}" if rotation is not None else "-"

        file_size_bytes = hover_metadata.get('file_size')
        if file_size_bytes is not None:
            if file_size_bytes >= 1_073_741_824:
                file_size_text = f"{file_size_bytes / 1_073_741_824:.2f} GB"
            elif file_size_bytes >= 1_048_576:
                file_size_text = f"{file_size_bytes / 1_048_576:.1f} MB"
            else:
                file_size_text = f"{file_size_bytes / 1024:.1f} KB"
        else:
            file_size_text = "-"

        bit_rate = hover_metadata.get('bit_rate')
        if bit_rate is not None:
            try:
                br_val = float(bit_rate)
                if br_val >= 1_000_000:
                    bitrate_text = f"{br_val / 1_000_000:.2f} Mbps"
                else:
                    bitrate_text = f"{br_val / 1_000:.0f} kbps"
            except (ValueError, TypeError):
                bitrate_text = "-"
        else:
            bitrate_text = "-"

        return {
            'section': section_title,
            'rows': [
                (self._tree_hover_text('hover_meta_resolution'), resolution_text),
                (self._tree_hover_text('hover_meta_pix_fmt'), color_metadata.get('pix_fmt') or '-'),
                (self._tree_hover_text('hover_meta_color_space'), color_metadata.get('color_space') or '-'),
                (self._tree_hover_text('hover_meta_color_primaries'), color_metadata.get('color_primaries') or '-'),
                (self._tree_hover_text('hover_meta_color_transfer'), color_metadata.get('color_transfer') or '-'),
                (self._tree_hover_text('hover_meta_color_range'), self._format_color_range_display(color_metadata)),
                (self._tree_hover_text('hover_meta_sar'), sar),
                (self._tree_hover_text('hover_meta_dar'), dar),
                (self._tree_hover_text('hover_meta_rotation'), rotation_text),
                (self._tree_hover_text('hover_meta_file_size'), file_size_text),
                (self._tree_hover_text('hover_meta_bitrate'), bitrate_text),
            ]
        }

    def _format_stream_size(self, size_bytes):
        if size_bytes is None:
            return "-"
        if size_bytes >= 1_073_741_824:
            return f"{size_bytes / 1_073_741_824:.2f} GB"
        if size_bytes >= 1_048_576:
            return f"{size_bytes / 1_048_576:.1f} MB"
        if size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KB"
        return f"{size_bytes} B"

    def _format_stream_bitrate(self, bit_rate):
        if bit_rate is None:
            return "-"
        try:
            br_val = float(bit_rate)
            if br_val >= 1_000_000:
                return f"{br_val / 1_000_000:.1f} Mbps"
            return f"{br_val / 1_000:.0f} kbps"
        except (ValueError, TypeError):
            return "-"

    def _format_hover_stream_rows(self, hover_metadata):
        if not isinstance(hover_metadata, dict):
            return []

        streams = hover_metadata.get('streams') or {}
        rows = []

        for stream in streams.get('video') or []:
            codec = str(stream.get('codec') or '-').upper()
            width = stream.get('width')
            height = stream.get('height')
            desc = codec
            if width and height:
                desc = f"{codec} {width}x{height}"
            rows.append(('V', stream.get('index'), '-', '-', '-', stream.get('start_time_ms', 0), self._format_stream_size(stream.get('size_bytes')), self._format_stream_bitrate(stream.get('bit_rate')), desc))

        for stream in streams.get('audio') or []:
            codec = str(stream.get('codec') or '-').upper()
            channels = stream.get('channels')
            title = str(stream.get('title') or '').strip()
            desc = codec
            if channels:
                desc = f"{desc} {channels}ch"
            if title:
                desc = f"{desc} - {title}"
            rows.append((
                'A',
                stream.get('index'),
                stream.get('lang') or 'und',
                'Y' if stream.get('default') else '-',
                'Y' if stream.get('forced') else '-',
                stream.get('start_time_ms', 0),
                self._format_stream_size(stream.get('size_bytes')),
                self._format_stream_bitrate(stream.get('bit_rate')),
                desc
            ))

        for stream in streams.get('subtitle') or []:
            codec = str(stream.get('codec') or '-').upper()
            title = str(stream.get('title') or '').strip()
            desc = codec if not title else f"{codec} - {title}"
            rows.append((
                'S',
                stream.get('index'),
                stream.get('lang') or 'und',
                'Y' if stream.get('default') else '-',
                'Y' if stream.get('forced') else '-',
                stream.get('start_time_ms', 0),
                self._format_stream_size(stream.get('size_bytes')),
                self._format_stream_bitrate(stream.get('bit_rate')),
                desc
            ))

        return rows

    def _truncate_hover_cell(self, value, width):
        text = str(value or "-")
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[:width - 3] + "..."

    def _format_hover_stream_section_lines(self, hover_metadata):
        rows = self._format_hover_stream_rows(hover_metadata)
        desc_width = 26
        size_label = "M\u00e9ret"
        bitrate_label = "Bitr\u00e1ta"
        desc_label = "Le\u00edr\u00e1s"
        header = (
            f"{'T':<1} {'Idx':>3} {'Lang':<5} {'D':<1} {'F':<1} "
            f"{'Off':>6} {size_label:>10} {bitrate_label:>11} {desc_label:<{desc_width}}"
        )
        lines = [header, "-" * len(header)]
        if not rows:
            no_stream_text = "Nincs s\u00e1vadat."
            lines.append(f"{no_stream_text:<{len(header)}}")
            return lines

        for track_type, index, lang, default_flag, forced_flag, offset_ms, size_text, bitrate_text, desc in rows:
            idx_text = "-" if index in (None, "") else str(index)
            lang_text = self._truncate_hover_cell(lang or 'und', 5)
            try:
                offset_text = str(int(round(float(offset_ms or 0))))
            except (TypeError, ValueError):
                offset_text = "0"
            desc_display = self._truncate_hover_cell(desc, desc_width)
            lines.append(
                f"{track_type:<1} {idx_text:>3} {lang_text:<5} {default_flag:<1} {forced_flag:<1} "
                f"{offset_text:>6} {str(size_text):>10} {str(bitrate_text):>11} {desc_display:<{desc_width}}"
            )
        return lines

    def _format_tree_hover_metadata_text(self, video_path, hover_metadata):
        if not isinstance(hover_metadata, dict):
            return f"{self._tree_hover_text('hover_meta_title')}\n{Path(video_path).name}\n\n{self._tree_hover_text('hover_meta_unavailable')}"

        source_metadata = hover_metadata.get('source')
        output_metadata = hover_metadata.get('output')
        source_section = self._format_single_hover_metadata_section(self._tree_hover_text('hover_meta_source_section'), source_metadata)
        output_section = self._format_single_hover_metadata_section(
            self._tree_hover_text('hover_meta_output_section'),
            output_metadata if output_metadata else None
        )

        lines = [
            self._tree_hover_text('hover_meta_title'),
            str(Path(video_path).name),
            "",
        ]

        source_header = source_section['section']
        output_header = output_section['section']
        label_width = max(
            [len(label) for label, _value in source_section['rows']] +
            [len(label) for label, _value in output_section['rows']] +
            [len(source_header), len(output_header)]
        )
        value_width = max(
            [len(str(value)) for _label, value in source_section['rows']] +
            [len(str(value)) for _label, value in output_section['rows']] +
            [len(self._tree_hover_text('hover_meta_unavailable'))]
        )

        source_stream_lines = self._format_hover_stream_section_lines(source_metadata)
        output_stream_lines = self._format_hover_stream_section_lines(output_metadata)
        source_stream_title = f"{source_section['section']} | Sávok"
        output_stream_title = f"{output_section['section']} | Sávok"
        stream_panel_width = max(
            [len(source_stream_title), len(output_stream_title)] +
            [len(line) for line in source_stream_lines] +
            [len(line) for line in output_stream_lines]
        )
        block_width = max(
            label_width + value_width + 2,
            len(source_header),
            len(output_header),
            stream_panel_width
        )

        def append_metadata_block(section):
            lines.append(str(section['section']))
            lines.append("-" * block_width)
            for label, value in section['rows']:
                cell = f"{label + ':':<{label_width + 1}} {str(value):<{value_width}}"
                lines.append(f"{cell:<{block_width}}".rstrip())

        def append_stream_block(title, stream_lines):
            lines.append("")
            lines.append(str(title))
            lines.append("-" * block_width)
            for stream_line in stream_lines:
                lines.append(f"{stream_line:<{block_width}}".rstrip())

        append_metadata_block(source_section)
        append_stream_block(source_stream_title, source_stream_lines)
        lines.append("")
        append_metadata_block(output_section)
        append_stream_block(output_stream_title, output_stream_lines)
        return "\n".join(lines)

    def _show_tree_hover_popup(self, item_id, hover_token):
        self._tree_hover_after_id = None

        if item_id != getattr(self, '_tree_hover_item_id', None):
            return
        if hover_token != getattr(self, '_tree_hover_token', None):
            return

        video_path = self._get_video_path_by_item(item_id)
        if not video_path:
            return

        cached_metadata = self._get_tree_hover_cached_metadata(item_id)
        output_file = self.video_to_output.get(video_path)
        output_exists = bool(output_file and Path(output_file).exists())
        source_ready = bool(cached_metadata and cached_metadata.get('source'))
        source_complete = (
            source_ready and
            isinstance(cached_metadata.get('source'), dict) and
            cached_metadata['source'].get('file_size') is not None and
            hasattr(self, '_extra_metadata_has_size_and_bitrate') and
            self._extra_metadata_has_size_and_bitrate(cached_metadata['source'])
        )
        output_ready = (
            not output_exists or
            (
                cached_metadata and
                hasattr(self, '_extra_metadata_has_any_stream_rows') and
                self._extra_metadata_has_any_stream_rows(cached_metadata.get('output'))
            )
        )
        output_complete = (
            output_ready and output_exists and
            isinstance(cached_metadata.get('output'), dict) and
            cached_metadata['output'].get('file_size') is not None and
            hasattr(self, '_extra_metadata_has_size_and_bitrate') and
            self._extra_metadata_has_size_and_bitrate(cached_metadata['output'])
        )
        if source_ready and output_ready and source_complete and (not output_exists or output_complete):
            self._ensure_tree_hover_popup(item_id, self._format_tree_hover_metadata_text(video_path, cached_metadata))
            return

        self._ensure_tree_hover_popup(
            item_id,
            f"{self._tree_hover_text('hover_meta_title')}\n{Path(video_path).name}\n\n{self._tree_hover_text('hover_meta_loading')}"
        )

        probe_key = str(item_id)
        if probe_key in self._tree_hover_probe_inflight:
            return
        self._tree_hover_probe_inflight.add(probe_key)

        def _worker():
            result = None
            error_text = None
            probe_registration = None
            try:
                from .core_audio_video_ops import get_video_extra_metadata
                probe_targets = [video_path]
                if output_file and Path(output_file).exists():
                    probe_targets.append(output_file)

                if hasattr(self, '_try_begin_metadata_probe_activity'):
                    probe_allowed, probe_registration = self._try_begin_metadata_probe_activity(*probe_targets)
                    if not probe_allowed:
                        raise RuntimeError("hover metadata probe skipped because the file is pending delete/re-encode")

                result = {
                    'source': get_video_extra_metadata(video_path),
                    'output': get_video_extra_metadata(output_file) if output_file and Path(output_file).exists() else None,
                }
            except Exception as hover_error:
                error_text = str(hover_error)
            finally:
                if probe_registration and hasattr(self, '_finish_metadata_probe_activity'):
                    self._finish_metadata_probe_activity(*probe_registration)
                self._tree_hover_metadata_queue.put((item_id, hover_token, video_path, result, error_text))

        threading.Thread(target=_worker, daemon=True, name="TreeHoverMetadataProbe").start()

    def _poll_tree_hover_metadata_queue(self):
        try:
            while True:
                item_id, hover_token, video_path, result, error_text = self._tree_hover_metadata_queue.get_nowait()
                self._tree_hover_probe_inflight.discard(str(item_id))

                if result:
                    self.set_tree_item_meta(
                        item_id,
                        source_extra_metadata=result.get('source'),
                        output_extra_metadata=result.get('output')
                    )
                    if hasattr(self, 'update_single_video_extra_metadata_in_db'):
                        self._start_db_thread(
                            lambda vp=video_path, src=result.get('source'), out=result.get('output'): self.update_single_video_extra_metadata_in_db(
                                vp, source_extra_metadata=src, output_extra_metadata=out
                            ),
                            name="HoverExtraMetadataDB",
                            daemon=True
                        )
                else:
                    self.set_tree_item_meta(item_id, hover_metadata_error=error_text or "unknown")

                if item_id == getattr(self, '_tree_hover_item_id', None) and hover_token == getattr(self, '_tree_hover_token', None):
                    if result:
                        text = self._format_tree_hover_metadata_text(video_path, result)
                    else:
                        text = (
                            f"{self._tree_hover_text('hover_meta_title')}\n{Path(video_path).name}\n\n"
                            f"{self._tree_hover_text('hover_meta_unavailable')}\n{error_text or '-'}"
                        )
                    self._ensure_tree_hover_popup(item_id, text)
        except queue.Empty:
            pass

        try:
            self.root.after(150, self._poll_tree_hover_metadata_queue)
        except tk.TclError:
            pass

    def get_tree_values(self, item_id, min_length=9):
        """Get Tree values and extend if necessary.
        
        Args:
            item_id: The tree item ID.
            min_length: Minimum length of returned values list.
            
        Returns:
            list: List of values, extended with empty strings if needed.
                  Returns list of empty strings if item doesn't exist.
        """
        try:
            item_data = self.tree.item(item_id)
            if not item_data or 'values' not in item_data:
                return [''] * min_length
            current_values = list(item_data['values'])
            if len(current_values) < min_length:
                current_values.extend([''] * (min_length - len(current_values)))
            return current_values
        except (tk.TclError, KeyError, AttributeError):
            return [''] * min_length


    def _get_video_path_by_item(self, item_id):
        """Get video path associated with a tree item ID.
        
        Args:
            item_id: The tree item ID.
            
        Returns:
            Path or None: The video path if found, else None.
        """
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                return video_path
        return None

    def _show_hidden_item_if_needed(self, item_id):
        """Reattaches a previously hidden item when its status is no longer completed."""
        if item_id not in getattr(self, 'hidden_items', set()):
            return
        if not hasattr(self, 'tree'):
            return
        video_path = self._get_video_path_by_item(item_id)
        if not video_path:
            return
        try:
            id_to_video_path = {vid: path for path, vid in self.video_items.items()}
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
            self.hidden_items.discard(item_id)
        except (tk.TclError, KeyError, AttributeError, ValueError):
            pass

    def on_column_resize(self, event):
        """Handle column resize events to sync widths with summary table.
        
        Args:
            event: The event object.
        """
        tree = event.widget
        region = tree.identify_region(event.x, event.y)
        if region == "separator":
            self.root.after(10, self.sync_column_widths)

    def sync_column_widths(self):
        """Synchronize column widths between main tree and summary tree."""
        for col in ['#0'] + list(self.tree['columns']):
            width = self.tree.column(col, 'width')
            self.col_widths[col] = width
            # Keep left alignment for file size columns
            if col in ("orig_size", "new_size", "size_change"):
                self.summary_tree.column(col, width=width, anchor=tk.W)
            else:
                self.summary_tree.column(col, width=width)

    def _sort_tree_by_order_num(self):
        """Sort TreeView items by order_num (ABC order)."""
        try:
            # Get all main items (children are not sorted separately)
            items = []
            for item_id in self.tree.get_children():
                tags = self.tree.item(item_id, 'tags')
                if 'subtitle' not in tags:  # Only sort videos, not subtitles
                    items.append(item_id)
            
            # Sort by order_num (text field contains order_num)
            def get_order_key(item_id):
                try:
                    text = self.tree.item(item_id, 'text')
                    return int(text) if text.isdigit() else 999999
                except (ValueError, TypeError, tk.TclError):
                    return 999999
            
            items.sort(key=get_order_key)
            
            # Re-insert in sorted order
            for item_id in items:
                self.tree.move(item_id, "", tk.END)
        except Exception as e:
            # Silent error, not logging because this is just sorting
            pass

    def sort_by_column(self, column):
        """Sort by column (toggle A-Z / Z-A)."""
        # If clicked on the same column, reverse sorting
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False
        
        # Sort key function
        def get_sort_key(item_id):
            video_path = None
            for vp, vid_id in self.video_items.items():
                if vid_id == item_id:
                    video_path = vp
                    break
            
            if not video_path:
                return (999999,) if column == "#0" else ("", 999999)
            
            video_order = getattr(self, 'video_order', {})
            order_num = video_order.get(video_path, 999999)
            
            if column == "#0":
                # Sort by order number - only order number matters
                return (order_num,)
            
            # Sort by other columns - primary is column, secondary is order number
            values = self.tree.item(item_id, 'values')
            
            if column == "video_name":
                video_name = values[self.COLUMN_INDEX['video_name']] if len(values) > self.COLUMN_INDEX['video_name'] else ""
                return (video_name.lower() if video_name else "", order_num)
            elif column == "status":
                status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
                return (status.lower(), order_num)
            elif column == "denoise":
                denoise = values[self.COLUMN_INDEX['denoise']] if len(values) > self.COLUMN_INDEX['denoise'] else ""
                # Sort by presence of checkmark (descending by default for checkmarks usually, but here string sort works: "[OK]" > "")
                return (denoise, order_num)
            elif column == "hard_rotate":
                hard_rotate = values[self.COLUMN_INDEX['hard_rotate']] if len(values) > self.COLUMN_INDEX['hard_rotate'] else "0"
                return (normalize_hard_rotate_degrees(hard_rotate), order_num)
            elif column == "cq":
                cq = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else ""
                try:
                    # Handle localized number
                    cq_normalized = normalize_number_string(cq) if cq != "-" else "-"
                    cq_val = float(cq_normalized) if cq_normalized != "-" else 999999
                except (ValueError, TypeError):
                    cq_val = 999999
                return (cq_val, order_num)
            elif column == "vmaf":
                vmaf = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else ""
                try:
                    # Handle localized number
                    vmaf_normalized = normalize_number_string(vmaf) if vmaf != "-" else "-"
                    vmaf_val = float(vmaf_normalized) if vmaf_normalized != "-" else 0
                except (ValueError, TypeError):
                    vmaf_val = 0
                return (vmaf_val, order_num)
            elif column == "psnr":
                psnr = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else ""
                try:
                    # Handle localized number
                    psnr_normalized = normalize_number_string(psnr) if psnr != "-" else "-"
                    psnr_val = float(psnr_normalized) if psnr_normalized != "-" else 0
                except (ValueError, TypeError):
                    psnr_val = 0
                return (psnr_val, order_num)
            elif column == "progress":
                progress = values[self.COLUMN_INDEX['progress']] if len(values) > self.COLUMN_INDEX['progress'] else ""
                return (progress.lower(), order_num)
            elif column == "orig_size":
                orig_size = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else ""
                try:
                    # Handle localized number - extract number from "XXX.X MB" format
                    if orig_size != "-":
                        size_str = orig_size.replace(" MB", "").strip()
                        size_normalized = normalize_number_string(size_str)
                        size_val = float(size_normalized) if size_normalized != "-" else 0
                    else:
                        size_val = 0
                except (ValueError, TypeError, AttributeError):
                    size_val = 0
                return (size_val, order_num)
            elif column == "new_size":
                new_size = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else ""
                try:
                    # Handle localized number - extract number from "XXX.X MB" format
                    if new_size != "-":
                        size_str = new_size.replace(" MB", "").strip()
                        size_normalized = normalize_number_string(size_str)
                        size_val = float(size_normalized) if size_normalized != "-" else 0
                    else:
                        size_val = 0
                except (ValueError, TypeError, AttributeError):
                    size_val = 0
                return (size_val, order_num)
            elif column == "size_change":
                size_change = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else ""
                try:
                    # Handle localized number - extract number from "±XX.X%" format
                    if size_change != "-":
                        # Remove % sign and +/- signs
                        clean_val = size_change.replace("%", "").replace("+", "").strip()
                        change_normalized = normalize_number_string(clean_val)
                        change_val = float(change_normalized) if change_normalized != "-" else 0
                    else:
                        change_val = 0
                except (ValueError, TypeError, AttributeError):
                    change_val = 0
                return (change_val, order_num)
            elif column == "duration":
                duration = values[self.COLUMN_INDEX['duration']] if len(values) > self.COLUMN_INDEX['duration'] else ""
                parsed_seconds = parse_duration_cell_source_seconds(duration)
                duration_seconds = parsed_seconds if parsed_seconds is not None else 0
                return (duration_seconds, order_num)
            elif column == "frames":
                frames = values[self.COLUMN_INDEX['frames']] if len(values) > self.COLUMN_INDEX['frames'] else ""
                parsed_frames = parse_frames_cell_source_count(frames)
                frames_val = parsed_frames if parsed_frames is not None else 0
                return (frames_val, order_num)
            elif column == "completed_date":
                completed_date = values[self.COLUMN_INDEX['completed_date']] if len(values) > self.COLUMN_INDEX['completed_date'] else ""
                return (completed_date.lower(), order_num)
            
            return ("", order_num)
        
        # Get all main items (children are not sorted separately)
        items = []
        for item_id in self.tree.get_children():
            tags = self.tree.item(item_id, 'tags')
            if 'subtitle' not in tags:  # Only sort videos, not subtitles
                items.append(item_id)
        
        # Sorting
        items.sort(key=get_sort_key, reverse=self.sort_reverse)
        
        # Re-insert in sorted order
        for item_id in items:
            self.tree.move(item_id, "", tk.END)
        
        # Update header (arrow indication) - column names remain, only adding arrow
        for col in ['#0'] + list(self.tree['columns']):
            if col == column:
                arrow = " ↓" if self.sort_reverse else " ↑"
            else:
                arrow = ""
            # Get column names from t() function to remain localized
            heading_text_map = {
                "#0": t('column_order'),
                "denoise": t('column_denoise'),
                "hard_rotate": t('column_hard_rotate'),
                "video_name": t('column_video'),
                "status": t('column_status'),
                "cq": t('column_cq'),
                "vmaf": t('column_vmaf'),
                "psnr": t('column_psnr'),
                "progress": t('column_progress'),
                "orig_size": t('column_orig_size'),
                "new_size": t('column_new_size'),
                "size_change": t('column_size_change'),
                "duration": t('column_duration'),
                "frames": t('column_frames'),
                "completed_date": t('column_completed')
            }
            heading_text = heading_text_map.get(col, "")
            self.tree.heading(col, text=heading_text + arrow)

    def clear_table(self):
        """Clear table completely."""
        result = messagebox.askyesno(
            t('btn_clear_table'),
            t('msg_clear_confirm')
        )
        
        if not result:
            return
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.video_items.clear()
        self.subtitle_items.clear()
        self.video_to_output.clear()
        self.video_files.clear()
        if hasattr(self, 'video_hard_rotate_degrees'):
            self.video_hard_rotate_degrees.clear()
        self.tree_item_data.clear()  # Clear data behind Tree item
        self.video_stat_cache.clear()  # Clear stat cache
        
        # Cleanup stop events for removed videos
        with self.video_stop_events_lock:
            self.video_stop_events.clear()
        
        # Delete database
        if self.db_path.exists():
            try:
                self.db_path.unlink()
            except (OSError, PermissionError, FileNotFoundError):
                pass
        
        self.update_summary_row()
        self.status_label.config(text=t('btn_clear_table'))

    def update_summary_row(self):
        """Update the summary row with total stats."""
        total_orig_size_bytes = 0
        total_new_size_bytes = 0
        encoded_count = 0
        
        for video_path, item_id in self.video_items.items():
            values = self.tree.item(item_id)['values']
            status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
            if is_status_completed(status):
                try:
                    # Parse size strings to bytes (handles MB/GB/TB automatically via parse_size_to_bytes)
                    orig_size_str = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-"
                    new_size_str = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else "-"
                    
                    orig_bytes = parse_size_to_bytes(orig_size_str)
                    new_bytes = parse_size_to_bytes(new_size_str)
                    
                    if orig_bytes is not None:
                        total_orig_size_bytes += orig_bytes
                    if new_bytes is not None:
                        total_new_size_bytes += new_bytes
                    
                    if orig_bytes is not None or new_bytes is not None:
                        encoded_count += 1
                except (ValueError, TypeError, AttributeError, IndexError):
                    pass
        
        for item in self.summary_tree.get_children():
            self.summary_tree.delete(item)
        
        if encoded_count > 0 and total_orig_size_bytes > 0:
            # Show summary row if there are completed videos
            self.summary_frame.pack(fill=tk.X)
            change_percent = ((total_new_size_bytes - total_orig_size_bytes) / total_orig_size_bytes) * 100 if total_orig_size_bytes > 0 else 0
            orig_size_str = format_size_auto(total_orig_size_bytes)
            new_size_str = format_size_auto(total_new_size_bytes)
            change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
            self.summary_tree.insert("", tk.END, text="Σ",
                values=("", "", f"━━━━ SUMMARY ({encoded_count} videos) ━━━━", "", "", "", "", "", orig_size_str, new_size_str, change_percent_str, "", "", ""), tags=("summary",))
        else:
            # Hide summary row if no completed videos
            self.summary_frame.pack_forget()
