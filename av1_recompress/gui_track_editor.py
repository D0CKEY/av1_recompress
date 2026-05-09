
from .gui_imports import *
import threading
from pathlib import Path
import time
import traceback

from .i18n import t
# from .gui_shared import ToolTip, set_transient_window_style
import queue
from .core_audio_video_ops import get_video_streams_for_editor, remux_video_with_track_selection
from .core_paths_tools_logging import get_log_writer
# from .core_paths_tools_logging import ConsoleLogger

class SimpleLogger:
    """Thread-safe logger using a queue, specifically for track editor console."""
    def __init__(self, msg_queue):
        self.msg_queue = msg_queue

    def write(self, message):
        if message:
            self.msg_queue.put(message)

    def flush(self):
        pass


def parse_subtitle_filename(filename):
    """Parse subtitle filename to extract language code and detect 'forced' flag.

    Recognizes patterns like:
    - Movie.hun.srt -> (hun, "Hungarian")
    - Movie.eng.forced.srt -> (eng, "English Forced")
    - Movie.hu.srt -> (hu, "Hungarian")
    - Movie.spa.ass -> (spa, "Spanish")

    Returns:
        tuple: (language_code, suggested_title) or (None, None) if not detected
    """
    import re

    # Common language codes mapping (ISO 639-1 and ISO 639-2/3)
    language_names = {
        # ISO 639-1 (2-letter)
        'hu': 'Hungarian', 'en': 'English', 'de': 'German', 'fr': 'French',
        'es': 'Spanish', 'it': 'Italian', 'pl': 'Polish', 'cs': 'Czech',
        'sk': 'Slovak', 'ru': 'Russian', 'ja': 'Japanese', 'zh': 'Chinese',
        'ko': 'Korean', 'ar': 'Arabic', 'pt': 'Portuguese', 'nl': 'Dutch',
        'sv': 'Swedish', 'da': 'Danish', 'fi': 'Finnish', 'no': 'Norwegian',
        'tr': 'Turkish', 'el': 'Greek', 'he': 'Hebrew', 'ro': 'Romanian',
        # ISO 639-2/3 (3-letter)
        'hun': 'Hungarian', 'eng': 'English', 'ger': 'German', 'deu': 'German',
        'fre': 'French', 'fra': 'French', 'spa': 'Spanish', 'ita': 'Italian',
        'pol': 'Polish', 'cze': 'Czech', 'ces': 'Czech', 'slo': 'Slovak',
        'slk': 'Slovak', 'rus': 'Russian', 'jpn': 'Japanese', 'chi': 'Chinese',
        'zho': 'Chinese', 'kor': 'Korean', 'ara': 'Arabic', 'por': 'Portuguese',
        'dut': 'Dutch', 'nld': 'Dutch', 'swe': 'Swedish', 'dan': 'Danish',
        'fin': 'Finnish', 'nor': 'Norwegian', 'tur': 'Turkish', 'gre': 'Greek',
        'ell': 'Greek', 'heb': 'Hebrew', 'rum': 'Romanian', 'ron': 'Romanian',
    }

    # Remove extension
    name_without_ext = Path(filename).stem

    # Split by dots
    parts = name_without_ext.lower().split('.')

    # Look for language code (2-3 letter codes)
    detected_lang = None
    is_forced = False

    for i, part in enumerate(parts):
        # Check if this part is 'forced'
        if part in ('forced', 'kenyszeritett'):
            is_forced = True
            continue

        # Check if this part is a language code (2-3 lowercase letters)
        if re.match(r'^[a-z]{2,3}$', part) and part in language_names:
            detected_lang = part
            break

    if not detected_lang:
        return (None, None)

    # Generate title
    language_name = language_names.get(detected_lang, detected_lang.upper())
    if is_forced:
        title = f"{language_name} Forced"
    else:
        title = language_name

    return (detected_lang, title)


class LanguageInputDialog(tk.Toplevel):
    """Modal dialog for entering language code and title for external subtitles."""

    def __init__(self, parent, subtitle_filename, suggested_language=None, suggested_title=None):
        super().__init__(parent)
        self.parent = parent
        self.subtitle_filename = subtitle_filename
        self.suggested_language = suggested_language
        self.suggested_title = suggested_title
        self.result = None  # Will be (lang_code, title) or None if cancelled

        self.title(t('lang_input_title'))
        self.geometry("450x300")  # Increased height to show buttons
        self.resizable(False, False)

        # Modal dialog setup
        self.transient(parent)
        self.grab_set()

        self._create_widgets()

        # Center on screen
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')

        # Focus on language entry
        self.lang_entry.focus_set()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # File name label
        file_label = ttk.Label(main_frame, text=t('lang_input_file_label').format(filename=self.subtitle_filename),
                               font=("Segoe UI", 9, "bold"), wraplength=400)
        file_label.pack(anchor=tk.W, pady=(0, 15))

        # Language code section
        lang_frame = ttk.Frame(main_frame)
        lang_frame.pack(fill=tk.X, pady=(0, 5))

        ttk.Label(lang_frame, text=t('lang_input_lang_label')).pack(anchor=tk.W)
        self.lang_entry = ttk.Entry(lang_frame, width=10)
        self.lang_entry.pack(anchor=tk.W, pady=(5, 0))

        # Pre-fill suggested language if provided
        if self.suggested_language:
            self.lang_entry.insert(0, self.suggested_language)
            self.lang_entry.select_range(0, tk.END)  # Select all for easy replacement

        # Examples label
        examples_label = ttk.Label(lang_frame, text=t('lang_input_lang_examples'),
                                   font=("Segoe UI", 8), foreground="gray")
        examples_label.pack(anchor=tk.W, pady=(2, 0))

        # Title section
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill=tk.X, pady=(15, 0))

        ttk.Label(title_frame, text=t('lang_input_title_label')).pack(anchor=tk.W)
        self.title_entry = ttk.Entry(title_frame, width=40)
        self.title_entry.pack(anchor=tk.W, pady=(5, 0))

        # Pre-fill suggested title if provided
        if self.suggested_title:
            self.title_entry.insert(0, self.suggested_title)

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, pady=(20, 0))

        ttk.Button(button_frame, text="OK", command=self._on_ok, width=10).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(button_frame, text=t('track_editor_btn_cancel'), command=self._on_cancel, width=10).pack(side=tk.LEFT)

        # Bind Enter key to OK
        self.lang_entry.bind('<Return>', lambda e: self._on_ok())
        self.title_entry.bind('<Return>', lambda e: self._on_ok())

    def _validate_language(self, lang_str):
        """Validate and normalize language code.

        Returns:
            Normalized language code or None if invalid
        """
        import re

        lang = lang_str.strip().lower()

        # Check format: 2-3 lowercase ASCII letters
        if not re.match(r'^[a-z]{2,3}$', lang):
            return None

        # Normalize using core function
        try:
            from .core_subtitles_and_metadata import normalize_language_code
            normalized = normalize_language_code(lang)
            return normalized
        except Exception:
            # Fallback: return as-is if normalization fails
            return lang

    def _on_ok(self):
        lang = self.lang_entry.get().strip().lower()
        title = self.title_entry.get().strip()

        # Validate language code
        if not lang:
            messagebox.showerror(
                t('track_editor_error'),
                t('lang_input_error_empty'),
                parent=self
            )
            return

        # Validate format
        normalized = self._validate_language(lang)
        if not normalized:
            messagebox.showerror(
                t('track_editor_error'),
                t('lang_input_error_invalid'),
                parent=self
            )
            return

        # Warn if unknown language code
        if normalized == 'und' and lang != 'und':
            response = messagebox.askyesno(
                t('track_editor_warning'),
                t('lang_input_warning_unknown').format(lang=lang),
                parent=self
            )
            if not response:
                return
            # Use original code if user confirms
            normalized = lang

        # Set result and close
        self.result = (normalized, title)
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


class TrackEditorDialog(tk.Toplevel):
    def __init__(self, parent, video_path, source_path=None, item_id=None, on_completion=None):
        super().__init__(parent)
        self.parent = parent
        self.video_path = Path(video_path)
        self.source_path = Path(source_path) if source_path else None
        self.item_id = item_id
        self.on_completion = on_completion

        self.title(t('track_editor_title'))
        self.geometry("1000x600")
        self.minsize(600, 500)
        
        # Setup styles
        self.style = ttk.Style()
        self.style.configure("Mismatch.TLabel", foreground="#ff5555")

        # Ablak stílus (hasonló a többihez)
        try:
            if hasattr(self, 'iconbitmap'):
                pass
        except (OSError, AttributeError, tk.TclError):
            pass

        self.style.configure("Loading.TLabel", font=("Segoe UI", 10))

        # Adatok betöltése - kimeneti fájl GYORSAN (size/bitrate később)
        self.streams = get_video_streams_for_editor(self.video_path, quick=True)
        if not self.streams:
            messagebox.showerror(t('track_editor_error'), t('track_editor_read_error'), parent=self)
            self.destroy()
            return

        self.source_streams = None
        if self.source_path and self.source_path.exists():
            self.source_streams = get_video_streams_for_editor(self.source_path, quick=True)

        self.video_track = self.streams.get('video')
        self.audio_tracks = self.streams.get('audio', [])
        self.subtitle_tracks = self.streams.get('subtitle', [])

        self.user_offsets = {}
        self.original_offsets = {}
        
        self.pending_conversions = []
        self.next_virtual_iid = 10000
        self.next_external_subtitle_iid = 20000
        self.external_subtitles = []

        self._track_editor_cache = None
        self._cache_fully_covered = False

        _log = get_log_writer()
        if _log:
            try:
                _log.write(f"\n[TRACK_EDITOR] Megnyitás: {self.video_path.name}\n")
                _log.write(f"[TRACK_EDITOR] Output quick load: {len(self.audio_tracks)} audio, {len(self.subtitle_tracks)} subtitle sáv\n")
                if self.source_path and self.source_path.exists():
                    src_audio = len(self.source_streams.get('audio', [])) if self.source_streams else 0
                    src_sub = len(self.source_streams.get('subtitle', [])) if self.source_streams else 0
                    _log.write(f"[TRACK_EDITOR] Source quick load: {src_audio} audio, {src_sub} subtitle sáv ({self.source_path.name})\n")
                for at in self.audio_tracks:
                    idx = at.get('index', '?')
                    codec = at.get('codec', '?')
                    lang = at.get('lang', 'und')
                    ch = at.get('channels', '?')
                    sz = at.get('size_mb')
                    br = at.get('bit_rate')
                    sz_str = f"{sz:.1f} MB" if sz is not None else "N/A"
                    br_str = f"{int(float(br)/1000)} kbps" if br else "N/A"
                    _log.write(f"[TRACK_EDITOR]   Audio #{idx}: {codec} {ch}ch lang={lang} size={sz_str} bitrate={br_str}\n")
                for st in self.subtitle_tracks:
                    idx = st.get('index', '?')
                    codec = st.get('codec', '?')
                    lang = st.get('lang', 'und')
                    sz = st.get('size_mb')
                    sz_str = f"{sz*1024:.1f} KB" if sz is not None else "N/A"
                    _log.write(f"[TRACK_EDITOR]   Subtitle #{idx}: {codec} lang={lang} size={sz_str}\n")
                _log.flush()
            except Exception:
                pass

        try:
            app_ref = parent
            while app_ref and not hasattr(app_ref, 'get_track_editor_cache_from_db'):
                app_ref = getattr(app_ref, 'master', None) or getattr(app_ref, 'parent', None)
            if app_ref and hasattr(app_ref, 'get_track_editor_cache_from_db'):
                cache = app_ref.get_track_editor_cache_from_db(
                    self.source_path or self.video_path,
                    output_path=self.video_path
                )
                if cache:
                    self._track_editor_cache = cache
                    self._apply_cached_track_info(cache)
                    if _log:
                        try:
                            _log.write(f"[TRACK_EDITOR] Cache TALÁLHATÓ -> sávinfók (size/bitrate) DB-ből betöltve\n")
                            _log.flush()
                        except Exception:
                            pass
                else:
                    if _log:
                        try:
                            _log.write(f"[TRACK_EDITOR] Cache NEM található -> lassú ffprobe beolvasás indul\n")
                            _log.flush()
                        except Exception:
                            pass
        except Exception:
            if _log:
                try:
                    _log.write(f"[TRACK_EDITOR] Cache lekérdezés hiba -> lassú ffprobe beolvasás indul\n")
                    _log.flush()
                except Exception:
                    pass

        self._create_widgets()
        
        if self._cache_fully_covered:
            if _log:
                try:
                    _log.write(f"[TRACK_EDITOR] Cache TELJESEN lefedi az összes sávot -> lazy loading KIHAGYVA\n")
                    for at in self.audio_tracks:
                        idx = at.get('index', '?')
                        sz = at.get('size_mb')
                        br = at.get('bit_rate')
                        sz_str = f"{sz:.1f} MB" if sz is not None else "N/A"
                        br_str = f"{int(float(br)/1000)} kbps" if br else "N/A"
                        _log.write(f"[TRACK_EDITOR]   Audio #{idx}: size={sz_str} bitrate={br_str} (cache)\n")
                    for st in self.subtitle_tracks:
                        idx = st.get('index', '?')
                        sz = st.get('size_mb')
                        sz_str = f"{sz*1024:.1f} KB" if sz is not None else "N/A"
                        _log.write(f"[TRACK_EDITOR]   Subtitle #{idx}: size={sz_str} (cache)\n")
                    _log.flush()
                except Exception:
                    pass
        
        # Start lazy size/bitrate loading in background (skip if cache fully covered)
        if not self._cache_fully_covered:
            self._start_lazy_track_info_loading()
        
        # Center window relative to parent
        self.transient(parent)
        self.grab_set()
        
        def _on_parent_focus(event):
            try:
                if self.winfo_exists():
                    self.lift()
                    self.focus_force()
            except tk.TclError:
                pass
        parent.bind('<FocusIn>', _on_parent_focus, add='+')
        
        # Center on screen
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')
        
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        title_label = ttk.Label(main_frame, text=t('track_editor_file_label').format(filename=self.video_path.name), font=("Segoe UI", 10))
        title_label.pack(anchor=tk.W, pady=(0, 15))

        # --- Video Section ---
        ttk.Label(main_frame, text=t('track_editor_video_label'), font=("Segoe UI", 9, "bold")).pack(anchor=tk.W, pady=(0, 5))

        video_frame = ttk.Frame(main_frame)
        video_frame.pack(fill=tk.X, pady=(0, 15))

        if self.video_track:
            # Video info label
            video_info = t('track_editor_video_info').format(
                index=self.video_track.get('index', 0),
                codec=self.video_track.get('codec', '*').upper(),
                width=self.video_track.get('width', 0),
                height=self.video_track.get('height', 0)
            )
            video_bitrate = None
            try:
                from .core_audio_video_ops import get_video_track_size_bytes, get_video_info
                vp = self.video_path if hasattr(self, 'video_path') else None
                if vp and vp.exists():
                    video_bytes = get_video_track_size_bytes(vp)
                    if video_bytes and video_bytes > 0:
                        dur, _ = get_video_info(vp)
                        if dur and dur > 0:
                            video_bitrate = str(int((video_bytes * 8) / dur))
            except Exception:
                pass
            if video_bitrate:
                try:
                    br_mbps = float(video_bitrate) / 1_000_000
                    video_info += f"  |  {br_mbps:.1f} Mbps"
                except (ValueError, TypeError):
                    pass
            ttk.Label(video_frame, text=video_info).pack(side=tk.LEFT, padx=(0, 20))

            # Current video offset from output file
            current_video_offset_ms = self.video_track.get('start_time_ms', 0)

            # Original video offset from SOURCE file (for reference display)
            self.original_video_offset_ms = None
            if self.source_streams and self.source_streams.get('video'):
                self.original_video_offset_ms = self.source_streams['video'].get('start_time_ms', 0)

            # Video offset spinbox
            ttk.Label(video_frame, text=t('track_editor_col_offset') + ":").pack(side=tk.LEFT, padx=(0, 5))
            self.video_offset_var = tk.StringVar(value=str(current_video_offset_ms))
            
            # Trace changes to update color immediately
            self.video_offset_var.trace_add("write", self._update_video_visuals)
            
            self.video_offset_spinbox = ttk.Spinbox(
                video_frame, from_=-999999, to=999999, width=10,
                textvariable=self.video_offset_var
            )
            self.video_offset_spinbox.pack(side=tk.LEFT)
            
            # Show original offset from SOURCE file in parentheses
            original_display = str(self.original_video_offset_ms) if self.original_video_offset_ms is not None else "-"
            # Use ttk.Label with custom style for color modification
            self.video_offset_label = ttk.Label(video_frame, text=f"ms ({original_display})")
            self.video_offset_label.pack(side=tk.LEFT, padx=(2, 0))
            
            # Initial visual state update
            self.after(50, lambda: self._update_video_visuals())

            # --- Video Rotation Combobox (relative mode) ---
            ttk.Label(video_frame, text="  |  ").pack(side=tk.LEFT)

            # Show current rotation in label, normalize to 0-359
            current_rotation = self.video_track.get('rotation', 0)
            if current_rotation < 0:
                current_rotation = (current_rotation + 360) % 360
            self._current_rotation = current_rotation

            ttk.Label(video_frame, text=t('track_editor_rotation_label').format(degrees=current_rotation) + ":").pack(side=tk.LEFT, padx=(0, 5))

            rotation_options = [
                t('track_editor_rotation_normal'),    # index 0 → delta 0°
                t('track_editor_rotation_right90'),   # index 1 → delta +90° CW
                t('track_editor_rotation_180'),       # index 2 → delta +180°
                t('track_editor_rotation_left90'),    # index 3 → delta +270° (= -90° CCW)
            ]
            self.rotation_var = tk.StringVar()
            self.rotation_combo = ttk.Combobox(
                video_frame, textvariable=self.rotation_var,
                values=rotation_options, state='readonly', width=14
            )
            self.rotation_combo.pack(side=tk.LEFT)
            self.rotation_combo.current(0)  # Always start at "Unchanged"
        else:
            ttk.Label(video_frame, text="No video stream found").pack(side=tk.LEFT)
            self.video_offset_var = tk.StringVar(value="0")
            self.original_video_offset_ms = 0
            self.rotation_combo = None

        # --- Audio Section ---
        ttk.Label(main_frame, text=t('track_editor_audio_label'), font=("Segoe UI", 9, "bold")).pack(anchor=tk.W, pady=(0, 5))

        self.audio_tree_frame = ttk.Frame(main_frame)
        self.audio_tree_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        # Columns setup (with Size, Offset and Original columns for audio)
        audio_cols = [t('track_editor_col_title'), t('track_editor_col_lang'), t('track_editor_col_codec'), t('track_editor_col_channels'), t('track_editor_col_size'), t('track_editor_col_bitrate'), t('track_editor_col_offset'), t('track_editor_col_original'), t('track_editor_col_default')]
        self.audio_tree = self._create_treeview(self.audio_tree_frame, audio_cols)
        self.audio_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Bind right-click for conversion menu on audio tracks
        self.audio_tree.bind('<Button-3>', self._on_audio_right_click)

        # Bind double-click for offset editing on audio tree
        self.audio_tree.bind('<Double-1>', lambda e: self._on_tree_double_click(e, 'audio'))
        
        # Scrollbar
        audio_scroll = ttk.Scrollbar(self.audio_tree_frame, orient="vertical", command=self.audio_tree.yview)
        self.audio_tree.configure(yscrollcommand=audio_scroll.set)
        audio_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Audio Buttons
        audio_btn_frame = ttk.Frame(self.audio_tree_frame)
        audio_btn_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(10, 0))
        
        ttk.Button(audio_btn_frame, text="▲", width=4, command=lambda: self._move_item(self.audio_tree, -1)).pack(pady=2)
        ttk.Button(audio_btn_frame, text="▼", width=4, command=lambda: self._move_item(self.audio_tree, 1)).pack(pady=2)
        ttk.Separator(audio_btn_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(audio_btn_frame, text="Default", command=lambda: self._set_default(self.audio_tree, 'audio')).pack(pady=2)
        ttk.Separator(audio_btn_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(audio_btn_frame, text=t('track_editor_btn_remove'), command=lambda: self._remove_track(self.audio_tree)).pack(pady=2)
        
        # --- Subtitle Section ---
        ttk.Label(main_frame, text=t('track_editor_subtitle_label'), font=("Segoe UI", 9, "bold")).pack(anchor=tk.W, pady=(0, 5))
        
        self.subtitle_tree_frame = ttk.Frame(main_frame)
        self.subtitle_tree_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        sub_cols = [t('track_editor_col_title'), t('track_editor_col_lang'), t('track_editor_col_codec'), t('track_editor_col_size'), t('track_editor_col_offset'), t('track_editor_col_original'), t('track_editor_col_default'), t('track_editor_col_forced')]
        self.subtitle_tree = self._create_treeview(self.subtitle_tree_frame, sub_cols)
        self.subtitle_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Bind double-click for offset editing on subtitle tree
        self.subtitle_tree.bind('<Double-1>', lambda e: self._on_tree_double_click(e, 'subtitle'))

        # Scrollbar
        sub_scroll = ttk.Scrollbar(self.subtitle_tree_frame, orient="vertical", command=self.subtitle_tree.yview)
        self.subtitle_tree.configure(yscrollcommand=sub_scroll.set)
        sub_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Subtitle Buttons
        sub_btn_frame = ttk.Frame(self.subtitle_tree_frame)
        sub_btn_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(10, 0))
        
        ttk.Button(sub_btn_frame, text="▲", width=4, command=lambda: self._move_item(self.subtitle_tree, -1)).pack(pady=2)
        ttk.Button(sub_btn_frame, text="▼", width=4, command=lambda: self._move_item(self.subtitle_tree, 1)).pack(pady=2)
        ttk.Separator(sub_btn_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(sub_btn_frame, text=t('track_editor_btn_load_external'),
                   command=self._load_external_subtitle).pack(pady=2, fill='x')
        ttk.Separator(sub_btn_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(sub_btn_frame, text="Default", command=lambda: self._set_default(self.subtitle_tree, 'subtitle')).pack(pady=2)
        ttk.Button(sub_btn_frame, text="Forced", command=lambda: self._toggle_forced(self.subtitle_tree)).pack(pady=2)
        ttk.Separator(sub_btn_frame, orient='horizontal').pack(fill='x', pady=5)
        ttk.Button(sub_btn_frame, text=t('track_editor_btn_remove'), command=lambda: self._remove_track(self.subtitle_tree)).pack(pady=2)

        # --- Main Buttons ---
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(15, 0))
        
        save_btn = ttk.Button(btn_frame, text=t('track_editor_btn_save'), command=self._save_changes)
        save_btn.pack(side=tk.RIGHT, padx=5)
        
        cancel_btn = ttk.Button(btn_frame, text=t('track_editor_btn_cancel'), command=self.destroy)
        cancel_btn.pack(side=tk.RIGHT, padx=5)

        # Populate Views
        self._populate_tree(self.audio_tree, self.audio_tracks, 'audio')
        self._populate_tree(self.subtitle_tree, self.subtitle_tracks, 'subtitle')

    def _create_treeview(self, parent, columns):
        tree = ttk.Treeview(parent, columns=columns, show="headings", selectmode="browse", height=6)
        
        # Define column widths
        total_width = 500
        col_width = total_width // len(columns)
        
        for col in columns:
            tree.heading(col, text=col)
            if col in [t('track_editor_col_default'), t('track_editor_col_forced'), t('track_editor_col_channels')]:
                tree.column(col, width=50, anchor='center')
            elif col == t('track_editor_col_lang') or col == t('track_editor_col_codec'):
                tree.column(col, width=60, anchor='center')
            elif col == t('track_editor_col_size'):
                tree.column(col, width=70, anchor='e')  # Right-aligned for size values
            elif col == t('track_editor_col_bitrate'):
                tree.column(col, width=80, anchor='e')
            elif col == t('track_editor_col_offset'):
                tree.column(col, width=70, anchor='e')  # Editable offset column
            elif col == t('track_editor_col_original'):
                tree.column(col, width=70, anchor='e')  # Read-only original offset column
            elif col == t('track_editor_col_title'):
                tree.column(col, width=250, anchor='w') # More space for title
            else:
                tree.column(col, width=100, anchor='w')
        
        # Configure tags for visual feedback
        tree.tag_configure('offset_mismatch', foreground='#ff5555')  # Red for offset mismatch
                
        return tree

    def _find_matching_source_track(self, track, source_track_list):
        """Find matching track in source list using heuristics (not just index).
        
        This handles cases where tracks have been reordered in a previous remux.
        """
        if not source_track_list:
            return None

        # 1. Try exact index match first (fastest and most common for original files)
        index_match = next((t for t in source_track_list if t['index'] == track['index']), None)
        
        # Check if index match is valid (same language and codec)
        if index_match:
            # If languages match (or one is undef), assume it's the correct track
            track_lang = track.get('lang', 'und')
            source_lang = index_match.get('lang', 'und')
            
            # Simple normalization for comparison
            if track_lang == source_lang or track_lang == 'und' or source_lang == 'und':
                return index_match
            
            # If codecs match strongly, might still accept
            if track.get('codec') == index_match.get('codec'):
                pass # Continue to strict checking below if you want, but often codec+index is enough
            else:
                # Index matches but properties differ significantly -> likely reordered!
                pass # Fall through to content search

        # 2. Heuristic Search: Find track with matching properties
        # Build "fingerprint" for current track
        current_fingerprint = (
            track.get('lang', 'und'),
            track.get('codec', ''),
            track.get('channels', 0)
        )

        candidates = []
        for src in source_track_list:
            src_fingerprint = (
                src.get('lang', 'und'),
                src.get('codec', ''),
                src.get('channels', 0)
            )
            
            # Count matches
            score = 0
            if current_fingerprint[0] == src_fingerprint[0] and current_fingerprint[0] != 'und': score += 3 # Language match
            if current_fingerprint[1] == src_fingerprint[1]: score += 1 # Codec match
            if current_fingerprint[2] == src_fingerprint[2]: score += 1 # Channels match
            
            if score > 0:
                candidates.append((score, src))
        
        # Sort by score (highest first)
        candidates.sort(key=lambda x: x[0], reverse=True)
        
        if candidates:
            # Return best match if score is high enough (e.g., language matches)
            return candidates[0][1]
            
        # 3. Fallback to index match if content search failed (maybe metadata was wiped)
        return index_match

    def _populate_tree(self, tree, tracks, track_type):
        for track in tracks:
            title = track.get('title') or f"Track {track['index']}"
            lang = track.get('lang', 'und')
            codec = track.get('codec', '')

            # Get offset from OUTPUT file (editable, current value)
            offset_ms = track.get('start_time_ms', 0)
            track_key = (track_type, track['index'])  # (type, original_index) tuple

            # Get offset from SOURCE file (read-only reference for "Eredeti" column)
            source_offset_ms = None  # Default to None (displays as "-")

            if self.source_streams:
                source_track_list = self.source_streams.get(track_type, [])

                # Use smart matching instead of blind index matching
                source_track = self._find_matching_source_track(track, source_track_list)

                if source_track:
                    source_offset_ms = source_track.get('start_time_ms', 0)

            self.original_offsets[track_key] = source_offset_ms  # Store SOURCE offset (can be None)
            self.user_offsets[track_key] = offset_ms

            source_offset_display = str(source_offset_ms) if source_offset_ms is not None else "-"
            
            values = [title, lang, codec]

            if track_type == 'audio':
                # Audio: Title, Lang, Codec, Channels, Size, Bitrate, Offset, Original, Default
                values.append(str(track.get('channels', '*')))
                size_mb = track.get('size_mb')
                if size_mb is not None:
                    values.append(f"{size_mb:.1f} MB")
                else:
                    values.append("-")
                bit_rate = track.get('bit_rate')
                if bit_rate:
                    try:
                        br_kbps = int(float(bit_rate) / 1000)
                        values.append(f"{br_kbps} kbps")
                    except (ValueError, TypeError):
                        values.append("-")
                else:
                    values.append("-")
                # Add offset column (editable, current OUTPUT value)
                values.append(str(offset_ms))
                # Add original column (read-only, SOURCE value)
                values.append(source_offset_display)
                values.append("[OK]" if track.get('default') else "")
            else:
                # Subtitle: Title, Lang, Codec, Size, Offset, Original, Default, Forced
                size_mb = track.get('size_mb')
                if size_mb is not None:
                    size_kb = size_mb * 1024
                    if size_kb >= 1:
                        values.append(f"{size_kb:.1f} KB")
                    else:
                        values.append(f"{size_mb * 1024 * 1024:.0f} B")
                else:
                    values.append("-")
                # Add offset column (editable, current OUTPUT value)
                values.append(str(offset_ms))
                # Add original column (read-only, SOURCE value)
                values.append(source_offset_display)
                values.append("[OK]" if track.get('default') else "")
                values.append("[OK]" if track.get('forced') else "")

            # Use original index as IID to identify unique tracks
            tags = ()
            # Check for offset mismatch: diff != 0 AND source exists (not None/-)
            if source_offset_ms is not None and offset_ms != source_offset_ms:
                tags = ('offset_mismatch',)
                
            tree.insert("", "end", iid=str(track['index']), values=values, tags=tags)

    def _get_current_tree_order_list(self, tree, source_list):
        """Reconstruct list based on visual tree order."""
        new_list = []
        for iid in tree.get_children():
            # Find the track data corresponding to this IID (original index)
            original_idx = int(iid)
            track_data = next((t for t in source_list if t['index'] == original_idx), None)
            if track_data:
                new_list.append(track_data)
        return new_list

    def _refresh_tree_item_visuals(self, tree, tracks, track_type):
        """Update visible checkmarks and offset values based on underlying data."""
        for track in tracks:
            iid = str(track['index'])
            if tree.exists(iid):
                current_values = list(tree.item(iid, 'values'))
                
                # Default checkmark (last or second to last column)
                is_default = track.get('default', False)
                
                # Check column count to find correct index
                # Audio: ..., Size(4), Bitrate(5), Offset(6), Original(7), Default(8) -> default=8
                # Sub: ..., Size(3), Offset(4), Original(5), Default(6), Forced(7) -> default=6
                if track_type == 'audio':
                    default_idx = 8
                    offset_idx = 6
                    original_idx = 7
                else: 
                    default_idx = 6
                    offset_idx = 4
                    original_idx = 5
                    
                    # Update forced checkmark too
                    forced_idx = 7
                    is_forced = track.get('forced', False)
                    if len(current_values) > forced_idx:
                        current_values[forced_idx] = "[OK]" if is_forced else ""

                if len(current_values) > default_idx:
                    current_values[default_idx] = "[OK]" if is_default else ""
                    
                # Update offset value
                track_key = (track_type, track['index'])
                offset_ms = self.user_offsets.get(track_key, track.get('start_time_ms', 0))
                if len(current_values) > offset_idx:
                    current_values[offset_idx] = str(offset_ms)
                    
                # Check for mismatch against stored original offset
                source_offset_ms = self.original_offsets.get(track_key, None)
                
                # Update original column display (in case it wasn't set correctly or to ensure sync)
                source_display = str(source_offset_ms) if source_offset_ms is not None else "-"
                if len(current_values) > original_idx:
                     current_values[original_idx] = source_display
                
                # Apply tag if mismatch (Only if source is known, i.e. not None/-)
                tags = ()
                if source_offset_ms is not None and offset_ms != source_offset_ms:
                    tags = ('offset_mismatch',)

                tree.item(iid, values=current_values, tags=tags)

    def _update_video_visuals(self, *args):
        """Update video offset label color based on mismatch."""
        if not hasattr(self, 'video_offset_label') or not hasattr(self, 'original_video_offset_ms'):
            return
            
        try:
            current_val = int(self.video_offset_var.get())
        except ValueError:
            current_val = 0
            
        # Check mismatch
        if self.original_video_offset_ms is not None and current_val != self.original_video_offset_ms:
            self.video_offset_label.configure(style="Mismatch.TLabel")
        else:
            self.video_offset_label.configure(style="TLabel")

    def _refresh_virtual_track_visuals(self, tree, track_type):
        """Update visible checkmarks for virtual (converted) tracks."""
        for item in tree.get_children():
            iid = int(item)
            if iid < 10000:
                continue  # Skip original tracks

            # Find in pending_conversions
            conv = next((c for c in self.pending_conversions if c.get('virtual_iid') == iid), None)
            if conv:
                values = list(tree.item(item, 'values'))
                if track_type == 'audio':
                    # Audio columns: Title(0), Lang(1), Codec(2), Channels(3), Size(4), Bitrate(5), Offset(6), Original(7), Default(8)
                    # Virtual tracks have offset 0 by default
                    values[8] = "[OK]" if conv.get('default') else ""
                    tree.item(item, values=values)

    def _refresh_external_subtitle_visuals(self):
        """Update visible checkmarks for external subtitle tracks."""
        for item in self.subtitle_tree.get_children():
            iid = int(item)
            if iid < 20000:
                continue  # Skip non-external tracks

            ext_sub = next((s for s in self.external_subtitles if s['virtual_iid'] == iid), None)
            if ext_sub:
                values = list(self.subtitle_tree.item(item, 'values'))
                # Subtitle columns: Title(0), Lang(1), Codec(2), Size(3), Offset(4), Original(5), Default(6), Forced(7)
                # External subtitles have offset 0, no original (shown as "-")
                values[4] = "0"    # Offset column (editable)
                values[5] = "-"    # Original column (read-only, no source)
                values[6] = "[OK]" if ext_sub.get('default') else ""  # Default column
                values[7] = "[OK]" if ext_sub.get('forced') else ""   # Forced column
                self.subtitle_tree.item(item, values=values)

    def _move_item(self, tree, direction):
        selected = tree.selection()
        if not selected: return
        
        item = selected[0]
        index = tree.index(item)
        
        # Move visual item
        if direction == -1 and index > 0:
            tree.move(item, tree.parent(item), index - 1)
        elif direction == 1 and index < len(tree.get_children()) - 1:
            tree.move(item, tree.parent(item), index + 1)
            
    def _set_default(self, tree, track_type):
        selected = tree.selection()
        if not selected: return
        selected_iid = int(selected[0])

        tracks = self.audio_tracks if track_type == 'audio' else self.subtitle_tracks

        # Check if this is a virtual (converted) track or external subtitle
        is_virtual = selected_iid >= 10000
        is_external_subtitle = selected_iid >= 20000

        # First find the current status of the selected track
        is_already_default = False
        if is_external_subtitle:
            # Check in external_subtitles
            for ext_sub in self.external_subtitles:
                if ext_sub.get('virtual_iid') == selected_iid:
                    is_already_default = ext_sub.get('default', False)
                    break
        elif is_virtual:
            # Check in pending_conversions
            for conv in self.pending_conversions:
                if conv.get('virtual_iid') == selected_iid:
                    is_already_default = conv.get('default', False)
                    break
        else:
            for track in tracks:
                if track['index'] == selected_iid and track.get('default'):
                    is_already_default = True
                    break

        # Toggle: if already default, turn it off. If not, turn it on (and others off).
        new_status = not is_already_default

        # Update all original tracks
        for track in tracks:
            if track['index'] == selected_iid:
                track['default'] = new_status
            else:
                track['default'] = False

        # Update all virtual tracks in pending_conversions
        for conv in self.pending_conversions:
            if conv.get('virtual_iid') == selected_iid:
                conv['default'] = new_status
            else:
                conv['default'] = False

        # Update all external subtitles
        for ext_sub in self.external_subtitles:
            if ext_sub.get('virtual_iid') == selected_iid:
                ext_sub['default'] = new_status
            else:
                ext_sub['default'] = False

        # Refresh visuals for original tracks
        self._refresh_tree_item_visuals(tree, tracks, track_type)

        # Also refresh virtual tracks visually
        self._refresh_virtual_track_visuals(tree, track_type)

        # Also refresh external subtitles visually
        if track_type == 'subtitle':
            self._refresh_external_subtitle_visuals()

    def _toggle_forced(self, tree):
        selected = tree.selection()
        if not selected: return
        selected_iid = int(selected[0])

        # Check if external subtitle
        if selected_iid >= 20000:
            ext_sub = next((s for s in self.external_subtitles if s['virtual_iid'] == selected_iid), None)
            if ext_sub:
                ext_sub['forced'] = not ext_sub.get('forced', False)
                self._refresh_external_subtitle_visuals()
            return

        tracks = self.subtitle_tracks
        track = next((t for t in tracks if t['index'] == selected_iid), None)
        if track:
            track['forced'] = not track.get('forced', False)
            self._refresh_tree_item_visuals(tree, tracks, 'subtitle')

    def _remove_track(self, tree):
        selected = tree.selection()
        if not selected: return

        # In browse mode, only one is selected
        item = selected[0]

        # Also remove from pending_conversions if it's a virtual track
        try:
            iid = int(item)
            self.pending_conversions = [c for c in self.pending_conversions if c.get('virtual_iid') != iid]

            # Remove from external_subtitles if it's an external subtitle (IID >= 20000)
            if iid >= 20000:
                self.external_subtitles = [s for s in self.external_subtitles if s['virtual_iid'] != iid]
        except ValueError:
            pass

        tree.delete(item)

    def _load_external_subtitle(self):
        """Load external subtitle file and add to subtitle tree."""
        from tkinter import filedialog

        # Open file dialog
        filetypes = [
            ("Subtitle files", "*.srt *.ass *.ssa *.vtt *.sub"),
            ("All files", "*.*")
        ]
        file_path = filedialog.askopenfilename(
            parent=self,
            title=t('track_editor_load_subtitle_title'),
            filetypes=filetypes
        )

        if not file_path:
            return  # User cancelled

        sub_path = Path(file_path)

        # Validate file
        try:
            from .core_subtitles_and_metadata import is_valid_subtitle_file
            is_valid, error_msg = is_valid_subtitle_file(sub_path)
        except Exception as e:
            is_valid = False
            error_msg = str(e)

        if not is_valid:
            messagebox.showerror(
                t('track_editor_error'),
                t('track_editor_invalid_subtitle').format(
                    filename=sub_path.name,
                    reason=error_msg
                ),
                parent=self
            )
            return

        # Parse filename for automatic language/title detection
        suggested_lang, suggested_title = parse_subtitle_filename(sub_path.name)

        # Open language input dialog with suggestions
        lang_dialog = LanguageInputDialog(
            self,
            sub_path.name,
            suggested_language=suggested_lang,
            suggested_title=suggested_title
        )
        self.wait_window(lang_dialog)

        if not lang_dialog.result:
            return  # User cancelled

        lang_code, title = lang_dialog.result

        # Create virtual track
        virtual_iid = str(self.next_external_subtitle_iid)
        self.next_external_subtitle_iid += 1

        # Determine codec from extension
        codec = sub_path.suffix.lstrip('.').upper()  # .srt -> SRT

        # Build display title
        display_title = f"[External] {title}" if title else f"[External] {sub_path.name}"

        # Create track data
        external_sub_data = {
            'is_external': True,
            'external_path': sub_path,
            'lang': lang_code,
            'title': title or sub_path.name,
            'codec': codec,
            'default': False,
            'forced': False,
            'virtual_iid': int(virtual_iid),
            'index': int(virtual_iid),  # For compatibility
            'original_index': None
        }

        self.external_subtitles.append(external_sub_data)

        # Add to treeview
        # Subtitle columns: Title, Lang, Codec, Offset, Original, Default, Forced
        values = [
            display_title,
            lang_code,
            codec,
            "0",      # Offset (external subtitles start at 0)
            "-",      # Original (no source file offset)
            "",       # Default
            ""        # Forced
        ]

        self.subtitle_tree.insert("", "end", iid=virtual_iid, values=values)

        # Select the newly added item
        self.subtitle_tree.selection_set(virtual_iid)
        self.subtitle_tree.see(virtual_iid)

    def _on_audio_right_click(self, event):
        """Handle right-click on audio tree for conversion menu."""
        item_id = self.audio_tree.identify_row(event.y)
        if not item_id:
            return
            
        # Select the item
        self.audio_tree.selection_set(item_id)
        
        # Get track info
        try:
            original_idx = int(item_id)
        except ValueError:
            return
            
        # Find track data - check original tracks
        track = next((t for t in self.audio_tracks if t['index'] == original_idx), None)
        
        # If not found, might be a virtual converted track - don't show conversion menu
        if not track:
            return
            
        channels = track.get('channels', 0)
        
        # Only show conversion menu for 5.1+ (>= 5 channels)
        if channels < 5:
            return
            
        # Build conversion context menu
        menu = tk.Menu(self, tearoff=0)
        
        # Submenu for "As new track"
        new_track_menu = tk.Menu(menu, tearoff=0)
        new_track_menu.add_command(
            label=t('audio_compression_fast'),
            command=lambda: self._convert_audio_track(track, 'fast', 'new')
        )
        new_track_menu.add_command(
            label=t('audio_compression_dialogue'),
            command=lambda: self._convert_audio_track(track, 'dialogue', 'new')
        )
        menu.add_cascade(label=t('track_editor_convert_new'), menu=new_track_menu)
        
        # Submenu for "Replace existing"
        replace_menu = tk.Menu(menu, tearoff=0)
        replace_menu.add_command(
            label=t('audio_compression_fast'),
            command=lambda: self._convert_audio_track(track, 'fast', 'replace')
        )
        replace_menu.add_command(
            label=t('audio_compression_dialogue'),
            command=lambda: self._convert_audio_track(track, 'dialogue', 'replace')
        )
        menu.add_cascade(label=t('track_editor_convert_replace'), menu=replace_menu)
        
        menu.post(event.x_root, event.y_root)

    def _on_tree_double_click(self, event, track_type):
        """Handle double-click on tree item to edit offset."""
        tree = self.audio_tree if track_type == 'audio' else self.subtitle_tree

        # Identify which item and column was clicked
        item_id = tree.identify_row(event.y)
        column = tree.identify_column(event.x)

        if not item_id:
            return

        # Determine offset column index based on track type (Tkinter uses 1-based column IDs)
        # Audio: Title(#1), Lang(#2), Codec(#3), Channels(#4), Size(#5), Offset(#6), Original(#7), Default(#8)
        # Subtitle: Title(#1), Lang(#2), Codec(#3), Size(#4), Offset(#5), Original(#6), Default(#7), Forced(#8)
        offset_col = '#6' if track_type == 'audio' else '#5'

        if column != offset_col:
            return  # Only handle clicks on offset column

        try:
            original_idx = int(item_id)
        except ValueError:
            return

        # Virtual tracks (conversions, external subtitles) don't have editable offsets
        if original_idx >= 10000:
            return

        # Get current offset value
        track_key = (track_type, original_idx)
        current_offset = self.user_offsets.get(track_key, 0)

        # Open edit dialog
        self._edit_offset(tree, item_id, track_type, original_idx, current_offset)

    def _edit_offset(self, tree, item_id, track_type, original_idx, current_offset):
        """Open dialog to edit offset value."""
        from tkinter import simpledialog

        # Custom dialog for offset editing
        dialog = tk.Toplevel(self)
        dialog.title(t('track_editor_offset_edit_title'))
        dialog.geometry("300x150")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()

        # Center on parent
        dialog.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() - dialog.winfo_width()) // 2
        y = self.winfo_y() + (self.winfo_height() - dialog.winfo_height()) // 2
        dialog.geometry(f'+{x}+{y}')

        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        # Label
        ttk.Label(frame, text=t('track_editor_offset_edit_label')).pack(anchor=tk.W)

        # Spinbox for offset value
        offset_var = tk.StringVar(value=str(current_offset))
        spinbox = ttk.Spinbox(frame, from_=-999999, to=999999, width=15, textvariable=offset_var)
        spinbox.pack(anchor=tk.W, pady=(5, 5))
        spinbox.focus_set()
        spinbox.selection_range(0, tk.END)

        # Hint
        ttk.Label(frame, text=t('track_editor_offset_edit_hint'),
                  font=("Segoe UI", 8), foreground="gray").pack(anchor=tk.W)

        def on_ok():
            try:
                new_offset = int(offset_var.get())
            except ValueError:
                messagebox.showerror(t('track_editor_error'), "Invalid offset value", parent=dialog)
                return

            # Update user_offsets
            track_key = (track_type, original_idx)
            self.user_offsets[track_key] = new_offset

            # Update tree display with new offset value (separate column, no formatting needed)
            values = list(tree.item(item_id, 'values'))
            if track_type == 'audio':
                # Audio columns: Title(0), Lang(1), Codec(2), Channels(3), Size(4), Bitrate(5), Offset(6), Original(7), Default(8)
                values[6] = str(new_offset)  # Offset column for audio
            else:
                # Subtitle columns: Title(0), Lang(1), Codec(2), Size(3), Offset(4), Original(5), Default(6), Forced(7)
                values[4] = str(new_offset)  # Offset column for subtitle
            tree.item(item_id, values=values)

            dialog.destroy()

        def on_cancel():
            dialog.destroy()

        # Buttons
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(side=tk.BOTTOM, pady=(10, 0))
        ttk.Button(btn_frame, text="OK", command=on_ok, width=10).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_frame, text=t('track_editor_btn_cancel'), command=on_cancel, width=10).pack(side=tk.LEFT)

        # Bind Enter key
        spinbox.bind('<Return>', lambda e: on_ok())
        dialog.bind('<Escape>', lambda e: on_cancel())

    def _convert_audio_track(self, source_track, method, mode):
        """Handle audio track conversion request.
        
        Args:
            source_track: The original 5.1+ track data dict
            method: 'fast' or 'dialogue' compression method
            mode: 'new' (add as new track) or 'replace' (replace existing)
        """
        source_iid = str(source_track['index'])
        
        if mode == 'new':
            # Add a new virtual track to the tree
            virtual_iid = str(self.next_virtual_iid)
            self.next_virtual_iid += 1
            source_track_key = ('audio', source_track['index'])
            inherited_offset = self.user_offsets.get(source_track_key, source_track.get('start_time_ms', 0))
            inherited_original = self.original_offsets.get(source_track_key, source_track.get('start_time_ms', 0))
            
            # Create visual representation
            method_label = t('audio_compression_fast') if method == 'fast' else t('audio_compression_dialogue')
            title = f"{source_track.get('title', 'Track')} {t('track_editor_converted_suffix')} [{method_label}]"
            lang = source_track.get('lang', 'und')
            codec = 'AAC'
            channels = '2'
            size = '-'      # Size is unknown for converted tracks
            bitrate = '-'
            offset = str(inherited_offset)
            original = str(inherited_original) if inherited_original is not None else '-'
            default = ''

            values = [title, lang, codec, channels, size, bitrate, offset, original, default]
            
            # Insert after the source track
            source_position = self.audio_tree.index(source_iid)
            self.audio_tree.insert("", source_position + 1, iid=virtual_iid, values=values)
            
            # Store conversion info
            conversion_info = {
                'source_track': source_track.copy(),
                'method': method,
                'mode': 'new',
                'virtual_iid': int(virtual_iid),
                'title': title,
                'lang': lang
            }
            self.pending_conversions.append(conversion_info)
            
        elif mode == 'replace':
            # Mark the existing track for conversion
            method_label = t('audio_compression_fast') if method == 'fast' else t('audio_compression_dialogue')
            values = list(self.audio_tree.item(source_iid, 'values'))
            original_title = source_track.get('title', f"Track {source_track['index']}")
            values[0] = f"{original_title} -> 2.0 [{method_label}]"
            values[2] = 'AAC'
            values[3] = '2'
            values[4] = '-'  # Size becomes unknown/irrelevant for the target track
            values[5] = '-'  # Bitrate becomes unknown after conversion
            self.audio_tree.item(source_iid, values=values)
            
            # Store conversion info
            conversion_info = {
                'source_track': source_track.copy(),
                'method': method,
                'mode': 'replace',
                'virtual_iid': source_track['index'],
                'title': values[0],
                'lang': source_track.get('lang', 'und')
            }
            
            # Remove any existing conversion for this track
            self.pending_conversions = [c for c in self.pending_conversions 
                                        if c.get('virtual_iid') != source_track['index']]
            self.pending_conversions.append(conversion_info)

    def _save_changes(self):
        """Gather tracks from tree and prepare for saving.

        IMPORTANT: Only tracks visible in the tree will be saved!
        - Removed tracks: not in tree -> not saved
        - Reordered tracks: reflected in tree order
        - Converted tracks: marked in pending_conversions
        - User-modified offsets: applied to each track
        """
        # Get video offset from spinbox
        try:
            video_offset_ms = int(self.video_offset_var.get())
        except ValueError:
            video_offset_ms = 0

        # Get all IIDs in the audio tree in visual order
        audio_iids = list(self.audio_tree.get_children())
        subtitle_iids = list(self.subtitle_tree.get_children())

        # Build final audio track list with proper order, conversion info, and user offsets
        final_audio = []
        for iid in audio_iids:
            iid_int = int(iid)

            # Check if this is a conversion (new or replace)
            conversion = next((c for c in self.pending_conversions if c['virtual_iid'] == iid_int), None)

            if conversion:
                # This track needs conversion
                track_data = conversion['source_track'].copy()
                track_data['needs_conversion'] = True
                track_data['conversion_method'] = conversion['method']
                track_data['conversion_mode'] = conversion['mode']
                track_data['output_title'] = conversion['title']

                # Get default status from pending_conversions (updated by _set_default)
                track_data['default'] = conversion.get('default', False)

                source_track_key = ('audio', track_data['index'])
                track_data['user_offset_ms'] = self.user_offsets.get(
                    source_track_key,
                    track_data.get('start_time_ms', 0)
                )

                final_audio.append(track_data)
            else:
                # Original track (copy or just reorder)
                track = next((t for t in self.audio_tracks if t['index'] == iid_int), None)
                if track:
                    track_copy = track.copy()
                    track_copy['needs_conversion'] = False
                    # Get user-modified offset
                    track_key = ('audio', iid_int)
                    track_copy['user_offset_ms'] = self.user_offsets.get(track_key, track.get('start_time_ms', 0))
                    final_audio.append(track_copy)

        # Build final subtitle list (INCLUDING external subtitles)
        final_subs = []
        for iid in subtitle_iids:
            iid_int = int(iid)

            # Check if this is an external subtitle
            if iid_int >= 20000:
                ext_sub = next((s for s in self.external_subtitles if s['virtual_iid'] == iid_int), None)
                if ext_sub:
                    ext_copy = ext_sub.copy()
                    ext_copy['user_offset_ms'] = 0  # External subtitles start at 0
                    final_subs.append(ext_copy)
            else:
                # Embedded subtitle from original video
                track = next((t for t in self.subtitle_tracks if t['index'] == iid_int), None)
                if track:
                    track_copy = track.copy()
                    # Get user-modified offset
                    track_key = ('subtitle', iid_int)
                    track_copy['user_offset_ms'] = self.user_offsets.get(track_key, track.get('start_time_ms', 0))
                    final_subs.append(track_copy)

        # Get rotation from combobox (relative mode: delta added to current)
        rotation_degrees = None
        if self.rotation_combo is not None:
            rotation_index = self.rotation_combo.current()
            # FFmpeg display rotation uses CCW-positive degrees.
            # Right 90° (CW) = -90° = +270 modulo 360
            # Left 90° (CCW) = +90°
            delta_map = {0: 0, 1: 270, 2: 180, 3: 90}
            delta = delta_map.get(rotation_index, 0)

            if delta != 0:
                rotation_degrees = (self._current_rotation + delta) % 360

        # Update main treeview status to show saving in progress
        if self.item_id and hasattr(self.parent, 'encoding_queue'):
            self.parent.encoding_queue.put(("status_only", self.item_id, t('status_track_saving')))

        self.destroy()  # Close editor window

        # Open the separate process execution window with user offsets
        ProgressWindow(self.parent, self.video_path, final_audio, final_subs,
                       self.on_completion, video_offset_ms, rotation_degrees=rotation_degrees)

    def _apply_cached_track_info(self, cache):
        """Apply cached track info (size_mb, bit_rate) from DB to in-memory tracks."""
        _log = get_log_writer()
        all_covered = True
        for track_type in ('audio', 'subtitle'):
            cached_tracks = cache.get(track_type, [])
            live_tracks = self.audio_tracks if track_type == 'audio' else self.subtitle_tracks

            for cached_t in cached_tracks:
                cached_idx = cached_t.get('original_index')
                for live_t in live_tracks:
                    live_idx = live_t.get('original_index', live_t.get('index'))
                    if live_idx == cached_idx:
                        if cached_t.get('size_mb') is not None:
                            live_t['size_mb'] = cached_t['size_mb']
                        if cached_t.get('bit_rate') is not None:
                            live_t['bit_rate'] = cached_t['bit_rate']
                        if _log:
                            try:
                                sz = cached_t.get('size_mb')
                                br = cached_t.get('bit_rate')
                                sz_str = f"{sz:.1f} MB" if sz is not None else "N/A"
                                br_str = f"{int(float(br)/1000)} kbps" if br else "N/A"
                                label = 'Audio' if track_type == 'audio' else 'Subtitle'
                                _log.write(f"[TRACK_EDITOR]   {label} #{live_idx}: size={sz_str} bitrate={br_str} <- CACHE\n")
                            except Exception:
                                pass
                        break

            for live_t in live_tracks:
                has_size = live_t.get('size_mb') is not None
                has_br = live_t.get('bit_rate') is not None
                if track_type == 'audio' and not has_size and not has_br:
                    all_covered = False
                elif track_type == 'subtitle' and not has_size:
                    all_covered = False

        if _log:
            try:
                _log.flush()
            except Exception:
                pass
        self._cache_fully_covered = all_covered

    def _start_lazy_track_info_loading(self):
        self._lazy_loading_alive = True
        threading.Thread(target=self._lazy_load_worker, daemon=True, name="LazyTrackInfo").start()

    def _lazy_load_worker(self):
        _log = get_log_writer()
        if _log:
            try:
                _log.write(f"[TRACK_EDITOR] Lassú ffprobe beolvasás indul: {self.video_path.name}\n")
                _log.flush()
            except Exception:
                pass
        try:
            full_streams = get_video_streams_for_editor(self.video_path, quick=False)
            if not full_streams or not self._lazy_loading_alive:
                return

            for track_type in ('audio', 'subtitle'):
                full_tracks = full_streams.get(track_type, [])
                tree = self.audio_tree if track_type == 'audio' else self.subtitle_tree
                for full_track in full_tracks:
                    if not self._lazy_loading_alive:
                        return
                    stream_index = full_track.get('original_index', full_track.get('index'))
                    iid = str(stream_index)
                    size_mb = full_track.get('size_mb')
                    bit_rate = full_track.get('bit_rate')

                    if size_mb is None and bit_rate is None:
                        continue

                    if _log:
                        try:
                            sz_str = f"{size_mb:.1f} MB" if size_mb is not None else "N/A"
                            br_str = f"{int(float(bit_rate)/1000)} kbps" if bit_rate else "N/A"
                            label = 'Audio' if track_type == 'audio' else 'Subtitle'
                            _log.write(f"[TRACK_EDITOR]   {label} #{stream_index}: size={sz_str} bitrate={br_str} <- LASSÚ FFPROBE\n")
                        except Exception:
                            pass

                    try:
                        self.after(0, self._apply_lazy_track_info, iid, track_type, size_mb, bit_rate)
                    except Exception:
                        return

            if _log:
                try:
                    _log.write(f"[TRACK_EDITOR] Lassú ffprobe beolvasás kész (output)\n")
                    _log.flush()
                except Exception:
                    pass

            if self.source_path and self.source_path.exists():
                if _log:
                    try:
                        _log.write(f"[TRACK_EDITOR] Lassú ffprobe beolvasás indul (source): {self.source_path.name}\n")
                        _log.flush()
                    except Exception:
                        pass
                full_source = get_video_streams_for_editor(self.source_path, quick=False)
                if full_source and self._lazy_loading_alive:
                    for track_type in ('audio', 'subtitle'):
                        full_tracks = full_source.get(track_type, [])
                        for full_track in full_tracks:
                            if not self._lazy_loading_alive:
                                return
                            src_size = full_track.get('size_mb')
                            src_bitrate = full_track.get('bit_rate')
                            if src_size is not None or src_bitrate is not None:
                                if _log:
                                    try:
                                        idx = full_track.get('original_index', full_track.get('index'))
                                        sz_str = f"{src_size:.1f} MB" if src_size is not None else "N/A"
                                        br_str = f"{int(float(src_bitrate)/1000)} kbps" if src_bitrate else "N/A"
                                        label = 'Audio' if track_type == 'audio' else 'Subtitle'
                                        _log.write(f"[TRACK_EDITOR]   Source {label} #{idx}: size={sz_str} bitrate={br_str} <- LASSÚ FFPROBE\n")
                                    except Exception:
                                        pass
                                try:
                                    idx = full_track.get('original_index', full_track.get('index'))
                                    for t in (self.streams.get(track_type, []) if self.streams else []):
                                        if t.get('original_index', t.get('index')) == idx:
                                            t['size_mb'] = src_size
                                            t['bit_rate'] = src_bitrate
                                            break
                                except Exception:
                                    pass
                    if _log:
                        try:
                            _log.write(f"[TRACK_EDITOR] Lassú ffprobe beolvasás kész (source)\n")
                            _log.flush()
                        except Exception:
                            pass
        except Exception:
            pass

    def _apply_lazy_track_info(self, iid, track_type, size_mb, bit_rate):
        if not self._lazy_loading_alive:
            return
        try:
            tree = self.audio_tree if track_type == 'audio' else self.subtitle_tree
            if not tree.exists(iid):
                return
            vals = list(tree.item(iid, 'values'))

            if track_type == 'audio':
                size_col = 4
                bitrate_col = 5
                if size_mb is not None and len(vals) > size_col:
                    vals[size_col] = f"{size_mb:.1f} MB"
                if bit_rate is not None and len(vals) > bitrate_col:
                    try:
                        br_kbps = int(float(bit_rate) / 1000)
                        vals[bitrate_col] = f"{br_kbps} kbps"
                    except (ValueError, TypeError):
                        pass
            else:
                size_col = 3
                if size_mb is not None and len(vals) > size_col:
                    size_kb = size_mb * 1024
                    if size_kb >= 1:
                        vals[size_col] = f"{size_kb:.1f} KB"
                    else:
                        vals[size_col] = f"{size_mb * 1024 * 1024:.0f} B"

            tree.item(iid, values=vals)

            if track_type == 'audio':
                for t in self.audio_tracks:
                    if str(t.get('original_index', t.get('index'))) == iid:
                        if size_mb is not None:
                            t['size_mb'] = size_mb
                        if bit_rate is not None:
                            t['bit_rate'] = bit_rate
                        break
            else:
                for t in self.subtitle_tracks:
                    if str(t.get('original_index', t.get('index'))) == iid:
                        if size_mb is not None:
                            t['size_mb'] = size_mb
                        break
        except tk.TclError:
            pass

    def destroy(self):
        self._lazy_loading_alive = False
        super().destroy()


class ProgressWindow(tk.Toplevel):
    def __init__(self, parent, video_path, audio_tracks, subs_tracks, on_completion, video_offset_ms=0, rotation_degrees=None):
        super().__init__(parent)
        self.on_completion = on_completion
        self.video_offset_ms = video_offset_ms
        self.rotation_degrees = rotation_degrees
        
        video_filename = Path(video_path).name if video_path else ""
        self.title(f"{t('track_editor_progress_title')} - {video_filename}")
        self.geometry("700x450")
        
        # Center on screen
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')
        
        self.focus_force()
        self.lift()
        
        self.protocol("WM_DELETE_WINDOW", self._on_close_attempt)
        
        # Console-like text widget
        self.log_text = tk.Text(self, wrap="word", bg="#0c0c0c", fg="#cccccc", 
                               insertbackground="white", font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED) # Start as read-only
        
        # Scrollbar
        scroll = ttk.Scrollbar(self, command=self.log_text.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text['yscrollcommand'] = scroll.set
        
        # Setup Logger with Queue
        self.output_queue = queue.Queue()
        self.logger = SimpleLogger(self.output_queue)
        

        
        # Immediate visual feedback
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, t('track_editor_init_msg') + "\n")
        self.log_text.config(state=tk.DISABLED)
        self.update_idletasks()
        
        self.logger.write(t('track_editor_start_msg') + "\n")
        self.logger.write(t('track_editor_wait_msg') + "\n")
        self.update_idletasks()
        
        # Start worker thread
        self.thread = threading.Thread(
            target=self._run_process,
            args=(video_path, audio_tracks, subs_tracks, self.video_offset_ms),
            daemon=True
        )
        self.thread.start()
        
        # Start polling loop
        self.after(100, self._check_queue)

    def _check_queue(self):
        """Poll queue for new messages from worker thread."""
        try:
            while True:
                msg = self.output_queue.get_nowait()
                
                # Handle special signals
                if msg == ">>>SUCCESS<<<":
                    self.after(2000, self._finish_success)
                    continue
                elif msg == ">>>FAILURE<<<":
                    # Keep window open
                    continue
                
                # Normal text
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, msg)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
                self.update_idletasks()
        except queue.Empty:
            pass
        finally:
            if self.winfo_exists():
                self.after(100, self._check_queue)

    def _on_close_attempt(self):
        if self.thread.is_alive():
            messagebox.showwarning(t('track_editor_warning'), t('track_editor_process_running'))
        else:
            self.destroy()

    def _run_process(self, video_path, audio_tracks, subs_tracks, video_offset_ms=0):
        try:
            self.logger.write(f"DEBUG: Processing {video_path}\n")
            self.logger.write(f"DEBUG: Audio tracks: {len(audio_tracks)}\n")
            self.logger.write(f"DEBUG: Subs tracks: {len(subs_tracks)}\n")
            self.logger.write(f"DEBUG: Video offset: {video_offset_ms} ms\n")
            if self.rotation_degrees is not None:
                self.logger.write(f"DEBUG: Rotation: {self.rotation_degrees}°\n")

            # Pass our simple logger to the core function
            # Create a dummy stop_event that is never set to prevent global STOP_EVENT from affecting manual operations
            local_stop_event = threading.Event()  # Always False - manuális műveletek nem állnak le
            success = remux_video_with_track_selection(
                video_path, audio_tracks, subs_tracks,
                logger=self.logger, stop_event=local_stop_event,
                video_offset_ms=video_offset_ms,
                rotation_degrees=self.rotation_degrees
            )
            
            if success:
                 self.logger.write("\n" + "="*40 + "\n")
                 self.logger.write(t('track_editor_success') + "\n")
                 self.logger.write(t('track_editor_auto_close') + "\n")
                 # Send signal to main thread
                 self.logger.write(">>>SUCCESS<<<")
            else:
                 self.logger.write("\n" + "="*40 + "\n")
                 self.logger.write(t('track_editor_error_occurred') + "\n")
                 self.logger.write(t('track_editor_error_keep_open') + "\n")
                 self.logger.write(">>>FAILURE<<<")
        except Exception as e:
            err = traceback.format_exc()
            self.logger.write(f"\nCRITICAL ERROR:\n{err}\n")
            self.logger.write(">>>FAILURE<<<")

    def _finish_success(self):
        self.destroy()
        if self.on_completion:
            # Refresh GUI in main app safely
            self.on_completion()
