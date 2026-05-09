# Imports are handled by app.py dynamic loading
# from .gui_imports import *
# from .gui_shared import *
# from .gui_app_state_and_paths import AppStateAndPathsMixin
# from .gui_db_and_state_load import DBAndStateLoadMixin
# from .gui_tree_setup import TreeSetupMixin
# from .gui_video_loading import VideoLoadingMixin
# from .gui_encoding_control import EncodingControlMixin
# from .gui_queue_management import QueueManagementMixin
# from .gui_nvenc_worker import NvencWorkerMixin
# from .gui_svt_worker import SvtWorkerMixin
# from .gui_vmaf_worker import VmafWorkerMixin
# from .gui_audio_worker import AudioWorkerMixin
# from .gui_settings_control import SettingsControlMixin
# from .gui_context_menus import ContextMenusMixin
# from .gui_buttons_actions import ButtonsActionsMixin
# from .gui_language_and_labels import LanguageAndLabelsMixin
# from .gui_events_and_progress import EventsAndProgressMixin
# from .gui_misc_helpers import MiscHelpersMixin
# from .gui_widgets_layout import WidgetsLayoutMixin
# from .gui_task_list import TaskListMixin
# from .gui_http import HttpServerMixin
# from .gui_rebuild_worker import RebuildWorkerMixin

class VideoEncoderGUI(AppStateAndPathsMixin, DBAndStateLoadMixin, TreeSetupMixin, VideoLoadingMixin, EncodingControlMixin, TaskListMixin, QueueManagementMixin, NvencWorkerMixin, SvtWorkerMixin, VmafWorkerMixin, AudioWorkerMixin, RebuildWorkerMixin, SettingsControlMixin, ContextMenusMixin, ButtonsActionsMixin, LanguageAndLabelsMixin, EventsAndProgressMixin, MiscHelpersMixin, WidgetsLayoutMixin, HttpServerMixin):
    def __getattr__(self, attr):
        # Védelem: Ha a 'root' attribútum még nem létezik (pl. __init__ hiba),
        # vagy magát a 'root'-ot keressük, ne lépjünk rekurzióba.
        if attr == 'root':
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute 'root'")
        
        # Ha a 'root' nem létezik az objektumban, akkor AttributeError-t dobunk,
        # ahelyett, hogy megpróbálnánk elérni (ami végtelen rekurziót okozna)
        if 'root' not in self.__dict__:
             raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}' (and 'root' is not initialized)")

        return getattr(self.root, attr)

    def __init__(self, root, http_enabled=False, http_port=5000):
        """Initialize the VideoEncoderGUI application.
        
        Args:
            root: The root Tkinter window.
            http_enabled: Whether to start HTTP server.
            http_port: Port for HTTP server.
        """
        global CURRENT_LANGUAGE
        CURRENT_LANGUAGE = get_default_language()
        
        self.root = root
        self.root.title(t('app_title'))
        self.root.geometry("1400x900")
        self.root.minsize(1200, 700)  # Minimum window size to prevent widget clipping
        
        # Default sizes (for responsive layout)
        self.default_entry_width_source = 50  # Source/Dest entry fields
        self.default_entry_width_path = 38     # Path entry fields
        self.default_slider_length = 200       # Sliders
        
        # Minimum sizes (half of default)
        self.min_entry_width_source = 25
        self.min_entry_width_path = 19
        self.min_slider_length = 100
        
        global GUI_INSTANCE
        GUI_INSTANCE = self

        # Database operations lock - ensures database operations do not collide
        self.db_lock = threading.Lock()
        self.db_thread_lock = threading.Lock()
        self.active_db_threads = []
        self.load_db_save_completed = threading.Event()  # Flag: whether post-load DB save is completed
        self.db_update_notification_timer = None  # Timer for debouncing update notification
        self._db_update_notification_pending = False

        # SQLite database path determination (Fix for PyInstaller)
        import sys
        
        # Determine base directory explicitly
        if getattr(sys, 'frozen', False):
            # If frozen, ALWAYS use the executable's directory
            base_dir = Path(sys.executable).resolve().parent
        elif core.APP_ROOT:
            # If core.APP_ROOT is set and valid
            base_dir = core.APP_ROOT
        else:
            # Fallback to Current Working Directory
            base_dir = Path.cwd()
            
        self.db_path = base_dir / "save.db"
        
        # Ensure core knows about this root
        core.APP_ROOT = base_dir
        
        # Debug output to verify path resolution
        print(f"DEBUG: Resolved DB Path: {self.db_path}")
        # Initialize SQLite database
        self._init_database()
    
        # Automatic detection of program paths
        detected_programs = auto_detect_programs()
        self.ffmpeg_path = tk.StringVar(value=detected_programs['ffmpeg'] or '')
        self.virtualdub_path = tk.StringVar(value=detected_programs['virtualdub'] or '')
        self.abav1_path = tk.StringVar(value=detected_programs['abav1'] or '')
        self.apply_tool_paths_from_gui()
        for var in (self.ffmpeg_path, self.virtualdub_path, self.abav1_path):
            var.trace_add('write', self._on_tool_path_change)
        
        # Hybrid encoder path (optional - for SMDegrain denoising)
        # First check save.db for saved path
        saved_hybrid = get_saved_program_path('hybrid')
        if saved_hybrid:
            self.hybrid_path = tk.StringVar(value=saved_hybrid)
        else:
            # Check preferred locations
            hybrid_path = None
            try:
                preferred_paths = _get_preferred_search_paths('hybrid')
                for path in preferred_paths:
                    if path.exists():
                        hybrid_path = str(path)
                        break
            except Exception:
                pass
            # Fallback to default location
            if not hybrid_path:
                default_hybrid = Path("C:/Program Files/Hybrid")
                if default_hybrid.exists():
                    hybrid_path = str(default_hybrid)
            self.hybrid_path = tk.StringVar(value=hybrid_path if hybrid_path else "")
        
        # Write summary
        if LOG_WRITER:
            try:
                LOG_WRITER.write("\n=== PROGRAM DETECTION SUMMARY ===\n")
                LOG_WRITER.write(f"  FFmpeg: {'[OK] ' + detected_programs['ffmpeg'] if detected_programs['ffmpeg'] else '[ERROR] Not found'}\n")
                LOG_WRITER.write(f"  VirtualDub2: {'[OK] ' + detected_programs['virtualdub'] if detected_programs['virtualdub'] else '[ERROR] Not found'}\n")
                LOG_WRITER.write(f"  ab-av1: {'[OK] ' + detected_programs['abav1'] if detected_programs['abav1'] else '[ERROR] Not found'}\n")
                LOG_WRITER.write("=====================================\n\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
    
        self.source_path = None
        self.dest_path = None
        self.video_files = []
        self.video_items = {}
        self.subtitle_items = {}
        self.video_to_output = {}
        # Cache for stat() values received during loading (for cold start optimization)
        # Structure: {video_path: {'source_size_bytes': int, 'source_modified_timestamp': float}}
        self.video_stat_cache = {}
        
        # Original data behind tree items (for fast DB saving, without parsing)
        # Structure: {item_id: {'source_duration_seconds': float, 'source_frame_count': int, 'source_fps': float, ...}}
        self.tree_item_data = {}
        self.video_order = {}  # Order storage: {video_path: order_number}
        self.video_denoise_enabled = {}  # Denoise setting per video: {video_path: int (0=off, 1=strong, 2=light, 3=very-strong)}
        self.video_denoise_lock = threading.Lock()  # Thread-safe access to video_denoise_enabled
        self.video_hard_rotate_degrees = {}  # Hard rotate per video: {video_path: int (0/90/180/270)}
        self.video_hard_rotate_lock = threading.Lock()  # Thread-safe access to video_hard_rotate_degrees
        
        self.sort_column = None  # Current sort column
        self.sort_reverse = False  # Descending/ascending sort
        self.encoding_start_times = {}  # {item_id: start_time} - encoding/VMAF start time
        self.estimated_end_timer = None  # Timer for updating estimated completion time
        self.estimated_end_dates = {}  # Storage for estimated completion times (item_id -> date string)
        self.manual_nvenc_tasks = []  # Manual NVENC re-encoding tasks
        self.audio_edit_thread = None
        # Video logs storage: {video_path: [(timestamp, log_type, message), ...]}
        self.video_logs_lock = threading.Lock()  # Thread-safe access to video_logs
        self.video_logs = {}
        self.audio_edit_only_mode = False
        self.audio_edit_task_info = {}
        self.encoding_worker_running = False
        
        # Debounce timer for automatic saving of settings
        self.settings_save_timer = None
        

        self.manual_nvenc_active = False
        self.vmaf_worker_active = False
        self.nvenc_worker_threads = []
        self.svt_worker_threads = []
        self.nvenc_active_videos = set()
        self.nvenc_processing_videos = set()  # Videos that are already in NVENC_QUEUE or under processing
        self.nvenc_selection_lock = threading.Lock()
        self.nvenc_worker_stats_lock = threading.Lock()
        self.nvenc_worker_stats = {'completed': 0, 'failed': 0, 'needs_check': 0}
        self.svt_processing_videos = set()  # Videos that are already in SVT_QUEUE or under processing
        self.vmaf_processing_videos = set()  # Videos that are already in VMAF_QUEUE or under processing
        self.video_stop_events = {}  # {video_path: threading.Event()} - per-video stop for manual override
        self.video_stop_events_lock = threading.Lock()  # Protects access to video_stop_events
        self.manual_override_ready_events = {}  # {video_path: threading.Event()} - jelzi, hogy a GUI hozzáadta a replacement taskot

        # NEW: Task list based queue system (replaces queue.Queue)
        self.pending_svt_tasks = []      # Ordered list of pending SVT-AV1 tasks
        self.pending_nvenc_tasks = []    # Ordered list of pending NVENC tasks
        self.task_list_lock = threading.Lock()  # Thread-safe lock for both task lists
        self._cached_tree_order = []  # GUI-thread cached tree children order (for thread-safe sorting)
        
        # CRITICAL FIX #1: Thread-safe locks for encoding state management
        self.encoding_state_lock = threading.Lock()  # Protects is_encoding, encoding_worker_running, graceful_stop_requested
        self.video_items_lock = threading.Lock()  # Protects video_items dictionary access
        
        self.stop_event_lock = threading.Lock()  # Protects STOP_EVENT coordination
        
        self.col_widths = {
            '#0': 50, 'denoise': 60, 'hard_rotate': 70, 'video_name': 300, 'status': 200, 'cq': 40, 'vmaf': 40, 'psnr': 50, 'progress': 150,
            'orig_size': 70, 'new_size': 70, 'size_change': 50, 'duration': 80, 'frames': 80, 'completed_date': 120
        }
        

        
        self.encoding_queue = queue.Queue()
        self.is_encoding = False
        self.copy_thread = None  # Thread used for copying non-video files
        self.is_loading_videos = False
        self.auto_start_after_load = False  # Whether to start automatically after loading if Start button was requested
        self.last_load_errors = []
        self.current_video_index = -1
        self.graceful_stop_requested = False
        self.is_queue_loading = False  # True while queue loading thread is running
        self.queue_loading_stop_requested = False  # Set to True to cancel queue loading
        self.queue_loading_thread = None  # Reference to queue loading thread
        self.logged_invalid_subtitles = set()
        
        # Autotest mode flags (from command line --autotest*).
        autotest_enabled = os.environ.get("AV1_AUTO_TEST") == "1"
        raw_profile = (os.environ.get("AV1_AUTO_TEST_PROFILE") or "").strip().lower()
        if raw_profile not in ("mini", "medium", "maxi", "legacy"):
            raw_profile = "medium" if autotest_enabled else ""
        self.autotest_profile = raw_profile
        self.autotest_mode = bool(self.autotest_profile)
        
        self.min_vmaf = tk.DoubleVar(value=97.5)
        self.vmaf_step = tk.DoubleVar(value=0.25)
        self.max_encoded_percent = tk.DoubleVar(value=75.0)  # DoubleVar for fractional percent support
        self.max_encoded_mode = tk.StringVar(value='full')  # 'full' = entire file, 'video' = video track only
        self.svt_preset = tk.IntVar(value=2)
        self.debug_mode = tk.BooleanVar(value=False)
        self.auto_vmaf_psnr = tk.BooleanVar(value=False)
        self.resize_enabled = tk.BooleanVar(value=False)
        self.resize_height = tk.IntVar(value=1080)
        self.deband_enabled = tk.BooleanVar(value=True)
        self.force_8bit_denoised_master = tk.BooleanVar(value=False)
        self.skip_av1_files = tk.BooleanVar(value=False)
        self.vdub_validation_disabled = tk.BooleanVar(value=False)  # Disable VirtualDub2 validation
        self.nvenc_worker_count = tk.IntVar(value=1)
        self.svt_worker_count = tk.IntVar(value=1)
        self.crf_increment = tk.IntVar(value=1)  # CRF lépésköz az ab-av1 crf-search-höz
        self.max_cq_limit = tk.IntVar(value=0)  # Maximum CQ value (0 = no limit, higher = lower quality)
        
        # Audio dynamic range compression
        self.audio_compression_enabled = tk.BooleanVar(value=False)
        self.audio_compression_method = tk.StringVar(value='fast')  # 'fast' or 'dialogue'
        
        # NVENC enablement (detection of 40xx or 50xx GPU)
        nvenc_supported, gpu_name = detect_nvidia_gpu()
        self.nvenc_enabled = tk.BooleanVar(value=nvenc_supported)
        if gpu_name:
            self.detected_gpu_name = gpu_name
        else:
            self.detected_gpu_name = None
        default_nvenc_workers = 1
        if self.detected_gpu_name:
            gpu_name_upper = self.detected_gpu_name.upper()
            if "5090" in gpu_name_upper.replace(" ", ""):
                default_nvenc_workers = 3
        self.nvenc_worker_count.set(default_nvenc_workers)
        
        # Result logging
        if LOG_WRITER:
            try:
                LOG_WRITER.write("=== NVENC ENABLEMENT RESULT ===\n")
                if nvenc_supported and gpu_name:
                    LOG_WRITER.write(f"  [OK] NVENC enabled\n")
                    LOG_WRITER.write(f"  GPU: {gpu_name}\n")
                else:
                    LOG_WRITER.write(f"  [ERROR] NVENC not enabled\n")
                    if gpu_name:
                        LOG_WRITER.write(f"  GPU: {gpu_name} (not 40xx/50xx series)\n")
                    else:
                        LOG_WRITER.write(f"  GPU: Not found or not NVIDIA\n")
                LOG_WRITER.write("=====================================\n\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
        self.current_min_vmaf = float(self.min_vmaf.get())
        self.current_vmaf_step = float(self.vmaf_step.get())
        self.current_max_encoded_percent = float(self.max_encoded_percent.get())
        self.current_resize_enabled = bool(self.resize_enabled.get())
        self.current_resize_height = self.resize_height.get()
        self.current_deband_enabled = bool(self.deband_enabled.get())
        self.current_force_8bit_denoised_master = bool(self.force_8bit_denoised_master.get())
        self.current_audio_compression_enabled = bool(self.audio_compression_enabled.get())
        self.current_audio_compression_method = self.audio_compression_method.get()
        self.current_nvenc_enabled = bool(self.nvenc_enabled.get())
        self.current_nvenc_worker_count = int(self.nvenc_worker_count.get())
        self.current_svt_worker_count = int(self.svt_worker_count.get())
        
        # Rebuild worker state
        self._init_rebuild_state()
        
        # Console loggers
        self.tree = None
        self.nvenc_logger = None
        self.svt_logger = None
        self.nvenc_loggers = []
        self.nvenc_consoles = []
        self.nvenc_log_files = []
        
        # Checkbox to hide completed items
        self.hide_completed = tk.BooleanVar(value=False)
        
        # HTTP Server GUI controls
        self.http_enabled_var = tk.BooleanVar(value=http_enabled)
        self.http_port_var = tk.IntVar(value=http_port)
        self.http_server_running = False
        self.http_thread = None
        self.flask_app = None
        
        self.setup_ui()

        # Global audit logging for all user GUI interactions.
        # Entries are written to av1_recompress.log via LOG_WRITER.
        self.setup_user_interaction_logging()
        
        # HTTP Server Initialization (if enabled via command line)
        if http_enabled:
            self.start_http_server(port=http_port)
            self.http_server_running = True
        
        # Responsive layout: setting window resize event
        self.root.bind('<Configure>', self.on_window_resize)
        
        # Check and offer state load at program start
        self.root.after(100, self.check_and_offer_state_load)
        
        # Rebuild crash recovery: restore interrupted rebuilds after state load
        self.root.after(2000, self._delayed_rebuild_recovery)
        
        # Periodikus ellenőrzés elindítása (1 másodpercenként)
        # Gyorsan ellenőrzi, hogy van-e még aktív feladat, és ha nincs, visszaállítja a gombot
        self.root.after(1000, self._periodic_encoding_status_check)

        # Test mode: load videos from env var
        if os.environ.get("AV1_TEST_SOURCE"):
            test_source = os.environ.get("AV1_TEST_SOURCE")
            if os.path.exists(test_source):
                def _trigger_test_load():
                    self.source_path = Path(test_source)
                    self.source_entry.delete(0, tk.END)
                    self.source_entry.insert(0, str(self.source_path))
                    self.load_videos()
                self.root.after(1000, _trigger_test_load)
        
        # Autotest mode: profile-driven automated workflow
        if self.autotest_mode:
            if LOG_WRITER:
                LOG_WRITER.write(f"[AUTOTEST] Enabled profile: {self.autotest_profile}\n")
                LOG_WRITER.flush()
            # Start profile handler shortly after UI initialization.
            self.root.after(500, self._autotest_start_profile)
