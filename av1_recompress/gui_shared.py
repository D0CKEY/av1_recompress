from .gui_imports import *

# Import exception classes from core module to avoid duplicate definitions
from .core_paths_tools_logging import EncodingStopped, NoSuitableCRFFound, NVENCFallbackRequired, debug_print

# ===== GLOBAL EXIT FLAG =====
# Thread-safe flag to signal application shutdown to all threads
# This is critical to avoid deadlocks during exit

class _AppShutdownState:
    """Thread-safe state manager for application shutdown initialization and access."""
    def __init__(self):
        self._lock = threading.Lock()
        self._is_closing = False
        
    def set_closing(self):
        with self._lock:
            self._is_closing = True
            
    def is_closing(self):
        with self._lock:
            return self._is_closing

# Initialize the state manager
# Encapsulates state to prevent initialization race conditions
_APP_SHUTDOWN = _AppShutdownState()

def set_app_closing():
    """Signal all threads that the application is shutting down.
    
    This should be called at the very beginning of on_closing() to ensure
    all threads can check this flag and exit gracefully without waiting
    for root.after() callbacks that will never execute.
    """
    _APP_SHUTDOWN.set_closing()

def is_app_closing():
    """Check if the application is shutting down.
    
    Threads should check this flag regularly and exit immediately if True,
    avoiding any operations that depend on the GUI (e.g., root.after() calls).
    
    Returns:
        bool: True if application is closing, False otherwise.
    """
    return _APP_SHUTDOWN.is_closing()


def make_video_log_key(video_path):
    """Build a fast, stable dictionary key for per-video logs.

    IMPORTANT:
    - Do NOT call Path.resolve() here: on network paths it can block the GUI.
    - Keep it purely string-based normalization.
    """
    if video_path is None:
        return ""
    try:
        raw = os.fspath(video_path)
    except (TypeError, ValueError):
        raw = str(video_path)
    if not raw:
        return ""
    try:
        return os.path.normcase(os.path.normpath(raw))
    except (TypeError, ValueError, OSError):
        return str(raw)


def extract_source_cell_value(raw_value):
    """Extract source-side part from a combined 'source / target' cell value."""
    if raw_value is None:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    if " / " in text:
        return text.split(" / ", 1)[0].strip()
    if "/" in text:
        left, right = text.split("/", 1)
        if left.strip() and right.strip():
            return left.strip()
    return text


def extract_target_cell_value(raw_value):
    """Extract target-side part from a combined 'source / target' cell value."""
    if raw_value is None:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    if " / " in text:
        return text.split(" / ", 1)[1].strip()
    if "/" in text:
        left, right = text.split("/", 1)
        if left.strip() and right.strip():
            return right.strip()
    return ""


def _parse_duration_text_to_seconds(duration_text):
    if not duration_text or duration_text == "-":
        return None
    try:
        dur_text = str(duration_text).strip().lower().replace(",", ".")
        if dur_text.endswith("s"):
            dur_text = dur_text[:-1].strip()
        if ":" in dur_text:
            parts = [p.strip() for p in dur_text.split(":")]
            if len(parts) == 3:
                h, m, s = parts
                return int(h) * 3600 + int(m) * 60 + float(s)
            if len(parts) == 2:
                m, s = parts
                return int(m) * 60 + float(s)
            return None
        return float(dur_text) if dur_text else None
    except (ValueError, TypeError, AttributeError):
        return None


def _parse_frame_text_to_count(frame_text):
    if not frame_text or frame_text == "-":
        return None
    digits = "".join(ch for ch in str(frame_text) if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except (ValueError, TypeError):
        return None


def parse_duration_cell_source_seconds(raw_value):
    """Parse duration cell (supports 'HH:MM:SS' and 'source / target' form)."""
    return _parse_duration_text_to_seconds(extract_source_cell_value(raw_value))


def parse_duration_cell_target_seconds(raw_value):
    """Parse target-side duration from 'source / target' cell value."""
    return _parse_duration_text_to_seconds(extract_target_cell_value(raw_value))


def parse_frames_cell_source_count(raw_value):
    """Parse frame-count cell (supports 'source / target' form)."""
    return _parse_frame_text_to_count(extract_source_cell_value(raw_value))


def parse_frames_cell_target_count(raw_value):
    """Parse target-side frame-count from 'source / target' cell value."""
    return _parse_frame_text_to_count(extract_target_cell_value(raw_value))


def normalize_hard_rotate_degrees(value):
    """Normalize hard-rotate value to one of: 0, 90, 180, 270."""
    if value is None:
        return 0
    try:
        text = str(value).strip()
        if not text:
            return 0
        text = text.replace("°", "").strip()
        degrees = int(float(text)) % 360
        if degrees in (0, 90, 180, 270):
            return degrees
    except (ValueError, TypeError, AttributeError):
        pass
    return 0


def hard_rotate_to_display(value):
    """Format normalized hard-rotate value for tree display."""
    return str(normalize_hard_rotate_degrees(value))


def normalize_denoise_level(value):
    """Normalize denoise level to one of: 0, 1, 2, 3, 4."""
    if value is None:
        return 0
    try:
        level = int(float(str(value).strip()))
    except (ValueError, TypeError, AttributeError):
        return 0
    return level if level in (0, 1, 2, 3, 4) else 0


def denoise_level_to_display(value):
    """Format normalized denoise level for tree display."""
    level = normalize_denoise_level(value)
    if level == 4:
        return "[OK][OK][OK][OK]"  # Ultra strong
    if level == 3:
        return "[OK][OK][OK]"  # Very strong
    if level == 1:
        return "[OK][OK]"      # Strong
    if level == 2:
        return "[OK]"          # Light
    return ""


def display_to_denoise_level(value):
    """Parse tree display value into normalized denoise level (0/1/2/3/4)."""
    if value is None:
        return 0
    text = str(value)
    if "[OK][OK][OK][OK]" in text:
        return 4
    if "[OK][OK][OK]" in text:
        return 3
    if "[OK][OK]" in text:
        return 1
    if "[OK]" in text:
        return 2
    return 0


def infer_denoise_level_from_filter_info(denoise_info):
    """Infer denoise level from Settings/Filters metadata string.

    Parses ffprobe-extracted filter information to determine which
    denoise level was used during encoding.

    Returns:
        int or None: Denoise level (1=Strong, 2=Light, 3=Very strong,
                     4=Ultra strong), or None if no denoise filter detected.
    """
    if not denoise_info:
        return None

    info = str(denoise_info).strip().lower()
    if not info:
        return None

    explicit_level_match = re.search(
        r"\bdenoise(?:[_\s-]*level)?\s*[:=]\s*([0-4])\b",
        info
    )
    if explicit_level_match:
        return normalize_denoise_level(explicit_level_match.group(1)) or None

    explicit_name_match = re.search(
        r"\bdenoise(?:[_\s-]*level)?\s*[:=]\s*(ultra[_\s-]*strong|very[_\s-]*strong|strong|light)\b",
        info
    )
    if explicit_name_match:
        level_name = explicit_name_match.group(1).replace("_", " ").replace("-", " ")
        if level_name == "light":
            return 2
        if level_name == "strong":
            return 1
        if level_name == "very strong":
            return 3
        if level_name == "ultra strong":
            return 4

    # FFmpeg vaguedenoiser-based masters
    if "vaguedenoiser" in info:
        threshold_match = re.search(r"threshold\s*=\s*([0-9]+(?:\.[0-9]+)?)", info)
        if threshold_match:
            try:
                threshold_val = float(threshold_match.group(1))
                if threshold_val <= 2.0:
                    return 2  # Light
                if threshold_val <= 4.0:
                    return 1  # Strong
                if threshold_val <= 6.0:
                    return 3  # Very strong
                return 4      # Ultra strong
            except (TypeError, ValueError):
                pass
        # If filter exists but threshold is missing, assume strong.
        return 1

    # VapourSynth SMDegrain-based masters
    if "smdegrain" in info:
        tr_match = re.search(r"\btr\s*=\s*([0-9]+)", info)
        if tr_match:
            try:
                tr_val = int(tr_match.group(1))
                if tr_val <= 1:
                    return 2  # Light
                if tr_val <= 2:
                    return 1  # Strong
                if tr_val <= 3:
                    return 3  # Very strong
                return 4      # Ultra strong
            except (TypeError, ValueError):
                pass
        thsad_match = re.search(r"\bthsad\s*=\s*([0-9]+(?:\.[0-9]+)?)", info)
        if thsad_match:
            try:
                thsad_val = float(thsad_match.group(1))
                if thsad_val <= 150.0:
                    return 2  # Light
                if thsad_val <= 300.0:
                    return 1  # Strong
                if thsad_val <= 450.0:
                    return 3  # Very strong
                return 4      # Ultra strong
            except (TypeError, ValueError):
                pass
        return 1

    return None


def infer_manual_cq_from_settings(settings_info, cq_value=None):
    """Infer manual CQ metadata from the encoded Settings tag.

    Manual encodes are written as "Planned VMAF: Manual". Max-CQ capped
    automatic encodes intentionally use the same marker because the selected
    CRF was user-constrained rather than purely VMAF-search selected.
    """
    if not settings_info:
        return {}

    info = str(settings_info).strip()
    if not info:
        return {}

    if not re.search(r"\bplanned\s+vmaf\s*:\s*manual\b", info, re.IGNORECASE):
        return {}

    manual_cq = None
    if cq_value not in (None, "", "-"):
        try:
            manual_cq = int(float(str(cq_value).replace(",", ".")))
        except (TypeError, ValueError):
            manual_cq = None

    if manual_cq is None:
        cq_match = re.search(r"\b(?:CQ|CRF)\s*:\s*([0-9]+(?:[.,][0-9]+)?)", info, re.IGNORECASE)
        if cq_match:
            try:
                manual_cq = int(float(cq_match.group(1).replace(",", ".")))
            except (TypeError, ValueError):
                manual_cq = None

    if manual_cq is None:
        return {}

    has_vmaf = bool(re.search(r"\bactual\s+vmaf\s*:", info, re.IGNORECASE))
    has_psnr = bool(re.search(r"\bpsnr\s*:", info, re.IGNORECASE))
    if has_vmaf and has_psnr:
        manual_quality_check = "both"
    elif has_vmaf:
        manual_quality_check = "vmaf"
    elif has_psnr:
        manual_quality_check = "psnr"
    else:
        manual_quality_check = "none"

    return {
        "manual_cq_range": "metadata",
        "manual_cq_value": manual_cq,
        "manual_quality_check": manual_quality_check,
    }


class ConsoleLogger:
    """Thread-safe console logger for tkinter Text widget and log file"""

    def __init__(self, text_widget, gui_queue, log_file=None, log_files_list=None, logger_index=0):
        self.lock = threading.Lock()
        self.text_widget = text_widget
        self.gui_queue = gui_queue
        self.log_file = log_file  # Log file object (backward compatibility)
        self.log_files_list = log_files_list  # List of log files (selection based on logger_index)
        self.logger_index = logger_index  # Logger object's own index (does not change)
        self.encoder_type = None  # 'nvenc' or 'svt'
        self.buffer = ""  # Buffer for partial lines
        self._file_line_start = True  # Timestamp state for file output
        self.worker_index = 0  # Worker index (for queue messages)
        self.current_video_path = None  # Current video path being processed (for log storage)

    def set_encoder_type(self, encoder_type):
        self.encoder_type = encoder_type

    def set_worker_index(self, worker_index):
        try:
            self.worker_index = int(worker_index)
        except (ValueError, TypeError):
            self.worker_index = 0
    
    def set_current_video_path(self, video_path):
        """Set the current video path being processed (for log storage)"""
        with self.lock:
            self.current_video_path = video_path

    def _get_log_file(self):
        """Returns the appropriate log file based on logger index"""
        # If log_files_list exists, choose based on logger_index (not worker_index!)
        # This ensures that each logger object always writes to the same log file,
        # regardless of which worker uses it
        if self.log_files_list and self.encoder_type in ('nvenc', 'svt'):
            if len(self.log_files_list) > 0:
                # Choose based on logger index to avoid race condition
                log_file_idx = self.logger_index % len(self.log_files_list)
                return self.log_files_list[log_file_idx]
        # Backward compatibility: if no list, use original log_file
        return self.log_file

    def _write_payload_to_file(self, payload):
        """Write a complete payload chunk to the selected worker log file."""
        log_file = self._get_log_file()
        if not log_file:
            return
        try:
            log_file.write(self._timestamp_for_file(payload))
            log_file.flush()
        except (OSError, IOError, AttributeError):
            pass

    def _timestamp_for_file(self, payload):
        """Prefix each log line with datetime for worker file logs."""
        if not payload:
            return payload
        chunks = payload.splitlines(keepends=True)
        if not chunks:
            chunks = [payload]
        out = []
        for chunk in chunks:
            if self._file_line_start and chunk.strip("\r\n"):
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                out.append(f"[{ts}] ")
            out.append(chunk)
            self._file_line_start = chunk.endswith('\n')
        return ''.join(out)

    def _queue_payload(self, payload):
        """Queue payload for GUI console rendering and per-video in-memory logs."""
        if self.encoder_type == 'nvenc':
            self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))
        elif self.encoder_type == 'svt':
            self.gui_queue.put(("svt_log", self.worker_index, self.logger_index, payload, self.current_video_path))
        else:
            self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))

    def write(self, message):
        """Thread-safe writing to console and log file - with newline handling fix"""
        if not message:
            return

        with self.lock:
            # Normalize CR-only progress lines to LF-based chunks.
            self.buffer += message.replace('\r', '\n')

            # Emit only complete lines here to avoid partial-line duplication.
            lines = self.buffer.split('\n')
            self.buffer = lines.pop()  # Keep last partial line

            for line in lines:
                # Skip empty lines to avoid clutter in logs
                if not line.strip():
                    continue
                payload = line + '\n'
                self._write_payload_to_file(payload)
                self._queue_payload(payload)

    def flush(self):
        """Flush buffer - ONLY to the console corresponding to its encoder type"""
        with self.lock:
            if not self.buffer:
                return

            payload = self.buffer + '\n'
            self.buffer = ""

            if payload.strip():
                self._write_payload_to_file(payload)
                self._queue_payload(payload)
class ThreadSafeStdoutRouter:
    """Thread-separated stdout redirection."""

    def __init__(self, fallback_stream):
        self._fallback = fallback_stream
        self._local = threading.local()
        self.encoding = getattr(fallback_stream, 'encoding', None)
        self.errors = getattr(fallback_stream, 'errors', None)

    def set_logger(self, logger):
        stack = self._get_stack()
        stack.append(logger)

    def clear_logger(self):
        stack = self._get_stack()
        if stack:
            stack.pop()

    def _current_logger(self):
        stack = self._get_stack()
        if stack:
            return stack[-1]
        return None

    def _get_stack(self):
        stack = getattr(self._local, 'logger_stack', None)
        if stack is None:
            stack = []
            self._local.logger_stack = stack
        return stack

    def write(self, message):
        logger = self._current_logger()
        if logger is not None:
            logger.write(message)
        else:
            if self._fallback is not None:
                try:
                    self._fallback.write(message)
                except (AttributeError, OSError, IOError):
                    # Ha a fallback nem írható, csendben elnyeljük
                    pass

    def flush(self):
        logger = self._current_logger()
        if logger is not None and hasattr(logger, 'flush'):
            logger.flush()
        else:
            if self._fallback is not None:
                try:
                    self._fallback.flush()
                except (AttributeError, OSError, IOError):
                    # Ha a fallback nem flush-olható, csendben elnyeljük
                    pass

    def isatty(self):
        if self._fallback is not None:
            try:
                return self._fallback.isatty()
            except (AttributeError, OSError, IOError):
                return False
        return False

    def fileno(self):
        if self._fallback is not None:
            try:
                return self._fallback.fileno()
            except (AttributeError, OSError, IOError):
                raise OSError("fileno() not available")
        raise OSError("fileno() not available")

    def __getattr__(self, item):
        if self._fallback is not None:
            return getattr(self._fallback, item)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{item}'")
