import os
import subprocess
import sys
import re
import shutil
import random
import tempfile
import platform
import signal
from pathlib import Path

from PIL import Image
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import scrolledtext
from contextlib import contextmanager
import ctypes
import sqlite3
import json  # FFprobe JSON kimenetéhez szükséges
from datetime import datetime
import locale
import multiprocessing
import traceback
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Common imports used by mixins
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Import i18n functions
from .i18n import t, format_localized_number, is_status_completed, is_status_failed, is_status_needs_check, is_status_rebuild, is_status_rebuild_queued, normalize_status_to_code, status_code_to_localized, parse_size_to_bytes, normalize_number_string, format_size_auto, to_decorative_text, from_decorative_text

# Import core functions and variables
from .core_preamble_and_imports import (
    VMAF_QUEUE, AUDIO_EDIT_QUEUE, SVT_QUEUE, NVENC_QUEUE,
    LOAD_DEBUG, FFPROBE_PATH, CPU_WORKER_LOCK, STOP_EVENT,
    set_low_priority
)

# Import logging functions
from .core_paths_tools_logging import load_debug_log, get_startup_info, console_redirect, debug_pause, get_crf_increment, cleanup_ab_av1_temp_dirs, get_log_writer

# Create a proxy for LOG_WRITER that dynamically fetches it
# This is necessary because the LOG_WRITER is set after module import time
class _LogWriterProxy:
    """Thread-safe proxy for LOG_WRITER with automatic error handling.
    
    This proxy ensures that:
    1. All write/flush operations are thread-safe (multiple workers can log safely)
    2. Closed file state is checked before every operation
    3. All I/O errors are gracefully handled (no crashes)
    4. Workers never block on logging issues
    """
    
    def __init__(self):
        self._lock = threading.RLock()  # Re-entrant lock for nested calls
    
    def write(self, msg):
        """Thread-safe write with automatic error handling."""
        with self._lock:
            try:
                writer = get_log_writer()
                if writer is None:
                    return  # Gracefully ignore if not initialized
                
                # Check if file is closed
                if hasattr(writer, 'closed') and writer.closed:
                    return  # Gracefully ignore closed file
                
                writer.write(msg)
            except (OSError, IOError, ValueError, AttributeError):
                # File was closed during write, or other I/O error - ignore gracefully
                # This prevents worker crashes when app is closing
                # Attempt to write to stderr if possible
                try:
                    sys.stderr.write(f"Proxy log write error: {msg}\n")
                except Exception:
                    pass
    
    def flush(self):
        """Thread-safe flush with automatic error handling."""
        with self._lock:
            try:
                writer = get_log_writer()
                if writer is None:
                    return  # Gracefully ignore if not initialized
                
                # Check if file is closed
                if hasattr(writer, 'closed') and writer.closed:
                    return  # Gracefully ignore closed file
                
                writer.flush()
            except (OSError, IOError, ValueError, AttributeError):
                # File was closed during flush, or other I/O error - ignore gracefully
                pass
    
    def close(self):
        """Thread-safe close with automatic error handling."""
        with self._lock:
            try:
                writer = get_log_writer()
                if writer is None:
                    return

                # Check if file is already closed
                if hasattr(writer, 'closed') and writer.closed:
                    return

                # Explicitly close the file
                if hasattr(writer, 'close'):
                    writer.close()
            except (OSError, IOError, ValueError, AttributeError):
                # Ignore close errors gracefully
                pass
    
    def __getattr__(self, name):
        """Fallback for other attributes (e.g., closed check)."""
        with self._lock:
            writer = get_log_writer()
            if writer is None:
                if name == 'closed':
                    return True  # Treat None as closed
                raise AttributeError(f'LOG_WRITER is not initialized (tried to access {name})')
            return getattr(writer, name)
    
    def __bool__(self):
        """Check if writer is available and open."""
        try:
            writer = get_log_writer()
            if writer is None:
                return False
            if hasattr(writer, 'closed'):
                return not writer.closed
            return True
        except Exception:
            # Catch all exceptions in __bool__ to avoid crashes
            return False

LOG_WRITER = _LogWriterProxy()

# Ensure core module uses the same proxy writer
# This fix resolves the ambiguity where core modules might see None while GUI uses the proxy
from . import core_preamble_and_imports
core_preamble_and_imports.LOG_WRITER = LOG_WRITER

# Import audio/video operations
from .core_audio_video_ops import (
    get_output_filename, get_output_file_info,
    check_source_video_with_vdub2, copy_video_fallback, get_copy_filename,
    apply_faststart, get_audio_streams_total_size_mb, create_denoised_lossless_master,
    get_hybrid_paths, create_smdegrain_master, cleanup_smdegrain_files,
    extract_audio_tracks_with_metadata, merge_video_audio_subtitles, cleanup_extracted_audio_files,
    extract_settings_from_file
)

# Import metrics and validation functions
from .core_metrics_and_validation import (
    calculate_full_vmaf, update_video_metadata_vmaf,
    format_metric_value, format_seconds_hms
)

# Import file operations
from .core_workers_and_flows import open_video_file, encode_video, encode_single_attempt
