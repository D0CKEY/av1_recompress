"""Shared worker utilities for SVT and NVENC encoding workers.

This module contains common functionality extracted from gui_svt_worker.py
and gui_nvenc_worker.py to eliminate code duplication.
"""

import threading
import time
from pathlib import Path
from typing import Optional, Callable, Any, Dict, Tuple, TYPE_CHECKING

from .i18n import TRANSLATIONS, t

if TYPE_CHECKING:
    from queue import Queue


class CombinedStopEvent(threading.Event):
    """Wrapper that combines per-video stop event with global STOP_EVENT.

    This allows manual override to stop only ONE video, while global Stop button
    still stops ALL videos. The denoising/encoding functions check is_set(),
    which returns True if EITHER event is set.
    """
    def __init__(self, video_event, global_event):
        super().__init__()
        self.video_event = video_event
        self.global_event = global_event

    def is_set(self):
        """Returns True if either video-specific or global stop is requested."""
        if self.video_event.is_set():
            return True
        if self.global_event.is_set():
            return True
        return False

    def set(self):
        """Sets the video-specific event (for manual override)."""
        self.video_event.set()
        super().set()

    def clear(self):
        """Clears the video-specific event."""
        self.video_event.clear()
        super().clear()

    def wait(self, timeout=None):
        """Wait until either event is set or timeout occurs.
        
        Optimized to use Event.wait() on the primary event to reduce polling overhead
        and improve accuracy.
        """
        # Fast path if already set
        if self.is_set():
            return True
        
        # Calculate deadline
        deadline = time.time() + timeout if timeout is not None else None
        
        while True:
            # Determine how long to wait in this iteration
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    # Final check before returning timeout
                    return self.is_set()
                # Poll interval: max 0.05s, or remaining time if shorter
                wait_interval = min(remaining, 0.05)
            else:
                # No timeout: poll every 0.05s
                wait_interval = 0.05
            
            # Use wait() on one event (video_event) to block efficiently
            # This makes reaction to video_event instant
            if self.video_event.wait(wait_interval):
                return True
            
            # Check the other event (global_event)
            if self.global_event.is_set():
                return True


# ============================================================================
# DENOISE LEVEL UTILITIES
# ============================================================================

def read_denoise_level_file(level_file: Path) -> int:
    """Read denoise level from sidecar file next to denoised master.

    Args:
        level_file: Path to the .denoise_level sidecar file.

    Returns:
        int: Stored denoise level, or -1 if file doesn't exist or is invalid.
    """
    try:
        if level_file.exists():
            return int(level_file.read_text().strip())
    except (ValueError, OSError):
        pass
    return -1


def write_denoise_level_file(level_file: Path, denoise_level: int) -> None:
    """Write denoise level to sidecar file next to denoised master.

    Args:
        level_file: Path to the .denoise_level sidecar file.
        denoise_level: The denoise level (0-4) to store.
    """
    try:
        level_file.write_text(str(denoise_level))
    except OSError:
        pass


def read_denoise_params_file(params_file: Path) -> str:
    """Read full denoise filter metadata from a sidecar file."""
    try:
        if params_file.exists():
            return params_file.read_text(encoding='utf-8', errors='replace').strip()
    except OSError:
        pass
    return ""


def write_denoise_params_file(params_file: Path, denoise_params: str) -> None:
    """Write full denoise filter metadata next to denoised master."""
    try:
        params_text = str(denoise_params or "").strip()
        if params_text:
            params_file.write_text(params_text, encoding='utf-8')
    except OSError:
        pass


def get_denoise_level_from_task(
    task: Dict[str, Any],
    column_index: Dict[str, int],
    video_denoise_enabled: Optional[Dict[Path, int]] = None,
    video_denoise_lock: Optional[threading.Lock] = None
) -> int:
    """Extract denoise level from task dictionary.

    Args:
        task: Task dictionary containing cached_values
        column_index: Dictionary mapping column names to indices (COLUMN_INDEX)
        video_denoise_enabled: Optional dict tracking per-video denoise state
        video_denoise_lock: Lock for thread-safe access to video_denoise_enabled

    Returns:
        int: Denoise level (0=disabled, 1=strong, 2=light, 3=very-strong, 4=ultra-strong)
    """
    from .gui_shared import display_to_denoise_level, normalize_denoise_level

    explicit_denoise_level = normalize_denoise_level(task.get('denoise_enabled', 0))
    if explicit_denoise_level > 0:
        return explicit_denoise_level

    denoise_level = 0

    try:
        cached_values = task.get('cached_values') or []
        denoise_idx = column_index.get('denoise', -1)
        if denoise_idx >= 0 and len(cached_values) > denoise_idx:
            denoise_str = cached_values[denoise_idx]
            denoise_level = display_to_denoise_level(denoise_str)
    except (KeyError, IndexError, AttributeError):
        pass

    # Fallback to internal dict if available
    if denoise_level == 0 and video_denoise_enabled is not None:
        video_path = task.get('video_path')
        if video_path:
            if video_denoise_lock:
                with video_denoise_lock:
                    denoise_level = normalize_denoise_level(video_denoise_enabled.get(video_path, 0))
            else:
                denoise_level = normalize_denoise_level(video_denoise_enabled.get(video_path, 0))

    return denoise_level


def get_hard_rotate_degrees_from_task(
    task: Dict[str, Any],
    column_index: Dict[str, int],
    video_hard_rotate_degrees: Optional[Dict[Path, int]] = None,
    video_hard_rotate_lock: Optional[threading.Lock] = None
) -> int:
    """Extract hard-rotate degrees from task dictionary."""
    from .gui_shared import normalize_hard_rotate_degrees

    rotate_degrees = normalize_hard_rotate_degrees(task.get('hard_rotate_degrees', 0))
    if rotate_degrees in (90, 180, 270):
        return rotate_degrees

    try:
        cached_values = task.get('cached_values', [])
        rotate_idx = column_index.get('hard_rotate', -1)
        if rotate_idx >= 0 and len(cached_values) > rotate_idx:
            rotate_degrees = normalize_hard_rotate_degrees(cached_values[rotate_idx])
            if rotate_degrees in (90, 180, 270):
                return rotate_degrees
    except (KeyError, IndexError, AttributeError):
        pass

    video_path = task.get('video_path')
    if video_path and video_hard_rotate_degrees is not None:
        try:
            if video_hard_rotate_lock:
                with video_hard_rotate_lock:
                    rotate_degrees = normalize_hard_rotate_degrees(video_hard_rotate_degrees.get(video_path, 0))
            else:
                rotate_degrees = normalize_hard_rotate_degrees(video_hard_rotate_degrees.get(video_path, 0))
        except Exception:
            rotate_degrees = 0

    return rotate_degrees if rotate_degrees in (90, 180, 270) else 0


# ============================================================================
# SIZE CALCULATION UTILITIES
# ============================================================================

def calculate_effective_max_encoded(
    original_video_path: Path,
    denoised_master_path: Optional[Path],
    max_encoded: float,
    max_encoded_mode: str,
    get_video_stream_size_bytes_exact: Callable[[Path], Optional[int]],
    get_audio_streams_total_size_mb: Callable[[Path], float],
    logger: Any = None,
    console_redirect: Optional[Callable] = None
) -> Tuple[float, Optional[float]]:
    """Calculate effective max_encoded percentage for ab-av1.

    This handles the size correction logic for both denoised masters
    and normal encoding with 'full video' mode.

    Args:
        original_video_path: Path to the original source video
        denoised_master_path: Path to denoised master (None if not using denoise)
        max_encoded: User-specified max encoded percentage
        max_encoded_mode: 'full' or 'video' mode
        get_video_stream_size_bytes_exact: Function to get exact video stream size
        get_audio_streams_total_size_mb: Function to get audio streams size
        logger: Logger for console output
        console_redirect: Context manager for redirecting console output

    Returns:
        Tuple of (effective_max_encoded, audio_size_mb)
    """
    effective_max_encoded = max_encoded
    audio_size_mb: Optional[float] = None

    def log_print(msg: str):
        if logger and console_redirect:
            with console_redirect(logger):
                print(msg)

    try:
        if denoised_master_path and denoised_master_path.exists():
            # ZAJSZŰRÉS esetén MINDIG kell korrekció
            orig_size = original_video_path.stat().st_size
            master_size = denoised_master_path.stat().st_size
            audio_size_mb = 0.0
            audio_size_bytes = 0

            if master_size > 0:
                # PONTOS videó stream mérés (packet scan)
                log_print("[STATS] Pontos videó stream méret mérése (packet scan)...")

                exact_video_size = get_video_stream_size_bytes_exact(original_video_path)

                if exact_video_size:
                    original_video_size = exact_video_size
                    estimated_audio_overhead = max(0, orig_size - original_video_size)
                    if audio_size_bytes == 0 or estimated_audio_overhead < audio_size_bytes:
                        audio_size_bytes = estimated_audio_overhead
                        audio_size_mb = audio_size_bytes / (1024 * 1024)

                    log_print(f"   [OK] Videó stream méret: {exact_video_size/(1024**2):.1f} MB")
                    log_print(f"   [OK] Korrigált egyéb adat (hang+overhead): {audio_size_mb:.1f} MB")
                else:
                    if original_video_path:
                        try:
                            audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
                            audio_size_bytes = audio_size_mb * 1024 * 1024
                        except (OSError, IOError, AttributeError, ValueError):
                            audio_size_mb = 0.0
                            audio_size_bytes = 0
                    original_video_size = max(0, orig_size - audio_size_bytes)
                    if original_video_size <= 0:
                        original_video_size = orig_size
                    log_print(f"   [WARN] Pontos mérés sikertelen, becsült méret használata: {original_video_size/(1024**2):.1f} MB")

                original_video_size = max(0, original_video_size)

                if max_encoded_mode == 'video':
                    target_video_size = original_video_size * (max_encoded / 100.0)
                    expected_final_size = target_video_size + audio_size_bytes
                else:
                    target_final_size = orig_size * (max_encoded / 100.0)
                    target_video_size = target_final_size - audio_size_bytes
                    target_video_size = max(0, target_video_size)
                    expected_final_size = target_final_size

                effective_max_encoded = (target_video_size / master_size) * 100.0 if master_size > 0 else max_encoded
                effective_max_encoded = round(effective_max_encoded, 2)
                effective_max_encoded = max(0.01, effective_max_encoded)

                mode_label = t('max_encoded_mode_video') if max_encoded_mode == 'video' else t('max_encoded_mode_full')
                log_print(f"⚖ Zajszűrés méret korrekció ({mode_label} mód):")
                log_print(f"   Eredeti forrásfájl: {orig_size/(1024**2):.1f} MB")
                log_print(f"   Eredeti videó (becsült): {original_video_size/(1024**2):.1f} MB")
                log_print(f"   Mesterfájl: {master_size/(1024**2):.1f} MB")
                log_print(f"   Hang mérete (másolásra kerül): {audio_size_mb:.1f} MB")
                log_print(f"   Cél videó méret: {target_video_size/(1024**2):.1f} MB")
                log_print(f"   Várható végső fájlméret: {expected_final_size/(1024**2):.1f} MB ({(expected_final_size/orig_size)*100:.1f}%)")
                log_print(f"   -> ab-av1 max-encoded-percent: {effective_max_encoded:.2f}%")

        elif max_encoded_mode == 'full':
            # NORMÁL ÁTKÓDOLÁS + teljes videó mód: korrekció kell
            orig_size = original_video_path.stat().st_size
            audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
            audio_size_bytes = audio_size_mb * 1024 * 1024

            original_video_size = orig_size - audio_size_bytes
            original_video_size = max(0, original_video_size)

            if original_video_size > 0:
                target_final_size = orig_size * (max_encoded / 100.0)
                target_video_size = target_final_size - audio_size_bytes
                target_video_size = max(0, target_video_size)

                effective_max_encoded = (target_video_size / original_video_size) * 100.0
                effective_max_encoded = round(effective_max_encoded, 2)
                effective_max_encoded = max(0.01, effective_max_encoded)

                log_print(f"⚖ Normál méret korrekció (Teljes videó mód):")
                log_print(f"   Eredeti forrásfájl: {orig_size/(1024**2):.1f} MB")
                log_print(f"   Eredeti videó (becsült): {original_video_size/(1024**2):.1f} MB")
                log_print(f"   Hang mérete (változatlan): {audio_size_mb:.1f} MB")
                log_print(f"   Cél végső méret ({max_encoded}%): {target_final_size/(1024**2):.1f} MB")
                log_print(f"   Cél videó méret: {target_video_size/(1024**2):.1f} MB")
                log_print(f"   -> ab-av1 max-encoded-percent: {effective_max_encoded:.2f}%")
        else:
            # Videósáv mód, normál átkódolás: nincs korrekció
            log_print(f"⚖ Videósáv mód: ab-av1 max-encoded-percent: {max_encoded}% (korrekció nélkül)")

    except (OSError, IOError, AttributeError, KeyError, ValueError) as e:
        log_print(f"[WARN] Méret korrekció hiba: {e}")

    return effective_max_encoded, audio_size_mb


def format_predicted_size_with_audio(
    predicted_size_mb: Optional[float],
    audio_size_mb: Optional[float],
    original_video_path: Path,
    get_audio_streams_total_size_mb: Callable[[Path], float],
    get_video_track_size_bytes: Callable[[Path], Optional[int]],
    format_localized_number: Callable[[float, int], str],
    logger: Any = None,
    console_redirect: Optional[Callable] = None
) -> Tuple[str, float]:
    """Format predicted file size including audio overhead.

    Args:
        predicted_size_mb: Predicted video-only size in MB
        audio_size_mb: Pre-calculated audio size (None to calculate)
        original_video_path: Path to original source for audio calculation
        get_audio_streams_total_size_mb: Function to get audio size
        get_video_track_size_bytes: Function to get video track size
        format_localized_number: Localized number formatter
        logger: Logger for console output
        console_redirect: Context manager for redirecting console output

    Returns:
        Tuple of (formatted_size_string, audio_size_mb)
    """
    if predicted_size_mb is None or predicted_size_mb <= 0:
        return "-", 0.0

    current_audio_size_mb = 0.0

    if audio_size_mb is not None:
        current_audio_size_mb = audio_size_mb
    else:
        try:
            total_file_size = original_video_path.stat().st_size
            video_track_size = get_video_track_size_bytes(original_video_path)

            if video_track_size and video_track_size > 0:
                audio_and_extras_bytes = total_file_size - video_track_size
                current_audio_size_mb = max(0, audio_and_extras_bytes) / (1024 * 1024)
            else:
                current_audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
        except (OSError, IOError, ValueError, AttributeError):
            try:
                current_audio_size_mb = get_audio_streams_total_size_mb(original_video_path)
            except (OSError, IOError, ValueError, AttributeError):
                current_audio_size_mb = 0.0

    total_predicted_mb = predicted_size_mb + current_audio_size_mb
    predicted_size_str = f"~{format_localized_number(total_predicted_mb, 1)} MB"

    if logger and console_redirect:
        with console_redirect(logger):
            print(f"[STATS] Becsült végső méret: videó ~{format_localized_number(predicted_size_mb, 1)} MB + hang ~{format_localized_number(current_audio_size_mb, 1)} MB = ~{format_localized_number(total_predicted_mb, 1)} MB")

    return predicted_size_str, current_audio_size_mb


# ============================================================================
# DENOISE PROGRESS CALLBACK FACTORY
# ============================================================================

def create_denoise_progress_callback(
    encoding_queue: 'Queue',
    item_id: str,
    denoised_master_path: Path,
    format_localized_number: Callable[[float, int], str],
    t_func: Callable[[str], str]
) -> Callable:
    """Create a denoise progress callback function.

    This factory creates a callback that updates the GUI progress column
    during denoising operations.

    Args:
        encoding_queue: Queue for GUI updates
        item_id: Tree item ID for updates
        denoised_master_path: Path to denoised master (for size updates)
        format_localized_number: Localized number formatter
        t_func: Translation function

    Returns:
        Progress callback function compatible with denoising functions
    """
    def denoise_progress_callback(
        current_frame: int,
        total_frames: Optional[int],
        fps: float,
        speed: float,
        current_time_str: Optional[str] = None,
        total_duration_sec: Optional[float] = None
    ):
        """Progress callback for denoising - shows time/total format."""
        try:
            progress_text = ""

            # If time info is available, show "current / total" format
            if current_time_str and total_duration_sec:
                try:
                    # Format total duration to HH:MM:SS
                    m, s = divmod(int(total_duration_sec), 60)
                    h, m = divmod(m, 60)
                    total_time_str = f"{h:02d}:{m:02d}:{s:02d}"

                    # Cut decimal part from current time
                    current_time_display = current_time_str.split('.')[0]

                    progress_text = f"{current_time_display} / {total_time_str}"
                except (ValueError, TypeError, AttributeError):
                    pass

            # Use percentage if time calculation failed or not available
            if not progress_text:
                if total_frames and total_frames > 0:
                    percent = (current_frame / total_frames) * 100
                    progress_text = f"{format_localized_number(percent, 1)}%"
                else:
                    progress_text = f"{current_frame:,} {t_func('txt_frames')}"

            # Update progress column
            try:
                encoding_queue.put_nowait(("progress", item_id, progress_text))
            except (AttributeError, TypeError, ValueError):
                pass

            # Update new_size with current master file size
            try:
                if denoised_master_path and denoised_master_path.exists():
                    current_denoised_size_mb = denoised_master_path.stat().st_size / (1024 * 1024)
                    encoding_queue.put_nowait(("update_new_size", item_id, f"{current_denoised_size_mb:.2f} MB"))
            except (OSError, IOError, AttributeError):
                pass

        except (OSError, IOError, AttributeError):
            pass

    return denoise_progress_callback


# ============================================================================
# WORKER LOOP UTILITIES
# ============================================================================

def should_worker_exit(
    final_task_taken: bool,
    worker_index: int,
    worker_type: str,
    is_app_closing_func: Callable[[], bool],
    stop_event: Any,
    get_encoding_state_func: Callable[[], Tuple[bool, bool, bool]],
    log_writer: Any = None,
    logger: Any = None,
    console_redirect: Optional[Callable] = None,
    t_func: Optional[Callable[[str], str]] = None
) -> Tuple[bool, Optional[str]]:
    """Check if worker should exit the main loop.

    This consolidates all the exit condition checks that are common
    between SVT and NVENC workers.

    Args:
        final_task_taken: Whether a final task was already processed
        worker_index: Worker thread index (0-based)
        worker_type: "SVT" or "NVENC" for logging
        is_app_closing_func: Function to check if app is closing
        stop_event: STOP_EVENT to check
        get_encoding_state_func: Function to get (is_encoding, graceful_stop, _)
        log_writer: LOG_WRITER for logging
        logger: Console logger
        console_redirect: Context manager for console output
        t_func: Translation function

    Returns:
        Tuple of (should_exit, exit_reason)
        exit_reason is None if should_exit is False
    """
    # Check final task flag
    if final_task_taken:
        if log_writer:
            try:
                log_writer.write(f"[INFO] {worker_type} worker #{worker_index + 1}: Final task completed, exiting now.\n")
                log_writer.flush()
            except (OSError, IOError, AttributeError):
                pass
        return True, "final_task_completed"

    # Check app closing
    if is_app_closing_func():
        if log_writer:
            try:
                log_writer.write(f"[INFO] {worker_type} worker #{worker_index + 1}: Application closing detected, stopping.\n")
                log_writer.flush()
            except (OSError, IOError, AttributeError):
                pass
        return True, "app_closing"

    # Check STOP_EVENT and graceful_stop
    _, graceful_stop, _ = get_encoding_state_func()

    if stop_event.is_set():
        if graceful_stop:
            # Graceful stop timeout - no extra message needed
            pass
        else:
            # Immediate stop
            if logger and console_redirect and t_func:
                log_key = f'log_immediate_stop_{worker_type.lower()}'
                with console_redirect(logger):
                    print(f"\n{t_func(log_key)} #{worker_index + 1}\n")
        return True, "stop_event"

    if graceful_stop:
        return True, "graceful_stop"

    return False, None


def check_worker_count_and_get_task(
    worker_index: int,
    worker_type: str,
    get_configured_workers_func: Callable[[], int],
    get_next_task_func: Callable[[], Optional[Dict]],
    log_writer: Any = None
) -> Tuple[Optional[Dict], bool, bool]:
    """Check worker count and get next task.

    Handles the logic for checking if this worker should stop due to
    reduced worker count, and getting the next task.

    Args:
        worker_index: Worker thread index (0-based)
        worker_type: "SVT" or "NVENC" for logging
        get_configured_workers_func: Function to get configured worker count
        get_next_task_func: Function to get next task from queue
        log_writer: LOG_WRITER for logging

    Returns:
        Tuple of (task, worker_should_stop, final_task_taken)
        task is None if no more tasks and worker should stop
    """
    configured_workers = get_configured_workers_func()
    worker_should_stop = worker_index >= configured_workers
    final_task_taken = False

    if worker_should_stop:
        # Worker not needed due to reduced count
        task = get_next_task_func()
        if task is None:
            # No tasks and worker not needed - shut down
            if log_writer:
                try:
                    log_writer.write(f"[INFO] {worker_type} worker #{worker_index + 1}: Worker count reduced, no tasks left, stopping.\n")
                    log_writer.flush()
                except (OSError, IOError, AttributeError):
                    pass
            return None, True, False
        # Process one final task
        final_task_taken = True
        return task, True, True
    else:
        # Worker is still configured - get next task normally
        task = get_next_task_func()
        if task is None:
            # No pending tasks - worker auto-stops
            if log_writer:
                try:
                    log_writer.write(f"[INFO] {worker_type} worker #{worker_index + 1}: No more tasks, stopping.\n")
                    log_writer.flush()
                except (OSError, IOError, AttributeError):
                    pass
            return None, False, False
        return task, False, False


def check_worker_should_stop_after_task(
    worker_index: int,
    worker_type: str,
    initial_worker_should_stop: bool,
    get_configured_workers_func: Callable[[], int],
    log_writer: Any = None
) -> bool:
    """Check if worker should stop after processing a task.

    Re-checks worker count at loop end in case it changed during task processing.

    Args:
        worker_index: Worker thread index (0-based)
        worker_type: "SVT" or "NVENC" for logging
        initial_worker_should_stop: Whether worker was marked to stop at task start
        get_configured_workers_func: Function to get configured worker count
        log_writer: LOG_WRITER for logging

    Returns:
        bool: True if worker should exit loop
    """
    configured_workers_end = get_configured_workers_func()
    worker_should_stop_end = worker_index >= configured_workers_end

    if initial_worker_should_stop or worker_should_stop_end:
        if log_writer:
            try:
                log_writer.write(
                    f"[INFO] {worker_type} worker #{worker_index + 1}: Processed task, stopping due to "
                    f"reduced worker count (initial={initial_worker_should_stop}, final={worker_should_stop_end}).\n"
                )
                log_writer.flush()
            except (OSError, IOError, AttributeError):
                pass
        return True

    return False


# ============================================================================
# COPY FALLBACK UTILITIES
# ============================================================================

def handle_reencode_copy_fallback(
    video_path: Path,
    output_file: Path,
    item_id: str,
    task: Dict[str, Any],
    error: Exception,
    encoding_queue: 'Queue',
    column_index: Dict[str, int],
    orig_size_str: str,
    copy_video_and_subtitles: Callable[[Path, Path, bool], bool],
    format_localized_number: Callable[[float, int], str],
    update_single_video_in_db: Callable,
    t_func: Callable[[str], str],
    logger: Any = None,
    console_redirect: Optional[Callable] = None,
    estimated_end_dates: Optional[Dict] = None,
    clear_encoding_times: Optional[Callable] = None
) -> bool:
    """Handle copy fallback for re-encode tasks.

    When CRF search fails during re-encode, this handles copying
    the video unchanged instead.

    Args:
        video_path: Source video path
        output_file: Target output path
        item_id: Tree item ID
        task: Task dictionary
        error: Exception that triggered fallback
        encoding_queue: Queue for GUI updates
        column_index: COLUMN_INDEX dictionary
        orig_size_str: Original size string
        copy_video_and_subtitles: Copy function
        format_localized_number: Localized number formatter
        update_single_video_in_db: DB update function
        t_func: Translation function
        logger: Console logger
        console_redirect: Context manager for console output
        estimated_end_dates: Dict to clear estimated times
        clear_encoding_times: Function to clear encoding times

    Returns:
        bool: True if copy was successful, False otherwise
    """
    from datetime import datetime

    is_no_suitable_crf = (
        hasattr(error, '__class__') and
        error.__class__.__name__ == 'NoSuitableCRFFound'
    )

    reason = task.get('reason', '')
    reencode_type = (t('task_type_manual') if reason.startswith('manual_reencode') else t('task_type_auto')).capitalize()
    error_type = "nem talált megfelelő értéket" if is_no_suitable_crf else f"hiba történt: {str(error)}"

    if logger and console_redirect:
        with console_redirect(logger):
            if is_no_suitable_crf:
                print(f"\n[WARN] CRF keresés nem talált megfelelő értéket (VMAF >= 85.0 ÉS fájl <= 75%)")
            else:
                print(f"\n[WARN] CRF keresés során hiba történt: {str(error)}")
            print(f"   -> {reencode_type} újrakódolás: videó másolása változatlanul\n")

    # Másolás (overwrite=True)
    copy_success = copy_video_and_subtitles(video_path, output_file, True)

    if estimated_end_dates and item_id in estimated_end_dates:
        del estimated_end_dates[item_id]

    if clear_encoding_times:
        clear_encoding_times(item_id)

    if copy_success:
        try:
            orig_size_mb = video_path.stat().st_size / (1024**2)
            new_size_mb = output_file.stat().st_size / (1024**2)
            orig_size_str_new = f"{format_localized_number(orig_size_mb, 1)} MB"
            new_size_str = f"{format_localized_number(new_size_mb, 1)} MB"
        except (OSError, IOError):
            orig_size_str_new = orig_size_str
            new_size_str = "-"
            new_size_mb = 0.0

        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        encoding_queue.put_nowait((
            "update", item_id, t_func('status_completed_copy'),
            "-", "-", "-", "100%", orig_size_str_new, new_size_str, "0%", completed_date
        ))
        encoding_queue.put_nowait(("tag", item_id, "completed"))
        encoding_queue.put_nowait(("progress_bar", 0))

        # Update DB in background
        def update_db():
            try:
                update_single_video_in_db(
                    video_path, item_id, t_func('status_completed_copy'),
                    "-", "-", "-", orig_size_str_new,
                    new_size_mb, 0.0, completed_date
                )
            except (OSError, IOError, AttributeError, KeyError):
                pass

        owner = getattr(update_single_video_in_db, '__self__', None)
        if owner is not None and hasattr(owner, '_start_db_thread'):
            owner._start_db_thread(update_db, name="ReencodeCopyFallbackDB", daemon=True)
        else:
            import threading
            threading.Thread(target=update_db, daemon=True).start()

        if logger and console_redirect:
            with console_redirect(logger):
                print(f"[OK] Videó sikeresen másolva: {output_file.name}\n")

        return True
    else:
        # Copy failed - check if file already exists or actual error
        if output_file.exists():
            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            encoding_queue.put_nowait((
                "update", item_id, t_func('status_completed_exists'),
                "-", "-", "-", "100%", orig_size_str, "-", "-", completed_date
            ))
            encoding_queue.put_nowait(("tag", item_id, "completed"))
            encoding_queue.put_nowait(("progress_bar", 0))

            if logger and console_redirect:
                with console_redirect(logger):
                    print(f"[WARN] Videó már létezik a célhelyen, átugrás\n")

            return True
        else:
            # Actual copy failure (disk full, permission error, etc.)
            if logger and console_redirect:
                with console_redirect(logger):
                    print(f"[ERROR] Videó másolás sikertelen: {output_file.name}\n")

            return False


# ============================================================================
# AB-AV1 ERROR HANDLING
# ============================================================================

def handle_ab_av1_not_found(
    error: FileNotFoundError,
    item_id: str,
    orig_size_str: str,
    task: Dict[str, Any],
    column_index: Dict[str, int],
    encoding_queue: 'Queue',
    stop_event: Any,
    set_encoding_state: Callable,
    root: Any,
    is_app_closing_func: Callable[[], bool],
    messagebox: Any,
    log_writer: Any = None,
    logger: Any = None,
    console_redirect: Optional[Callable] = None,
    lang: str = 'hu'
) -> None:
    """Handle ab-av1.exe not found error.

    This displays error messages, sets stop event, and updates task status.

    Args:
        error: The FileNotFoundError that occurred
        item_id: Tree item ID
        orig_size_str: Original size string
        task: Task dictionary
        column_index: COLUMN_INDEX dictionary
        encoding_queue: Queue for GUI updates
        stop_event: STOP_EVENT to set
        set_encoding_state: Function to set encoding state
        root: Tk root for messagebox
        is_app_closing_func: Function to check if app is closing
        messagebox: tkinter messagebox module
        log_writer: LOG_WRITER for logging
        logger: Console logger
        console_redirect: Context manager for console output
        lang: Language for messages ('hu' or 'en')
    """
    import time as time_module
    import tkinter as tk

    lang_texts = TRANSLATIONS.get(lang) or TRANSLATIONS.get('en', {})
    error_msg = lang_texts.get('msg_abav1_fatal_error', 'FATAL ERROR: ab-av1.exe not found: {error}').format(error=error)
    title = lang_texts.get('fatal_error_title', 'Fatal Error')
    status_text = lang_texts.get('status_abav1_not_found', '[ERROR] Ab-av1.exe not found')

    if logger and console_redirect:
        with console_redirect(logger):
            print(f"\n{'='*80}")
            print(f"[WARN][WARN][WARN] {title} [WARN][WARN][WARN]")
            print(f"{'='*80}")
            print(error_msg)
            print(f"{'='*80}\n")

    if log_writer:
        try:
            log_writer.write(f"\n{'='*80}\n")
            log_writer.write(f"[WARN][WARN][WARN] {title} [WARN][WARN][WARN]\n")
            log_writer.write(f"{'='*80}\n")
            log_writer.write(f"{error_msg}\n")
            log_writer.write(f"{'='*80}\n\n")
            log_writer.flush()
        except (OSError, IOError, AttributeError):
            pass

    # Immediate stop
    stop_event.set()
    set_encoding_state(graceful_stop_requested=True)

    # Show MessageBox in GUI thread
    try:
        if not is_app_closing_func() and root.winfo_exists():
            root.after(0, lambda: messagebox.showerror(title, error_msg))
            time_module.sleep(0.5)
    except tk.TclError:
        pass

    # Update status
    cached_values = task.get('cached_values', [])
    completed_date_idx = column_index.get('completed_date', -1)
    completed_date = ""
    if completed_date_idx >= 0 and len(cached_values) > completed_date_idx:
        completed_date = cached_values[completed_date_idx]

    encoding_queue.put_nowait((
        "update", item_id, status_text,
        "-", "-", "-", "-", orig_size_str, "-", "-", completed_date
    ))
    encoding_queue.put_nowait(("tag", item_id, "failed"))
