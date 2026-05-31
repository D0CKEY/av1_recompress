from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import os
import shutil
import subprocess
import re
import threading
import time
import multiprocessing
import platform
from pathlib import Path

from .i18n import format_localized_number
from .core_preamble_and_imports import (
    STOP_EVENT,
    FFMPEG_PATH,
    FFPROBE_PATH,
    ABAV1_PATH,
    ACTIVE_PROCESSES,
    ACTIVE_PROCESSES_LOCK,
    CURRENT_LANGUAGE,
    DEBUG_MODE,
    terminate_process_tree,
    format_cmd_for_windows
)
from .core_paths_tools_logging import (
    EncodingStopped,
    get_startup_info
)
# NOTE: during `core` assembly this fragment is concatenated BEFORE
# core_audio_video_ops, so the submodule alias does not yet exist in sys.modules
# and Python loads core_audio_video_ops.py once from disk here. This is a harmless
# one-time startup double-load: both functions are used only inside functions
# (resolved at call time), never at module load.
from .core_audio_video_ops import get_video_info, get_video_resolution

LIBVMAF_SUPPORTS_PSNR = True

def calculate_psnr_only(reference_path, encoded_path, stop_event=None, logger=None):
    """Calculate PSNR metric using FFmpeg.
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        stop_event: Threading event to stop calculation.
        logger: Logger instance.
        
    Returns:
        float: PSNR value or None on error.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    
    psnr_cmd = [
        FFMPEG_PATH,
        '-i', reference_str,
        '-i', encoded_str,
        '-lavfi', 'psnr',
        '-f', 'null',
        '-'
    ]
    
    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"[SUB] PSNR CALCULATION (standalone)\n")
        logger.write(f"{'='*80}\n")
        logger.write(format_cmd_for_windows(psnr_cmd) + '\n')
        logger.write(f"{'='*80}\n\n")
        logger.flush()
    
    try:
        process = subprocess.Popen(
            psnr_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        psnr_value = None
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise EncodingStopped()
                
                if logger:
                    logger.write(line)
                    logger.flush()
                
                avg_match = re.search(r'(?:avg|average)[:=]\s*([\d.]+)', line, re.IGNORECASE)
                if avg_match:
                    try:
                        psnr_value = float(avg_match.group(1))
                    except ValueError:
                        pass
        except EncodingStopped:
            raise
        finally:
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
        
        try:
            process.wait()
        except (OSError, subprocess.SubprocessError) as e:
            if logger:
                logger.write(f"PSNR process wait hiba: {e}\n")
                logger.flush()
        
        if process.returncode != 0:
            error_msg = f"[ERROR] PSNR számítás hiba (return code: {process.returncode})\n"
            if logger:
                logger.write(error_msg)
                logger.flush()
            else:
                print(error_msg, end="")
            return psnr_value
        
        return psnr_value
    except EncodingStopped:
        raise
    except Exception as e:
        error_msg = f"[ERROR] PSNR számítás hiba: {e}\n"
        if logger:
            logger.write(error_msg)
            logger.flush()
        else:
            print(error_msg, end="")
        return None

def _is_abav1_available():
    """Check if AB-AV1 is executable."""
    if not ABAV1_PATH:
        return False
    try:
        ab_path = Path(ABAV1_PATH)
        if ab_path.exists():
            return True
    except (OSError, ValueError):
        pass
    return shutil.which(ABAV1_PATH) is not None


def _parse_eta_to_seconds(eta_text):
    """Parse FFmpeg ETA text (HH:MM:SS) to seconds.
    
    Args:
        eta_text: ETA string.
        
    Returns:
        int: Seconds or None.
    """

    if not eta_text:
        return None
    text = str(eta_text).strip().lower()
    if not text or text in {'n/a', 'na', 'nan', '-'}:
        return None

    # HH:MM:SS or MM:SS format
    if re.match(r'^\d{1,2}:\d{2}:\d{2}$', text):
        hours, minutes, seconds = (int(part) for part in text.split(':'))
        return hours * 3600 + minutes * 60 + seconds
    if re.match(r'^\d{1,2}:\d{2}$', text):
        minutes, seconds = (int(part) for part in text.split(':'))
        return minutes * 60 + seconds

    total_seconds = 0.0
    matched = False
    for value, unit in re.findall(r'(\d+(?:\.\d+)?)\s*(hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)', text):
        matched = True
        numeric_value = float(value)
        if unit.startswith('h'):
            total_seconds += numeric_value * 3600
        elif unit.startswith('m'):
            total_seconds += numeric_value * 60
        else:
            total_seconds += numeric_value
    if matched:
        return total_seconds

    # Simple number (assume seconds)
    try:
        return float(text)
    except ValueError:
        return None


def _format_eta_short(seconds_value):
    """Short, readable ETA format."""
    if seconds_value is None:
        return "n/a"
    try:
        seconds_value = max(0, int(seconds_value))
    except (OverflowError, ValueError):
        return "n/a"
    if seconds_value >= 3600:
        hours = seconds_value // 3600
        minutes = (seconds_value % 3600) // 60
        return f"{hours}h {minutes}m"
    if seconds_value >= 60:
        minutes = seconds_value // 60
        seconds = seconds_value % 60
        return f"{minutes}m {seconds}s"
    return f"{seconds_value}s"


def format_seconds_hms(total_seconds):
    """Format seconds to HH:MM:SS (None if invalid)."""
    if total_seconds is None:
        return None
    try:
        total_seconds = int(max(0, round(float(total_seconds))))
    except (ValueError, TypeError):
        return None
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_metric_value(value, decimals=2):
    """Common formatting for VMAF/PSNR values (round half up) - localized."""
    if value is None:
        return "-"
    try:
        quant = Decimal('1').scaleb(-decimals)
        decimal_value = Decimal(str(value))
        rounded = decimal_value.quantize(quant, rounding=ROUND_HALF_UP)
        formatted = format(rounded, f'.{decimals}f')
        # Localization: Hungarian = comma, English = dot
        if CURRENT_LANGUAGE == 'hu':
            formatted = formatted.replace('.', ',')
        return formatted
    except (InvalidOperation, ValueError, TypeError):
        try:
            formatted = f"{float(value):.{decimals}f}"
            if CURRENT_LANGUAGE == 'hu':
                formatted = formatted.replace('.', ',')
            return formatted
        except (ValueError, TypeError):
            return str(value)


def format_number_en(value, decimals=2, show_sign=False):
    """Format number in English format (always use dot as decimal separator) for metadata/technical purposes.
    
    Args:
        value: Number to format.
        decimals: Number of decimal places.
        show_sign: If True, show + sign for positive numbers.
        
    Returns:
        str: Formatted number string (always uses dot as decimal separator).
    """
    if value is None:
        return "-"
    try:
        quant = Decimal('1').scaleb(-decimals)
        decimal_value = Decimal(str(value))
        rounded = decimal_value.quantize(quant, rounding=ROUND_HALF_UP)
        if show_sign:
            formatted = f"{rounded:+.{decimals}f}"
        else:
            formatted = f"{rounded:.{decimals}f}"
        return formatted
    except (InvalidOperation, ValueError, TypeError):
        try:
            num_value = float(value)
            if show_sign:
                formatted = f"{num_value:+.{decimals}f}"
            else:
                formatted = f"{num_value:.{decimals}f}"
            return formatted
        except (ValueError, TypeError):
            return str(value) if value is not None else "-"


def _run_abav1_metric(metric, reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None, duration_seconds=None):
    """Run an ab-av1 metric command (vmaf or xpsnr) and return the result.
    
    Args:
        metric: 'vmaf' or 'xpsnr'.
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        duration_seconds: Video duration in seconds.
        
    Returns:
        float: Metric score or None on error.
    """
    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    metric_name = metric.lower()
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    cmd = [
        ABAV1_PATH,
        metric_name,
        '--reference', reference_str,
        '--distorted', encoded_str,
    ]

    try:
        duration_seconds = float(duration_seconds)
        if duration_seconds <= 0:
            duration_seconds = None
    except (TypeError, ValueError):
        duration_seconds = None

    if progress_callback:
        progress_callback(f"{metric_name.upper()} (ab-av1)")

    # Helper függvény a logger-re vagy konzolra íráshoz
    # KRITIKUS: print() használata, hogy a console_redirect blokk működjön!
    def _log_output(msg, end='\n'):
        print(msg, end=end, flush=True)

    # VMAF/PSNR command display
    _log_output(f"\n{'='*80}")
    _log_output(f"🎬 ab-av1 {metric_name.upper()} PARANCS:")
    _log_output(f"{'='*80}")
    _log_output(format_cmd_for_windows(cmd))
    _log_output(f"{'='*80}\n")
    
    # KRITIKUS FIX: NE használjuk a wait_for_file_ready()-t, mert a file_path.stat()
    # is BLOCKING lehet Windows-on lock esetén, és elakasztja a thread-et!
    # Helyette: egyszerű fix delay (1 másodperc), hogy az OS-nek legyen ideje
    # lezárni a file handle-eket az encoding után.
    # Az ab-av1 maga is robusztus és kezeli ha a fájl még nem teljesen kész.
    try:
        # _log_output("⏳ Waiting 1 second for file handles to close...")
        
        time.sleep(1)
        
        # _log_output("[OK] Ready to start ab-av1 process")
    except Exception as e:
        import traceback
        error_detail = f"[ERROR] EXCEPTION at time.sleep: {type(e).__name__}: {e}\n{traceback.format_exc()}\n"
        _log_output(error_detail)
        raise

    # _log_output("Creating threading objects...")
    interpolator_stop = threading.Event()
    interpolator_lock = threading.Lock()
    interpolation_state = {
        'percent': None,
        'eta_seconds': None,
        'update_time': None,
        'finish_time': None,
        'reset_requested': False,
    }
    # _log_output("Threading objects created OK")

    def _send_progress(percent=None, eta_seconds=None, eta_text=None, interpolated=False, done=False):
        if not progress_callback:
            return
        elapsed_seconds = None
        if percent is not None and duration_seconds:
            try:
                elapsed_seconds = max(0.0, min(duration_seconds, duration_seconds * (percent / 100.0)))
            except (TypeError, ValueError):
                elapsed_seconds = None
        payload = {
            'type': 'abav1_progress',
            'metric': metric_name.upper(),
            'percent': percent,
            'eta_seconds': eta_seconds,
            'eta_text': eta_text,
            'duration_seconds': duration_seconds,
            'interpolated': interpolated,
            'done': done,
            'timestamp': time.time(),
            'elapsed_seconds': elapsed_seconds,
        }
        display_eta = eta_text
        if display_eta is None and eta_seconds is not None:
            display_eta = _format_eta_short(eta_seconds)
        if percent is not None:
            percent_display = f"{format_localized_number(percent, decimals=1)}%"
        else:
            percent_display = "*"
        if display_eta is not None:
            payload['text'] = f"{metric_name.upper()} {percent_display} (ETA {display_eta})"
        else:
            payload['text'] = f"{metric_name.upper()} {percent_display}"
        try:
            progress_callback(payload)
        except Exception:
            # GUI callback error should not stop the main process
            pass

    def _start_interpolator():
        if not progress_callback:
            return None

        def _interpolate_progress():
            last_sent_percent = None
            while not interpolator_stop.wait(1):
                if stop_event.is_set():
                    break
                with interpolator_lock:
                    percent = interpolation_state.get('percent')
                    finish_time = interpolation_state.get('finish_time')
                    update_time = interpolation_state.get('update_time')
                    reset_requested = interpolation_state.get('reset_requested', False)
                    if reset_requested:
                        interpolation_state['reset_requested'] = False
                if reset_requested:
                    last_sent_percent = None
                if percent is None or finish_time is None or update_time is None:
                    continue
                now_monotonic = time.monotonic()
                total_window = finish_time - update_time
                if total_window <= 0:
                    continue
                elapsed = now_monotonic - update_time
                if elapsed <= 0:
                    continue
                elapsed_clamped = min(elapsed, total_window)
                remaining_fraction = 1.0 - (elapsed_clamped / total_window)
                projected_percent = min(percent + (1 - remaining_fraction) * (100 - percent), 99.9)
                if last_sent_percent is not None and projected_percent - last_sent_percent < 0.1:
                    continue
                last_sent_percent = projected_percent
                eta_seconds = max(finish_time - now_monotonic, 0)
                _send_progress(percent=projected_percent, eta_seconds=eta_seconds, interpolated=True)

        thread = threading.Thread(target=_interpolate_progress, name="abav1-progress", daemon=True)
        thread.start()
        return thread

    # _log_output("Function definitions complete, setting creationflags...")
    creationflags = 0
    preexec_fn = None
    if platform.system() == 'Windows':
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        preexec_fn = os.setsid

    # _log_output("About to create subprocess.Popen...")
    # _log_output(f"Command: {' '.join(cmd)}")

    # Use text=True like in other working code examples
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            startupinfo=get_startup_info(),
            creationflags=creationflags,
            preexec_fn=preexec_fn
        )
    except Exception as e:
        import traceback
        error_detail = f"[ERROR] EXCEPTION creating subprocess: {type(e).__name__}: {e}\nCommand: {format_cmd_for_windows(cmd)}\n{traceback.format_exc()}\n"
        _log_output(error_detail)
        raise

    # _log_output(f"Process created, PID: {process.pid}")

    try:
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
    except Exception as e:
        error_detail = f"[WARN] ACTIVE_PROCESSES append error: {type(e).__name__}: {e}\n"
        _log_output(error_detail)

    # Interpolator started silently

    interpolator_thread = _start_interpolator()
    full_output = []
    last_percent_reported = None
    
    # _log_output("Reading process stdout...")
    
    try:
        # Read output line by line with error handling
        # Use simple iteration like in other working code examples
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    terminate_process_tree(process)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        terminate_process_tree(process)
                    raise EncodingStopped()
                
                full_output.append(line)
                # ab-av1 kimenet a logger-re (SVT-AV1 worker konzol)
                # KRITIKUS: print() használata, hogy a console_redirect blokk működjön!
                print(line, end="", flush=True)
                percent_match = re.search(r'(\d+)%.*eta\s+(.+)', line, re.IGNORECASE)
                if percent_match:
                    try:
                        percent = int(percent_match.group(1))
                    except ValueError:
                        percent = None
                    eta_text = percent_match.group(2).strip()
                    eta_seconds = _parse_eta_to_seconds(eta_text)
                    now_monotonic = time.monotonic()
                    if percent is not None:
                        last_percent_reported = percent
                    with interpolator_lock:
                        prev_percent = interpolation_state.get('percent')
                        prev_time = interpolation_state.get('update_time')
                        estimated_eta = eta_seconds
                        if estimated_eta is None and percent is not None and prev_percent is not None and prev_time is not None and percent > prev_percent:
                            elapsed = now_monotonic - prev_time
                            percent_delta = percent - prev_percent
                            if elapsed > 0 and percent_delta > 0:
                                remaining = max(100 - percent, 0)
                                estimated_eta = (elapsed / percent_delta) * remaining
                        interpolation_state['percent'] = percent
                        interpolation_state['eta_seconds'] = estimated_eta
                        interpolation_state['update_time'] = now_monotonic
                        if estimated_eta is not None and percent is not None:
                            interpolation_state['finish_time'] = now_monotonic + estimated_eta
                        else:
                            interpolation_state['finish_time'] = None
                        interpolation_state['reset_requested'] = True
                    _send_progress(percent=percent, eta_seconds=eta_seconds, eta_text=eta_text)
        except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
            _log_output(f"[WARN] Process output reading error: {e}")
            # Continue to process.wait() to get the final result
        
        # Wait for process to complete
        try:
            process.wait(timeout=3600)  # 1 hour timeout
        except subprocess.TimeoutExpired:
            _log_output(f"[ERROR] ab-av1 {metric_name.upper()} calculation timeout (>1 hour)")
            terminate_process_tree(process)
            return None
        
        # Stop interpolator
        if interpolator_thread:
            interpolator_stop.set()
            interpolator_thread.join(timeout=2)
        
        # _log_output(f"\n{'='*80}")
        # _log_output(f"ab-av1 {metric_name.upper()} process finished")
        # _log_output(f"Return code: {process.returncode}")
        # _log_output(f"Output lines: {len(full_output)}")
        if len(full_output) == 0:
            _log_output(f"[WARN] WARNING: NO OUTPUT RECEIVED!")
        # _log_output(f"{'='*80}\n")
        
        # Extract metric value from output
        if process.returncode == 0:
            output_text = ''.join(full_output)
            metric_value = _extract_abav1_metric_value(metric_name, output_text)
            if metric_value is not None:
                _send_progress(done=True)
                return metric_value
            else:
                _log_output(f"[WARN] Could not extract {metric_name.upper()} value")
                return None
        else:
            _log_output(f"[ERROR] ab-av1 {metric_name.upper()} failed (code: {process.returncode})")
            return None
    except EncodingStopped:
        raise
    except Exception as e:
        _log_output(f"[ERROR] ab-av1 {metric_name.upper()} error: {e}")
        return None
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)
        # Stop interpolator in case of exception
        if interpolator_thread:
            interpolator_stop.set()
            interpolator_thread.join(timeout=2)

def _extract_abav1_metric_value(metric_name, output_text):
    """Try to extract metric value from ab-av1 output."""
    lines = output_text.splitlines()
    numeric_line_pattern = re.compile(r'^\s*([0-9]+(?:\.[0-9]+)?)\s*$')
    # First look for a line containing only the number (ab-av1 often ends like this)
    for line in reversed(lines):
        match = numeric_line_pattern.match(line)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                continue

    keywords = [metric_name.lower()]
    if metric_name.lower() == 'xpsnr':
        keywords.append('psnr')
    for line in reversed(lines):
        lower = line.lower()
        if any(key in lower for key in keywords):
            matches = re.findall(r'([0-9]+(?:\.[0-9]+)?)', line)
            for match in reversed(matches):
                try:
                    return float(match)
                except ValueError:
                    continue
    return None


def calculate_full_vmaf(reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None, check_vmaf=True, check_psnr=True, metric_done_callback=None):
    """Calculate VMAF and/or PSNR metrics using ab-av1 (preferred) or FFmpeg fallback.
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        check_vmaf: Whether to calculate VMAF.
        check_psnr: Whether to calculate PSNR.
        metric_done_callback: Callback when a metric is done.
        
    Returns:
        tuple: (vmaf_value, psnr_value)
    """
    """Full VMAF + XPSNR calculation using ab-av1, with FFmpeg fallback."""
    if stop_event is None:
        stop_event = STOP_EVENT
    reference_path = Path(reference_path)
    encoded_path = Path(encoded_path)
    if not check_vmaf and not check_psnr:
        return None, None

    # CRITICAL: Check if resolutions match before running VMAF/PSNR
    # Different resolutions will cause ab-av1/ffmpeg to fail with "input height must match" error
    try:
        ref_width, ref_height = get_video_resolution(reference_path)
        enc_width, enc_height = get_video_resolution(encoded_path)

        if ref_width and ref_height and enc_width and enc_height:
            if ref_width != enc_width or ref_height != enc_height:
                # Resolutions don't match - VMAF/PSNR will fail
                warning_msg = f"\n{'='*80}\n"
                warning_msg += f"[WARN] VMAF/PSNR KIHAGYVA: Eltérő felbontások miatt\n"
                warning_msg += f"{'='*80}\n"
                warning_msg += f"  Eredeti:  {ref_width}x{ref_height}\n"
                warning_msg += f"  Átkódolt: {enc_width}x{enc_height}\n"
                warning_msg += f"\n  A VMAF/PSNR számításhoz azonos felbontás szükséges.\n"
                warning_msg += f"  Az átkódolás során átméretezés történt, ezért a minőségi\n"
                warning_msg += f"  metrikák nem értelmezhetők.\n"
                warning_msg += f"{'='*80}\n\n"

                if logger:
                    logger.write(warning_msg)
                    logger.flush()
                else:
                    print(warning_msg, end="", flush=True)

                return None, None
    except Exception as e:
        # If resolution check fails, continue with VMAF/PSNR (will fail later if needed)
        warning_msg = f"[WARN] Felbontás ellenőrzés sikertelen: {e}\n"
        if logger:
            logger.write(warning_msg)
            logger.flush()
        else:
            print(warning_msg, end="", flush=True)

    # KRITIKUS: NE hívjuk meg a get_video_info-t itt, mert elakadhat!
    # A duration_seconds-t a _run_abav1_metric vagy progress callback frissíti
    duration_seconds = None
    if _is_abav1_available():
        try:
            vmaf_value = None
            psnr_value = None
            if check_vmaf:
                vmaf_value = _run_abav1_metric('vmaf', reference_path, encoded_path, progress_callback, stop_event, logger, duration_seconds=duration_seconds)
                if metric_done_callback and vmaf_value is not None:
                    metric_done_callback('VMAF', vmaf_value)
            if check_psnr:
                psnr_value = _run_abav1_metric('xpsnr', reference_path, encoded_path, progress_callback, stop_event, logger, duration_seconds=duration_seconds)
                if metric_done_callback and psnr_value is not None:
                    metric_done_callback('PSNR', psnr_value)
            return vmaf_value, psnr_value
        except EncodingStopped:
            raise
        except Exception as e:
            fallback_msg = f"[WARN] ab-av1 VMAF/XPSNR calculation error: {e} – FFmpeg fallback\n"
            if logger:
                logger.write(fallback_msg)
                logger.flush()
            else:
                print(fallback_msg, end="")

    vmaf_value, psnr_value = _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback, stop_event, logger)
    if metric_done_callback:
        if check_vmaf and vmaf_value is not None:
            metric_done_callback('VMAF', vmaf_value)
        if check_psnr and psnr_value is not None:
            metric_done_callback('PSNR', psnr_value)
    if not check_vmaf:
        vmaf_value = None
    if not check_psnr:
        psnr_value = None
    return vmaf_value, psnr_value


def _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None):
    """Calculate VMAF using FFmpeg directly (fallback method).
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        
    Returns:
        tuple: (vmaf_value, psnr_value) - PSNR is always None in this fallback.
    """
    """
    Run full VMAF and PSNR test using ffmpeg libvmaf.
    Returns (VMAF value, PSNR value) tuple or (None, None) on error.
    FFmpeg 8.0+ compatible - VMAF and PSNR in one command.
    """
    global LIBVMAF_SUPPORTS_PSNR
    if stop_event is None:
        stop_event = STOP_EVENT
    
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    
    # Get full video duration - try-except-tel védjük, mert elakadhat
    duration_seconds = None
    try:
        duration_seconds, _ = get_video_info(reference_path)
        if duration_seconds is None:
            duration_seconds = 0
    except Exception as e:
        # KRITIKUS HIBA: get_video_info elakadhat!
        error_msg = f"[ERROR] KRITIKUS HIBA: get_video_info elakadt vagy hibázott: {e}\n"
        error_msg += f"   reference_path: {reference_path}\n"
        if logger:
            logger.write(error_msg)
            import traceback
            logger.write(traceback.format_exc() + "\n")
            logger.flush()
        else:
            print(error_msg, flush=True)
            import traceback
            traceback.print_exc()
        # Ha elakad vagy hiba van, 0-t használunk (progress callback frissíti)
        duration_seconds = 0
    
    duration_hours = int(duration_seconds // 3600)
    duration_mins = int((duration_seconds % 3600) // 60)
    duration_secs = int(duration_seconds % 60)
    
    # FFmpeg command for VMAF calculation
    # Use libvmaf filter for the whole video
    # Automatic CPU core detection and usage
    cpu_count = multiprocessing.cpu_count()
    
    # FFmpeg 8.0+: VMAF and PSNR in one command (feature=name=psnr)
    base_libvmaf_filter = f'libvmaf=n_threads={cpu_count}'
    if LIBVMAF_SUPPORTS_PSNR:
        base_libvmaf_filter += ':feature=name=psnr'
    libvmaf_filter = base_libvmaf_filter
    
    ffmpeg_cmd = [
        FFMPEG_PATH,
        '-i', reference_str,
        '-i', encoded_str,
        '-lavfi', libvmaf_filter,
        '-f', 'null',
        '-'
    ]
    
    # Write FFmpeg command to console and logger
    print(f"\n{'='*80}")
    print(f"🎬 FFMPEG VMAF/PSNR PARANCS:")
    print(f"{'='*80}")
    print(format_cmd_for_windows(ffmpeg_cmd))
    print(f"Video duration: {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
    print(f"{'='*80}\n")
    
    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"🎬 VMAF/PSNR CALCULATION COMMAND:\n")
        logger.write(f"{'='*80}\n")
        logger.write(format_cmd_for_windows(ffmpeg_cmd) + '\n')
        logger.write(f"Video duration: {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}\n")
        logger.write(f"{'='*80}\n\n")
        logger.flush()
    
    try:
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        # Extract VMAF and PSNR values from stdout
        vmaf_value = None
        psnr_value = None
        full_output = []
        
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise EncodingStopped()
                
                full_output.append(line)
                
                # Write FFmpeg output to logger
                if logger:
                    logger.write(line)
                    logger.flush()
                
                # Search for VMAF value in output
                # libvmaf format: "VMAF score: XX.XXXX" or "VMAF score = XX.XXXX"
                vmaf_match = re.search(r'VMAF\s+score[:\s=]+\s*([\d.]+)', line, re.IGNORECASE)
                if vmaf_match:
                    try:
                        vmaf_value = float(vmaf_match.group(1))
                    except ValueError:
                        pass
                
                # Search for PSNR value in output
                # libvmaf format: "PSNR score: XX.XXXX" or "PSNR score = XX.XXXX" or "PSNR: XX.XXXX"
                psnr_match = re.search(r'PSNR\s+(?:score[:\s=]+|:)\s*([\d.]+)', line, re.IGNORECASE)
                if psnr_match:
                    try:
                        psnr_value = float(psnr_match.group(1))
                    except ValueError:
                        pass
                
                # Extract progress information - in duration format
                if progress_callback and ('frame=' in line or 'time=' in line):
                    if 'time=' in line:
                        time_match = re.search(r'time=(\d+):(\d+):(\d+)(?:\.\d+)?', line)
                        if time_match:
                            hours, mins, secs = map(int, time_match.groups())
                            elapsed_total = hours * 3600 + mins * 60 + secs
                            # Limit to video duration
                            elapsed_total = min(elapsed_total, duration_seconds) if duration_seconds > 0 else elapsed_total
                            
                            # Duration format: "HH:MM:SS / HH:MM:SS"
                            progress_hours = int(elapsed_total // 3600)
                            progress_mins = int((elapsed_total % 3600) // 60)
                            progress_secs = int(elapsed_total % 60)
                            
                            progress_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
        except EncodingStopped:
            raise
        except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
            error_msg = f"[ERROR] Process output reading error: {e}\n"
            if logger:
                logger.write(error_msg)
                logger.flush()
            else:
                print(error_msg, end="")
        finally:
            # Remove process from list - always execute
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
        
        # process.wait() moved to try block, so no duplicate cleanup
        process.wait()
        if process.returncode != 0:
            error_msg = f"[ERROR] VMAF calculation error (return code: {process.returncode})\n"
            if logger:
                logger.write(error_msg)
                logger.flush()
            else:
                print(error_msg, end="")
            if LIBVMAF_SUPPORTS_PSNR and ("feature" in ''.join(full_output).lower()):
                LIBVMAF_SUPPORTS_PSNR = False
                warning_msg = "[WARN] Libvmaf does not support feature=name=psnr option – switching to standalone PSNR.\n"
                if logger:
                    logger.write(warning_msg)
                    logger.flush()
                else:
                    print(warning_msg, end="")
                return _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback, stop_event, logger)
            return (None, None)
        
        if vmaf_value is None or psnr_value is None:
            # Try searching in all lines
            full_output_text = ''.join(full_output)
            if vmaf_value is None:
                vmaf_match = re.search(r'VMAF\s+score[:\s=]+\s*([\d.]+)', full_output_text, re.IGNORECASE)
                if vmaf_match:
                    try:
                        vmaf_value = float(vmaf_match.group(1))
                    except ValueError:
                        pass
            if psnr_value is None:
                psnr_match = re.search(r'PSNR\s+(?:score[:\s=]+|:)\s*([\d.]+)', full_output_text, re.IGNORECASE)
                if psnr_match:
                    try:
                        psnr_value = float(psnr_match.group(1))
                    except ValueError:
                        pass
        
        return (vmaf_value, psnr_value)
        
    except EncodingStopped:
        raise
    except Exception as e:
        error_msg = f"[ERROR] VMAF/PSNR calculation error: {e}\n"
        if logger:
            logger.write(error_msg)
            logger.flush()
        else:
            print(error_msg, end="")
        return (None, None)

def update_video_metadata_vmaf(video_path, vmaf_value, psnr_value=None, logger=None):
    """Update video metadata with calculated VMAF and PSNR values.
    
    Writes the values to the file metadata using FFmpeg.
    
    Args:
        video_path: Path to the video file.
        vmaf_value: Calculated VMAF score.
        psnr_value: Calculated PSNR score (optional).
        logger: Logger instance.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    video_str = None
    temp_path = None
    try:
        video_str = os.fspath(video_path.absolute())
        
        # Use language-independent (English) format for metadata
        vmaf_str_formatted = format_number_en(vmaf_value, decimals=2) if vmaf_value is not None else None
        psnr_str_formatted = format_number_en(psnr_value, decimals=2) if psnr_value is not None else None

        if logger:
            logger.write(f"\n{'='*80}\n")
            logger.write(f"📝 VMAF/PSNR METADATA UPDATE: {video_path.name}\n")
            logger.write(f"{'='*80}\n")
            logger.write(f"New VMAF value: {vmaf_str_formatted}\n")
            if psnr_value is not None:
                logger.write(f"New PSNR value: {psnr_str_formatted}\n")
            logger.flush()
        
        # Find current Settings metadata
        # 1. Try global metadata (format tags)
        probe_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format_tags=Settings',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_str
        ]
        
        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600, startupinfo=get_startup_info())
        current_settings = result.stdout.strip() if result.stdout else ""
        
        # 2. If not in global, try video stream 0 metadata (stream tags)
        if not current_settings:
            probe_cmd_stream = [
                FFPROBE_PATH, '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream_tags=Settings',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                video_str
            ]
            try:
                result_stream = subprocess.run(probe_cmd_stream, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600, startupinfo=get_startup_info())
                current_settings = result_stream.stdout.strip() if result_stream.stdout else ""
            except Exception:
                pass
        
        if logger:
            logger.write(f"Current Settings (probed): {current_settings}\n")
            logger.flush()
        
        # Create new Settings metadata with VMAF value
        if current_settings:
            # If Settings already exists, update VMAF part
            # Format: "FFMPEG NVENC - CQ:XX - Preset 7 - Planned VMAF: XX.X"
            # Or: "FFMPEG SVT-AV1 - CRF:XX - Preset 2 - Planned VMAF: XX.X"
            # Or: "... - Actual VMAF: XX.X" (if VMAF calculation already happened)
            new_settings = current_settings
            
            # Only update VMAF if we have a valid value
            if vmaf_str_formatted is not None:
                # Strategy: Insert "Actual VMAF" and "PSNR" right after "Planned VMAF" position
                # Extract everything after "Planned VMAF: X" to preserve it (tune params, Filters, etc.)
                
                if 'Planned VMAF' in current_settings:
                    # Find the position after "Planned VMAF: X"
                    match = re.search(r'(Planned VMAF:\s*[\d.,]+|Planned VMAF:\s*--|Planned VMAF:\s*-)', current_settings)
                    if match:
                        # Split at the end of "Planned VMAF: X"
                        before_vmaf = current_settings[:match.start()]
                        after_vmaf = current_settings[match.end():]
                        
                        # Clean up "after_vmaf" part - remove leading " - "
                        after_vmaf = re.sub(r'^\s*-\s*', '', after_vmaf).strip()
                        
                        # Build new settings: before + Actual VMAF + PSNR (if exists) + after
                        new_settings = before_vmaf.rstrip()
                        if new_settings.endswith('-'):
                            new_settings += ' '
                        else:
                             new_settings += ' - '
                        new_settings += f'Actual VMAF: {vmaf_str_formatted}'
                        
                        # Add PSNR immediately after VMAF if we have it
                        if psnr_value is not None:
                            new_settings += f' - PSNR: {psnr_str_formatted}'
                        
                        # Append remaining parts (tune params, Filters, etc.)
                        if after_vmaf:
                            new_settings += f' - {after_vmaf}'
                    else:
                        # Fallback: just append
                        new_settings = f"{current_settings} - Actual VMAF: {vmaf_str_formatted}"
                        if psnr_value is not None:
                            new_settings += f" - PSNR: {psnr_str_formatted}"
                
                elif 'Actual VMAF' in current_settings:
                    # Update existing "Actual VMAF" value in place
                    match = re.search(r'(Actual VMAF:\s*[\d.,]+)', current_settings)
                    if match:
                        before_vmaf = current_settings[:match.start()]
                        after_vmaf = current_settings[match.end():]
                        after_vmaf = re.sub(r'^\s*-\s*', '', after_vmaf).strip()
                        
                        new_settings = before_vmaf.rstrip()
                        if new_settings.endswith('-'):
                            new_settings += ' '
                        else:
                             new_settings += ' - '
                        new_settings += f'Actual VMAF: {vmaf_str_formatted}'
                        
                        # Update or add PSNR right after VMAF
                        if psnr_value is not None:
                            # Check if PSNR is in the "after" part
                            psnr_match = re.search(r'PSNR:\s*[\d.,]+', after_vmaf)
                            if psnr_match:
                                # Update PSNR in place
                                after_vmaf = re.sub(r'PSNR:\s*[\d.,]+', f'PSNR: {psnr_str_formatted}', after_vmaf)
                            else:
                                # Add PSNR after VMAF
                                new_settings += f' - PSNR: {psnr_str_formatted}'
                        
                        if after_vmaf:
                            new_settings += f' - {after_vmaf}'
                    else:
                        # Fallback
                        new_settings = current_settings
                        if psnr_value is not None:
                            if 'PSNR:' in new_settings:
                                new_settings = re.sub(r'PSNR:\s*[\d.,]+', f'PSNR: {psnr_str_formatted}', new_settings)
                            else:
                                new_settings += f" - PSNR: {psnr_str_formatted}"
                else:
                    # No VMAF info at all, add it
                    new_settings = f"{current_settings} - Actual VMAF: {vmaf_str_formatted}"
                    if psnr_value is not None:
                        new_settings += f" - PSNR: {psnr_str_formatted}"
            
            elif psnr_value is not None:
                # Only PSNR update (no VMAF value provided)
                # Try to insert PSNR right after existing VMAF if present
                new_settings = current_settings
                if 'Planned VMAF' in current_settings or 'Actual VMAF' in current_settings:
                    # Find VMAF position and insert PSNR after it
                    vmaf_match = re.search(r'((?:Planned|Actual) VMAF:\s*[\d.,]+)', current_settings)
                    if vmaf_match:
                        before_vmaf = current_settings[:vmaf_match.end()]
                        after_vmaf = current_settings[vmaf_match.end():]
                        after_vmaf = re.sub(r'^\s*-\s*', '', after_vmaf).strip()
                        
                        # Remove existing PSNR from after_vmaf if present
                        after_vmaf = re.sub(r'PSNR:\s*[\d.,]+(?:\s*-\s*)?', '', after_vmaf).strip()
                        after_vmaf = re.sub(r'^\s*-\s*', '', after_vmaf).strip()
                        
                        new_settings = f"{before_vmaf} - PSNR: {psnr_str_formatted}"
                        if after_vmaf:
                            new_settings += f' - {after_vmaf}'
                    elif 'PSNR:' in new_settings:
                        # Just update existing PSNR
                        new_settings = re.sub(r'PSNR:\s*[\d.,]+', f'PSNR: {psnr_str_formatted}', new_settings)
                    else:
                        new_settings += f" - PSNR: {psnr_str_formatted}"
                else:
                    # No VMAF at all
                    if 'PSNR:' in new_settings:
                        new_settings = re.sub(r'PSNR:\s*[\d.,]+', f'PSNR: {psnr_str_formatted}', new_settings)
                    else:
                        new_settings = f"{current_settings} - PSNR: {psnr_str_formatted}"
        else:
            # Ha nincs Settings metaadat, létrehozzuk
            if vmaf_str_formatted is not None and psnr_value is not None:
                new_settings = f"Actual VMAF: {vmaf_str_formatted} - PSNR: {psnr_str_formatted}"
            elif vmaf_str_formatted is not None:
                new_settings = f"Actual VMAF: {vmaf_str_formatted}"
            elif psnr_value is not None:
                # Only PSNR, no VMAF
                new_settings = f"PSNR: {psnr_str_formatted}"
            else:
                # No metadata to add
                return False
        
        # FFmpeg parancs metaadat frissítéshez (copy minden streamet, csak metaadatot módosítjuk)
        # Temp fájl az eredeti kiterjesztéssel, hogy az FFmpeg felismerje a formátumot
        # Kezeljük a több kiterjesztésű fájlokat is (pl. teszt.av1.mkv -> teszt.av1.tmp.mkv)
        file_name = video_path.name
        # Keresünk egy pontot, ahol be tudjuk szúrni a .tmp-ot a végső kiterjesztés elé
        if '.' in file_name:
            # Megkeressük az utolsó pontot (a végső kiterjesztés)
            last_dot_index = file_name.rfind('.')
            temp_name = file_name[:last_dot_index] + '.tmp' + file_name[last_dot_index:]
        else:
            # Ha nincs kiterjesztés, csak hozzáfűzzük a .tmp-ot
            temp_name = file_name + '.tmp'
        temp_path = Path(video_path.parent / temp_name)
        ffmpeg_cmd = [
            FFMPEG_PATH, '-i', video_str,
            '-map', '0',            # Minden stream megőrzése (videó, hang, felirat, melléklet)
            '-c', 'copy',           # Copy minden streamet
            '-map_chapters', '0',   # Fejezetek megőrzése
            '-map_metadata', '0',   # Meglévő metaadatok megtartása
            '-metadata', f'Settings={new_settings}',
            '-metadata:s:v:0', f'Settings={new_settings}',
            '-y',  # Overwrite
        ]
        

            
        ffmpeg_cmd.append(os.fspath(temp_path))
        
        if logger:
            logger.write(f"Új Settings: {new_settings}\n")
            logger.write(f"FFmpeg parancs: {format_cmd_for_windows(ffmpeg_cmd)}\n")
            logger.flush()
        
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600, startupinfo=get_startup_info())
        
        if result.returncode == 0:
            # Sikeres, átnevezzük a fájlt
            if temp_path.exists():
                temp_path.replace(video_path)
                if logger:
                    logger.write(f"[OK] Metadata frissítés sikeres: {video_path.name}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
                return True
            else:
                if logger:
                    logger.write(f"[ERROR] Hiba: Temp fájl nem jött létre: {temp_path}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
        else:
            if logger:
                logger.write(f"[ERROR] FFmpeg hiba (returncode: {result.returncode}):\n")
                if result.stderr:
                    logger.write(f"STDERR: {result.stderr}\n")
                if result.stdout:
                    logger.write(f"STDOUT: {result.stdout}\n")
                logger.write(f"{'='*80}\n\n")
                logger.flush()
        
        return False
        
    except Exception as e:
        error_msg = f"[ERROR] Metaadat frissítés hiba: {e}"
        if logger:
            logger.write(f"\n{error_msg}\n")
            import traceback
            logger.write(f"Traceback:\n{traceback.format_exc()}\n")
            logger.write(f"{'='*80}\n\n")
            logger.flush()
        print(error_msg)
        # Temp fájl törlése hiba esetén
        try:
            if temp_path and temp_path.exists() and not DEBUG_MODE:
                temp_path.unlink()
            elif temp_path and temp_path.exists() and DEBUG_MODE:
                print(f"  [STOP] DEBUG: Metadata temp KEPT: {temp_path}")
        except (OSError, PermissionError):
            pass
        return False
