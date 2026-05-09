import os
import copy
import shutil
import subprocess
import json
import re
import time
import threading
import random
import tempfile
from pathlib import Path
from datetime import datetime
from PIL import Image

from .i18n import t, format_localized_number, LANGUAGE_MAP
from .core_preamble_and_imports import (
    VIDEO_EXTENSIONS, SUBTITLE_EXTENSIONS, CURRENT_LANGUAGE,
    FRAME_MISMATCH_RATIO, FRAME_MISMATCH_MIN_DIFF,
    MAX_MEAN_BRIGHTNESS, MIN_STD_DEV, MIN_FILE_SIZE_BYTES, MIN_FRAME_FILE_SIZE,
    STOP_EVENT, DEBUG_MODE,
    FFMPEG_PATH, FFPROBE_PATH, VDUB2_PATH, MKVMERGE_PATH,
    ACTIVE_PROCESSES, ACTIVE_PROCESSES_LOCK,
    format_cmd_for_windows
)
from .core_paths_tools_logging import (
    EncodingStopped, managed_subprocess, get_startup_info, 
    console_redirect, debug_pause, find_virtualdub
)
from .core_subtitles_and_metadata import find_subtitle_files, normalize_language_code, detect_subtitle_encoding

def wait_for_file_ready(file_path, max_wait_seconds=5, check_interval=0.1):
    """Wait for file to be fully written and accessible (size stabilized).
    
    On Windows, files may be locked for a short time after encoding.
    This function waits until the file size becomes stable.
    CRITICAL: Does NOT try to open the file (to avoid Windows lock hang).
    
    Args:
        file_path: Path to the file to check.
        max_wait_seconds: Maximum time to wait in seconds.
        check_interval: Interval between checks in seconds.
        
    Returns:
        bool: True if file is ready (size stable), False if timeout.
    """
    if not file_path.exists():
        return False
    
    start_time = time.time()
    last_size = None
    stable_count = 0
    required_stable_checks = 2  # Require 2 consecutive stable size checks
    
    while time.time() - start_time < max_wait_seconds:
        try:
            # Check file size WITHOUT opening the file
            # This avoids Windows file locking issues
            current_size = file_path.stat().st_size
            
            if last_size is not None and current_size == last_size:
                # Size is stable
                stable_count += 1
                if stable_count >= required_stable_checks:
                    # Size has been stable for multiple checks - file is ready
                    return True
            else:
                # Size changed - reset counter
                stable_count = 0
            
            last_size = current_size
            time.sleep(check_interval)
            
        except (OSError, PermissionError, IOError, FileNotFoundError):
            # File access error - wait and retry
            time.sleep(check_interval)
    
    # Timeout - file might still be locked, but we'll try anyway
    return file_path.exists()


def check_disk_space(dest_path, required_bytes):
    """Check if destination drive has enough free space.
    
    Args:
        dest_path: Destination path (file or directory).
        required_bytes: Required space in bytes.
        
    Returns:
        bool: True if enough space (or cannot determine), False otherwise.
    """
    try:
        # Find closest existing parent directory
        p = Path(dest_path)
        # If p is a file that doesn't exist, start with parent
        if not p.exists() or not p.is_dir():
            p = p.parent
            
        # Walk up the tree until we find an existing directory
        while not p.exists() and len(p.parts) > 1:
            p = p.parent
            
        if not p.exists():
             # Cannot determine mount point/drive, allow operation to try
             return True
             
        total, used, free = shutil.disk_usage(p)
        
        # Add a safety buffer (e.g. 100MB) to be safe
        safety_buffer = 100 * 1024 * 1024
        
        return free >= (required_bytes + safety_buffer)
    except Exception as e:
        print(f"[WARN] Warning: Could not check disk space: {e}")
        return True

def copy_video_and_subtitles(source_video_path, dest_video_path, overwrite=False):
    """Copy video file and its associated subtitles to the destination.
    
    Also handles copying of valid subtitles found next to the source video.
    
    Args:
        source_video_path: Source video path.
        dest_video_path: Destination video path.
        overwrite: If True, delete existing file before copying. Default False.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    # Copy video file and associated subtitles.

    # Check if destination video already exists
    if dest_video_path.exists():
        if overwrite:
            # Delete existing file before copying (for re-encoding)
            try:
                dest_video_path.unlink()
            except Exception as e:
                print(f"[ERROR] Failed to delete existing file: {dest_video_path.name} - {e}")
                return False
        else:
            print(f"[WARN] Video already exists at destination, skipping: {dest_video_path.name}")
            return False
    
    try:
        # Copy video
        dest_video_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[NOTE] Copying video: {source_video_path.name}")
        
        # Check disk space
        try:
            source_size = source_video_path.stat().st_size
            if not check_disk_space(dest_video_path, source_size):
                print(f"[ERROR] Not enough disk space to copy: {source_video_path.name}")
                print(f"  Required: {source_size / (1024**2):.1f} MB (+buffer)")
                return False
        except Exception as e:
            print(f"[WARN] Disk space check failed: {e}")
        
        shutil.copy2(source_video_path, dest_video_path)
        
        # Wait for file to be fully written and accessible (Windows file locking)
        wait_for_file_ready(dest_video_path, max_wait_seconds=5, check_interval=0.1)
        
        print(f"[OK] Video copied: {dest_video_path}")
        
        # Find and copy subtitles
        subtitle_files = find_subtitle_files(source_video_path)
        if subtitle_files:
            for sub_path, lang_part in subtitle_files:
                # Calculate destination subtitle path
                dest_sub_name = dest_video_path.stem
                if lang_part:
                    dest_sub_name += f".{lang_part}"
                dest_sub_name += sub_path.suffix
                dest_sub_path = dest_video_path.parent / dest_sub_name
                
                print(f"[NOTE] Copying subtitle: {sub_path.name}")
                shutil.copy2(sub_path, dest_sub_path)
                print(f"[OK] Subtitle copied: {dest_sub_path}")
        
        return True
    except Exception as e:
        print(f"[ERROR] Copy error: {e}")
        # If video copy failed, cleanup partial result
        if dest_video_path.exists():
            try:
                dest_video_path.unlink()
            except (OSError, PermissionError):
                pass
        return False


def copy_video_fallback(source_path, dest_path, subtitle_files, logger=None, invalid_subtitles=None):
    """Copy video unchanged when encoding fails (preserves original extension).
    
    Used when no suitable CRF can be found for encoding. Copies source video
    to destination with original extension, plus all valid subtitles.
    
    Args:
        source_path: Source video path (Path).
        dest_path: Destination path with original extension (Path).
        subtitle_files: List of (subtitle_path, language) tuples.
        logger: Logger instance for console output (ConsoleLogger).
        invalid_subtitles: Optional list of (subtitle_path, language, reason) tuples.
        
    Returns:
        bool: True if copy successful, False otherwise.
    """
    try:
        if logger:
            with console_redirect(logger):
                print(f"\n{'='*80}")
                print(f"[NOTE] COPY UNCHANGED (no suitable CRF)")
                print(f"{'='*80}")
                print(f"Source: {source_path}")
                print(f"Dest: {dest_path}")
                print(f"Original extension preserved: {source_path.suffix}")
        
        # Copy video file
        # Copy video file
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check disk space
        try:
            source_size = source_path.stat().st_size
            if not check_disk_space(dest_path, source_size):
                msg = f"[ERROR] Not enough disk space to copy: {source_path.name}"
                if logger:
                    with console_redirect(logger):
                        print(msg)
                        print(f"  Required: {source_size / (1024**2):.1f} MB (+buffer)")
                return False
        except Exception as e:
            if logger:
                 with console_redirect(logger):
                    print(f"[WARN] Disk space check failed: {e}")
                    
        shutil.copy2(source_path, dest_path)
        
        # Wait for file to be fully written and accessible (Windows file locking)
        wait_for_file_ready(dest_path, max_wait_seconds=5, check_interval=0.1)
        
        # Copy valid subtitles
        subtitle_count = 0
        for sub_path, lang in subtitle_files:
            dest_sub = dest_path.parent / sub_path.name
            shutil.copy2(sub_path, dest_sub)
            subtitle_count += 1
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  [OK] Subtitle copied: {sub_path.name}{lang_display}")

        # Copy invalid subtitles (if any)
        if invalid_subtitles:
            for sub_path, lang, reason in invalid_subtitles:
                dest_sub = dest_path.parent / sub_path.name
                # Avoid overwriting if already copied (unlikely but possible)
                if not dest_sub.exists():
                    shutil.copy2(sub_path, dest_sub)
                    subtitle_count += 1
                    if logger:
                        with console_redirect(logger):
                            lang_display = f" ({lang})" if lang else ""
                            print(f"  [OK] Invalid subtitle copied: {sub_path.name}{lang_display} (Reason: {reason})")
        
        if logger:
            with console_redirect(logger):
                size_mb = dest_path.stat().st_size / (1024**2)
                size_str = format_localized_number(size_mb, decimals=1)
                print(f"\n[OK] Copy successful: {dest_path.name}")
                print(f"  Size: {size_str} MB")
                if subtitle_count:
                    print(f"  Subtitles: {subtitle_count} pcs")
                print(f"{'='*80}\n")
        
        return True
        
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Copy error: {e}")
        return False


def copy_non_video_files(source_root, dest_root, progress_callback=None, stop_event=None):
    """Copy non-video files (nfo, jpg, png, txt) from source to destination.

    Uses parallel source/destination indexing and (size, mtime) comparison.
    Copy is interruptible and writes to a temporary `.partial` file first, then
    atomically replaces destination on success.

    Args:
        source_root: Source root directory.
        dest_root: Destination root directory.
        progress_callback: Optional callback for progress updates.
        stop_event: Optional threading.Event for immediate stop support.
    """
    if not dest_root:
        return 0

    if stop_event is None:
        stop_event = STOP_EVENT

    def _stop_requested():
        try:
            return bool(stop_event and stop_event.is_set())
        except Exception:
            return False

    source_path = Path(source_root)
    dest_path = Path(dest_root)

    try:
        source_resolved = source_path.resolve()
    except OSError:
        source_resolved = source_path
    try:
        dest_resolved = dest_path.resolve()
    except OSError:
        dest_resolved = dest_path

    if source_resolved == dest_resolved:
        if progress_callback:
            progress_callback("Source and destination are the same, skipping non-video copy.")
        return 0

    skip_dest_subtree = False
    try:
        dest_resolved.relative_to(source_resolved)
        skip_dest_subtree = True
    except ValueError:
        skip_dest_subtree = False

    def _normalize_rel_key(rel_path):
        return os.path.normcase(os.path.normpath(str(rel_path)))

    def _copy_file_interruptible(source_file, dest_file, chunk_size=8 * 1024 * 1024):
        """Copy via temp file; abort-safe and atomic destination replace."""
        partial_file = dest_file.with_name(dest_file.name + ".partial")
        try:
            try:
                if partial_file.exists():
                    partial_file.unlink()
            except (OSError, PermissionError):
                pass

            with source_file.open('rb') as src_fp, partial_file.open('wb') as dst_fp:
                while True:
                    if _stop_requested():
                        raise EncodingStopped("Non-video copy interrupted")
                    chunk = src_fp.read(chunk_size)
                    if not chunk:
                        break
                    dst_fp.write(chunk)
                dst_fp.flush()

            if _stop_requested():
                raise EncodingStopped("Non-video copy interrupted")

            try:
                shutil.copystat(source_file, partial_file)
            except (OSError, PermissionError, shutil.Error):
                pass

            os.replace(partial_file, dest_file)
        except EncodingStopped:
            try:
                if partial_file.exists():
                    partial_file.unlink()
            except (OSError, PermissionError):
                pass
            raise
        except Exception:
            try:
                if partial_file.exists():
                    partial_file.unlink()
            except (OSError, PermissionError):
                pass
            raise

    source_entries = []
    source_scan_error = [None]
    dest_scan_error = [None]
    dest_meta_by_key_holder = [{}]

    def _scan_source_non_video():
        file_count = 0
        last_count_update = time.time()
        start_time = time.time()
        try:
            for root, _, files in os.walk(source_path):
                if _stop_requested():
                    return
                root_path = Path(root)
                for file_name in files:
                    if _stop_requested():
                        return
                    source_file = root_path / file_name

                    try:
                        current_resolved = source_file.resolve()
                    except OSError:
                        current_resolved = source_file

                    if skip_dest_subtree and (current_resolved == dest_resolved or dest_resolved in current_resolved.parents):
                        continue

                    if (source_file.suffix.lower() in VIDEO_EXTENSIONS or
                        source_file.suffix.lower() in SUBTITLE_EXTENSIONS):
                        continue

                    try:
                        src_stat = source_file.stat()
                    except (OSError, PermissionError):
                        continue

                    relative_path = source_file.relative_to(source_path)
                    source_entries.append((
                        source_file,
                        relative_path,
                        _normalize_rel_key(relative_path),
                        src_stat.st_size,
                        src_stat.st_mtime,
                    ))
                    file_count += 1

                    current_time = time.time()
                    if current_time - last_count_update >= 1.0:
                        if progress_callback:
                            elapsed_str = f"{int(current_time - start_time)}s"
                            progress_callback(("copy_progress", file_count, 0, f"Searching for non-video files... ({file_count} found, {elapsed_str})"))
                        last_count_update = current_time
        except Exception as e:
            source_scan_error[0] = e

    def _scan_dest_non_video():
        dest_meta_by_key = {}
        try:
            for root, _, files in os.walk(dest_path):
                if _stop_requested():
                    return
                root_path = Path(root)
                for file_name in files:
                    if _stop_requested():
                        return
                    dest_file = root_path / file_name

                    if (dest_file.suffix.lower() in VIDEO_EXTENSIONS or
                        dest_file.suffix.lower() in SUBTITLE_EXTENSIONS):
                        continue

                    try:
                        dst_stat = dest_file.stat()
                    except (OSError, PermissionError):
                        continue

                    try:
                        relative_path = dest_file.relative_to(dest_path)
                    except ValueError:
                        continue

                    dest_meta_by_key[_normalize_rel_key(relative_path)] = (
                        dst_stat.st_size,
                        dst_stat.st_mtime,
                    )
            dest_meta_by_key_holder[0] = dest_meta_by_key
        except Exception as e:
            dest_scan_error[0] = e

    if progress_callback:
        progress_callback(("copy_status", "Indexing source and destination non-video files..."))

    source_scan_thread = threading.Thread(target=_scan_source_non_video, daemon=True, name="NonVideoSourceScan")
    dest_scan_thread = threading.Thread(target=_scan_dest_non_video, daemon=True, name="NonVideoDestScan")
    source_scan_thread.start()
    dest_scan_thread.start()
    source_scan_thread.join()
    dest_scan_thread.join()

    if _stop_requested():
        if progress_callback:
            progress_callback(("copy_done", 0, 0, "Non-video copy cancelled by immediate stop."))
        return 0

    if source_scan_error[0] is not None:
        if progress_callback:
            progress_callback(("copy_error", 0, 0, f"Error searching source files: {source_scan_error[0]}"))
        return 0

    if dest_scan_error[0] is not None:
        if progress_callback:
            progress_callback(("copy_error", 0, 0, f"Error searching destination files: {dest_scan_error[0]}"))
        return 0

    total_files = len(source_entries)
    if total_files == 0:
        if progress_callback:
            progress_callback(("copy_done", 0, 0, "No non-video files to copy."))
        return 0

    dest_meta_by_key = dest_meta_by_key_holder[0]

    def _mtime_second_key(mtime_value):
        try:
            return int(float(mtime_value))
        except (TypeError, ValueError):
            return None

    def _mtime_close_enough(src_mtime, dst_mtime, tolerance_seconds=1.0):
        """Allow tiny timestamp drift when file size is identical."""
        try:
            return abs(float(src_mtime) - float(dst_mtime)) <= float(tolerance_seconds)
        except (TypeError, ValueError):
            return False

    files_to_copy = []
    unchanged_count = 0
    missing_count = 0
    size_diff_count = 0
    newer_count = 0
    for source_file, relative_path, rel_key, source_size, source_mtime in source_entries:
        if _stop_requested():
            if progress_callback:
                progress_callback(("copy_done", 0, 0, "Non-video copy cancelled by immediate stop."))
            return 0

        dest_meta = dest_meta_by_key.get(rel_key)
        if dest_meta is None:
            missing_count += 1
            files_to_copy.append((source_file, relative_path))
            continue

        dest_size, dest_mtime = dest_meta
        if dest_size != source_size:
            size_diff_count += 1
            files_to_copy.append((source_file, relative_path))
            continue

        # mtime compare: second-level key + source-newer rule.
        # This avoids endless recopy when destination FS has lower mtime precision.
        src_sec = _mtime_second_key(source_mtime)
        dst_sec = _mtime_second_key(dest_mtime)
        if src_sec is not None and dst_sec is not None and src_sec == dst_sec:
            unchanged_count += 1
            continue

        # Minimal mtime drift tolerance for same-size files.
        if _mtime_close_enough(source_mtime, dest_mtime):
            unchanged_count += 1
            continue

        # If destination is same size and at least as new, no copy needed.
        try:
            if float(source_mtime) <= float(dest_mtime):
                unchanged_count += 1
                continue
        except (TypeError, ValueError):
            pass

        newer_count += 1
        files_to_copy.append((source_file, relative_path))

    if not files_to_copy:
        if progress_callback:
            progress_callback(("copy_done", total_files, total_files, f"Non-video files are up to date. ({total_files} checked, 0 copied)"))
        return 0

    copy_total = len(files_to_copy)
    copied_count = 0
    processed_count = 0
    last_update_time = time.time()
    update_interval = 0.5
    copy_cancelled = False

    if progress_callback:
        progress_callback((
            "copy_start",
            copy_total,
            0,
            f"Copying changed non-video files... (0/{copy_total}, unchanged: {unchanged_count}, missing: {missing_count}, size diff: {size_diff_count}, newer: {newer_count})"
        ))

    for source_file, relative_path in files_to_copy:
        if _stop_requested():
            copy_cancelled = True
            break

        dest_file = dest_path / relative_path

        try:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            if progress_callback:
                progress_callback(("copy_error", 0, 0, f"Error creating directory: {relative_path.parent} - {e}"))
            continue

        processed_count += 1

        try:
            _copy_file_interruptible(source_file, dest_file)
            copied_count += 1
        except EncodingStopped:
            copy_cancelled = True
            break
        except (OSError, PermissionError, shutil.Error) as e:
            if progress_callback:
                progress_callback(("copy_error", 0, 0, f"Error copying: {relative_path.name[:50]}... - {e}"))

        current_time = time.time()
        if current_time - last_update_time >= update_interval:
            if progress_callback:
                status_text = f"Copying: {relative_path.name[:50]}... ({processed_count}/{copy_total}, unchanged: {unchanged_count})"
                progress_callback(("copy_progress", copy_total, processed_count, status_text))
            last_update_time = current_time

    if progress_callback:
        if copy_cancelled:
            progress_callback((
                "copy_done",
                copy_total,
                processed_count,
                f"Non-video copy cancelled: {copied_count} files copied before stop. ({processed_count}/{copy_total})"
            ))
        else:
            progress_callback(("copy_done", copy_total, processed_count, f"{copied_count} files copied, {unchanged_count} unchanged. ({total_files} checked)"))

    return copied_count

def get_video_info(video_path):
    """Retrieve video duration and FPS using FFprobe.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        tuple: (duration_seconds, fps) or (None, None) on error.
    """
    if not video_path.exists():
        return None, None

    try:
        # First try: Get duration, avg_frame_rate and r_frame_rate from stream
        # We use nokey=0 to get key=value pairs for reliable parsing
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=avg_frame_rate,r_frame_rate,duration',
            '-of', 'default=noprint_wrappers=1:nokey=0',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        lines = result.stdout.strip().split('\n')
        
        duration = None
        avg_fps = None
        r_fps = None
        
        for line in lines:
            line = line.strip()
            if not line or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            if key == 'duration':
                try:
                    duration = float(value)
                except ValueError:
                    pass
            elif key in ('avg_frame_rate', 'r_frame_rate'):
                if '/' in value:
                    try:
                        num, den = map(int, value.split('/'))
                        if den > 0:
                            fps_val = num / den
                            if key == 'avg_frame_rate':
                                avg_fps = fps_val
                            else:
                                r_fps = fps_val
                    except ValueError:
                        pass
        
        # Determine FPS: prefer avg_frame_rate, fallback to r_frame_rate with sanity check
        fps = None
        if avg_fps and avg_fps > 0:
            fps = avg_fps
        elif r_fps and r_fps > 0:
            # Sanity check: r_frame_rate can be the timebase (e.g. 90000)
            # We reject unreasonably high FPS values (e.g. > 1000)
            if r_fps < 1000:
                fps = r_fps
        
        # If stream duration is not available, try format duration
        if not duration or duration < 1:
            try:
                cmd_format = [
                    FFPROBE_PATH, '-v', 'error',
                    '-show_entries', 'format=duration',
                    '-of', 'default=noprint_wrappers=1:nokey=1',
                    os.fspath(video_path)
                ]
                result_format = subprocess.run(cmd_format, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
                duration_str = result_format.stdout.strip()
                if duration_str:
                    try:
                        duration = float(duration_str)
                    except ValueError:
                        pass
            except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
                pass
                    
        return duration, fps
        
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>120s) during get_video_info: {video_path.name}")
        return None, None
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        return None, None

def get_video_resolution(video_path):
    """Retrieve video resolution (width, height) using FFprobe.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        tuple: (width, height) or (None, None) on error.
    """
    if not video_path.exists():
        return None, None

    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'default=noprint_wrappers=1:nokey=0',
            os.fspath(video_path)
        ]
        print(f"FFprobe Resolution CMD: {format_cmd_for_windows(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        lines = result.stdout.strip().split('\n')
        
        width = None
        height = None
        
        for line in lines:
            line = line.strip()
            if not line or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            if key == 'width':
                try:
                    width = int(value)
                except ValueError:
                    pass
            elif key == 'height':
                try:
                    height = int(value)
                except ValueError:
                    pass
        
        return width, height
        
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>30s) during get_video_resolution: {video_path.name}")
        return None, None
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        return None, None

def get_frame_count(video_path):
    """Retrieve frame count using FFprobe or calculate from duration/fps.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        int: Frame count or None on error.
    """
    if not video_path.exists():
        return None

    cmd_frames = [
        FFPROBE_PATH, '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=nb_frames',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        os.fspath(video_path)
    ]
    print(f"FFprobe Frame Count CMD: {format_cmd_for_windows(cmd_frames)}")
    try:
        result = subprocess.run(cmd_frames, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        frames_str = result.stdout.strip()
        if frames_str and frames_str.upper() != 'N/A':
            return int(frames_str)
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>30s) during get_frame_count: {video_path.name}")
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        pass

    # MKV/AV1 often has nb_frames=N/A. In that case packet count is the most
    # reliable frame count for video streams.
    cmd_packets = [
        FFPROBE_PATH, '-v', 'error',
        '-select_streams', 'v:0',
        '-count_packets',
        '-show_entries', 'stream=nb_read_packets',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        os.fspath(video_path)
    ]
    print(f"FFprobe Packet Count CMD: {format_cmd_for_windows(cmd_packets)}")
    try:
        result_packets = subprocess.run(
            cmd_packets,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=True,
            timeout=600,
            startupinfo=get_startup_info()
        )
        packets_str = result_packets.stdout.strip()
        if packets_str and packets_str.upper() != 'N/A':
            return int(packets_str)
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>30s) during get_frame_count packet mode: {video_path.name}")
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        pass

    # Fallback: calculate from duration * fps
    duration, fps = get_video_info(video_path)
    if duration and fps:
        try:
            return int(duration * fps)
        except (ValueError, TypeError):
            return None
    return None

def get_output_file_info(output_path):
    """Get information about the output file using FFprobe.
    
    Args:
        output_path: Path to the output file.
        
    Returns:
        tuple: (cq_crf, vmaf, psnr, frame_count, file_size, modified_date, encoder_type, should_delete, duration_seconds, denoise_info)
        
        Returns (None, None, None, None, None, None, None, False, None, None) if the file does not exist or an error occurs.
    """
    if not output_path or not output_path.exists():
        return None, None, None, None, None, None, None, False, None, None

    # Initialize results with defaults
    cq_crf = None
    vmaf = None
    psnr = None
    frame_count = None
    file_size = None
    modified_date = None
    encoder_type = None
    duration_seconds = None
    denoise_info = None
    
    # 1. Get basic file stats (Always possible if exists)
    try:
        stat_info = output_path.stat()
        file_size = stat_info.st_size
        modified_timestamp = stat_info.st_mtime
        modified_date = datetime.fromtimestamp(modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"[ERROR] Destination stat error ({output_path.name}): {e}")
        return None, None, None, None, None, None, None, False, None, None

    # 2. Probe for metadata (might fail if ffprobe missing)
    try:
        # Get duration first
        try:
            duration_seconds, _ = get_video_info(output_path)
        except Exception:
            duration_seconds = None

        # Get Settings metadata (CQ/CRF and VMAF) using helper that checks both global and stream
        settings_str = extract_settings_from_file(output_path)
        
        if settings_str:
            # Extract encoder type (NVENC or SVT-AV1)
            # Check NVENC first (as both might be present)
            if 'NVENC' in settings_str.upper() or 'CQ:' in settings_str:
                encoder_type = 'nvenc'
            elif 'SVT-AV1' in settings_str.upper() or 'SVT' in settings_str.upper() or 'CRF:' in settings_str:
                encoder_type = 'svt-av1'
            
            # Extract CQ/CRF value
            cq_match = re.search(r'CQ:(\d+)', settings_str)
            crf_match = re.search(r'CRF:(\d+)', settings_str)
            if cq_match:
                cq_crf = int(cq_match.group(1))
            elif crf_match:
                cq_crf = int(crf_match.group(1))
            
            # Extract VMAF value (Actual VMAF or Planned VMAF)
            # Handles both "Actual VMAF: 94.11" and "Planned VMAF: 95.0"
            vmaf_match = re.search(r'(?:Actual|Planned)\s+VMAF:\s*([\d.,]+)', settings_str)
            if vmaf_match:
                vmaf_str = vmaf_match.group(1).replace(',', '.')  # Handle localized decimals
                vmaf = float(vmaf_str)
            
            # Extract PSNR value
            psnr_match = re.search(r'PSNR:\s*([\d.,]+)', settings_str)
            if psnr_match:
                psnr_str = psnr_match.group(1).replace(',', '.')  # Handle localized decimals
                psnr = float(psnr_str)

            # Extract Filter information (detailed denoise filters)
            denoise_match = re.search(r'Filters:\[(.*?)\]', settings_str)
            if denoise_match:
                denoise_info = denoise_match.group(1)
            else:
                denoise_level_match = re.search(
                    r'\bDenoise(?:[_\s-]*Level)?\s*[:=]\s*([0-4]|ultra[_\s-]*strong|very[_\s-]*strong|strong|light)\b',
                    settings_str,
                    re.IGNORECASE
                )
                if denoise_level_match:
                    denoise_info = f"DenoiseLevel:{denoise_level_match.group(1)}"
        
        # Get frame count
        cmd_frames = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=nb_frames',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(output_path.absolute())
        ]
        
        try:
            # Verify FFprobe exists before running
            ffprobe_exe = Path(FFPROBE_PATH) if FFPROBE_PATH else None
            if ffprobe_exe and (ffprobe_exe.exists() or shutil.which(FFPROBE_PATH) is not None):
                print(f"FFprobe Output Frames CMD: {format_cmd_for_windows(cmd_frames)}")
                result_frames = subprocess.run(cmd_frames, capture_output=True, text=True, encoding='utf-8', errors='replace', check=False, timeout=600, startupinfo=get_startup_info())
                if result_frames.returncode == 0:
                    frames_str = result_frames.stdout.strip()
                    if frames_str:
                        frame_count = int(frames_str)
        except (ValueError, TypeError, AttributeError, subprocess.CalledProcessError):
            pass

        # If nb_frames is missing (common in MKV), try packet count.
        if frame_count is None:
            cmd_packets = [
                FFPROBE_PATH, '-v', 'error',
                '-select_streams', 'v:0',
                '-count_packets',
                '-show_entries', 'stream=nb_read_packets',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                os.fspath(output_path.absolute())
            ]
            try:
                ffprobe_exe = Path(FFPROBE_PATH) if FFPROBE_PATH else None
                if ffprobe_exe and (ffprobe_exe.exists() or shutil.which(FFPROBE_PATH) is not None):
                    print(f"FFprobe Output Packets CMD: {format_cmd_for_windows(cmd_packets)}")
                    result_packets = subprocess.run(
                        cmd_packets,
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        check=False,
                        timeout=600,
                        startupinfo=get_startup_info()
                    )
                    if result_packets.returncode == 0:
                        packets_str = result_packets.stdout.strip()
                        if packets_str and packets_str.upper() != 'N/A':
                            frame_count = int(packets_str)
            except (ValueError, TypeError, AttributeError, subprocess.CalledProcessError):
                pass

        # Last resort fallback: calculate from duration and fps.
        if frame_count is None:
            try:
                duration, fps = get_video_info(output_path)
                if duration and fps:
                    frame_count = int(duration * fps)
            except (ValueError, TypeError, AttributeError, Exception):
                pass
                
    except Exception as e:
        print(f"[ERROR] Destination FFprobe error ({output_path.name}): {e}")
        # Return whatever we managed to collect (at least file stats)
        
    return cq_crf, vmaf, psnr, frame_count, file_size, modified_date, encoder_type, False, duration_seconds, denoise_info


def extract_settings_from_file(file_path):
    """Extract Settings metadata tag from global or video stream tags.
    
    Args:
        file_path: Path to the video file.
        
    Returns:
        str: Settings string or empty string if not found.
    """
    if not file_path or not file_path.exists():
        return ""
    
    # Verify FFprobe exists before running
    ffprobe_exe = Path(FFPROBE_PATH) if FFPROBE_PATH else None
    if not ffprobe_exe or (not ffprobe_exe.exists() and shutil.which(FFPROBE_PATH) is None):
        return ""

    try:
        # 1. Try global metadata (format tags)
        probe_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format_tags=Settings',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(file_path.absolute())
        ]
        
        result = subprocess.run(probe_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=False, timeout=600, startupinfo=get_startup_info())
        settings_str = result.stdout.strip() if (result.returncode == 0 and result.stdout) else ""
        
        if settings_str:
            return settings_str
            
        # 2. If not in global, try video stream 0 metadata (stream tags)
        probe_cmd_stream = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream_tags=Settings',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(file_path.absolute())
        ]
        result_stream = subprocess.run(probe_cmd_stream, capture_output=True, text=True, encoding='utf-8', errors='replace', check=False, timeout=600, startupinfo=get_startup_info())
        settings_str = result_stream.stdout.strip() if (result_stream.returncode == 0 and result_stream.stdout) else ""
        
        return settings_str
    except Exception:
        return ""


def extract_all_global_tags(file_path):
    """Extract all global tags (format tags) from file using FFprobe.
    
    Args:
        file_path: Path to the video file.
        
    Returns:
        dict: Dictionary of tags {key: value}.
    """
    if not file_path or not file_path.exists():
        return {}
    
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format_tags',
            '-of', 'json',
            os.fspath(file_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        format_info = data.get('format', {})
        tags = format_info.get('tags', {})
        return tags
    except Exception:
        return {}


def frames_significantly_different(source_frames, output_frames):
    """Determine if output frame count differs significantly from source."""
    if source_frames is None or output_frames is None:
        return False
    allowed_diff = max(FRAME_MISMATCH_MIN_DIFF, int(source_frames * FRAME_MISMATCH_RATIO))
    return abs(source_frames - output_frames) > allowed_diff

def normalize_audio_lang(lang_string):
    """Normalize audio language code for comparison."""
    if not lang_string:
        return 'unknown'
    lang_clean = lang_string.strip().lower()
    # If there is a hyphen, take only the first part (e.g. "hun-HUN" -> "hun")
    if '-' in lang_clean:
        lang_clean = lang_clean.split('-')[0]
    
    # If already 2 chars code, check if in LANGUAGE_MAP
    if len(lang_clean) == 2:
        if lang_clean in LANGUAGE_MAP:
            return lang_clean
        return lang_clean
    
    # If 3 chars or longer, search for reverse mapping in LANGUAGE_MAP
    # LANGUAGE_MAP format: 'hu': 'hun', 'en': 'eng', etc.
    # Search for key where value matches lang_clean
    for key, value in LANGUAGE_MAP.items():
        if value == lang_clean:
            # If key is 2 chars, return it
            if len(key) == 2:
                return key
            # If key is 3 chars, return first 2 chars
            if len(key) == 3:
                return key[:2]
        # If key matches lang_clean and is 2 chars
        if key == lang_clean and len(key) == 2:
            return key
    
    # If 3 chars language code (e.g. "hun", "eng"), return as-is
    if len(lang_clean) == 3:
        return lang_clean
    
    return lang_clean

def get_audio_streams_info(video_path):
    """Analyze audio streams in the video.
    
    Identifies the default language and counts 5.1 and 2.0 streams per language.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        tuple: (default_lang, lang_51_count, lang_20_count)
    """
    try:
        # Összes hangsáv információ lekérdezése (disposition is kell a default hangsávhoz)
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index,channels,disposition',
            '-show_entries', 'stream_tags=language,title',
            '-of', 'json',
            os.fspath(video_path)
        ]
        
        print(f"FFprobe Audio CMD: {format_cmd_for_windows(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        
        if 'streams' not in data or len(data['streams']) == 0:
            return None, {}, {}
        
        default_lang = None
        lang_51_count = {}  # Normalizált nyelv -> 5.1 hangsávok száma
        lang_20_count = {}  # Normalizált nyelv -> 2.0 hangsávok száma
        all_streams = []  # Összes hangsáv információ tárolása
        
        # Első körben összegyűjtjük az összes hangsáv információt
        for stream in data['streams']:
            channels = stream.get('channels', 0)
            tags = stream.get('tags', {})
            lang = tags.get('language', '') or tags.get('lang', '') or ''
            disposition = stream.get('disposition', {})
            # A disposition objektum tartalmazza a 'default' mezőt (0 vagy 1)
            is_default = disposition.get('default', 0) == 1 if isinstance(disposition, dict) else False
            
            lang_normalized = normalize_audio_lang(lang) if lang else 'unknown'
            
            all_streams.append({
                'lang': lang_normalized,
                'channels': channels,
                'is_default': is_default
            })
            
            # 5.1 hangsávok számlálása nyelv szerint (6 csatorna)
            if channels == 6:
                lang_51_count[lang_normalized] = lang_51_count.get(lang_normalized, 0) + 1
            # 2.0 hangsávok számlálása nyelv szerint (2 csatorna)
            elif channels == 2:
                lang_20_count[lang_normalized] = lang_20_count.get(lang_normalized, 0) + 1
        
        # Alapértelmezett nyelv meghatározása prioritás szerint:
        # 1. Explicit default hangsáv (disposition:default=1)
        for stream_info in all_streams:
            if stream_info['is_default']:
                default_lang = stream_info['lang']
                break
        
        # 2. Ha nincs explicit default, az applikáció nyelvének megfelelő hangsáv
        if default_lang is None or default_lang == 'unknown':
            app_lang = CURRENT_LANGUAGE  # 'hu' vagy 'en'
            # Keresünk olyan hangsávot, amelynek a normalizált nyelve megegyezik az applikáció nyelvével
            for stream_info in all_streams:
                if stream_info['lang'] == app_lang:
                    default_lang = stream_info['lang']
                    break
        
        # 3. If still none, use the first stream
        if default_lang is None or default_lang == 'unknown':
            if len(all_streams) > 0:
                default_lang = all_streams[0]['lang']
            else:
                default_lang = 'unknown'
        
        return default_lang, lang_51_count, lang_20_count
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>30s) during get_audio_streams_info: {video_path.name}")
        return None, {}, {}
    except Exception as e:
        print(f"[ERROR] FFprobe audio info error: {e}")
        return None, {}, {}

def get_video_stream_size_bytes_exact(video_path, timeout=600):
    """Calculate exact video stream size by summing packet sizes.
    
    This is slow (reads entire file) but accurate when metadata is wrong.
    Use only as a fallback for problematic files.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'packet=size',
            '-of', 'csv=p=0',
            os.fspath(video_path)
        ]
        
        print(f"FFprobe Exact Size Scan: {video_path.name}...")
        start_time = time.time()
        
        # Use subprocess with pipe to process line by line to safe memory
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True, 
            encoding='utf-8', 
            errors='replace',
            startupinfo=get_startup_info()
        )
        
        total_size = 0
        try:
            for line in process.stdout:
                if line.strip():
                    total_size += int(line.strip())
                
                # Timeout check inside loop
                if time.time() - start_time > timeout:
                    process.terminate()
                    print(f"[WARN] Size scan timed out (> {timeout}s)")
                    return None
        except Exception:
            process.terminate()
            return None
            
        process.wait()
        
        if process.returncode == 0:
            return total_size
            
    except Exception as e:
        print(f"[ERROR] Exact size scan failed: {e}")
        return None
    return None

def get_audio_streams_total_size_mb(video_path):
    """Calculate total size of all audio streams in MB.
    
    Uses bitrate and duration to calculate stream sizes.
    This is used to provide more accurate file size estimates during CRF search.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        float: Total audio size in MB, or 0.0 if calculation fails.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=bit_rate,duration',
            '-show_entries', 'stream_tags',
            '-show_entries', 'format=duration',
            '-of', 'json',
            os.fspath(video_path)
        ]
        print(f"FFprobe Audio Size CMD: {format_cmd_for_windows(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8',
                               errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        
        # Get format duration as fallback
        format_duration = None
        if 'format' in data and 'duration' in data['format']:
            try:
                format_duration = float(data['format']['duration'])
            except (ValueError, TypeError):
                pass
        
        total_size_bytes = 0.0
        
        for stream in data.get('streams', []):
            bit_rate = stream.get('bit_rate')
            duration = stream.get('duration')
            tags = stream.get('tags', {})
            
            # 1. Try direct size from tags (NUMBER_OF_BYTES) - MKV specific, most accurate
            size_bytes_tag = tags.get('NUMBER_OF_BYTES') or tags.get('NUMBER_OF_BYTES-eng')
            if size_bytes_tag:
                try:
                    total_size_bytes += float(size_bytes_tag)
                    continue # Found exact size, move to next stream
                except (ValueError, TypeError):
                    pass
            
            # 2. Try to get duration from stream tags if not available
            if duration is None:
                duration_str = tags.get('DURATION')
                if duration_str:
                    # Convert from HH:MM:SS.mmm format
                    try:
                        parts = duration_str.split(':')
                        if len(parts) == 3:
                            h, m, s = parts
                            duration = float(h) * 3600 + float(m) * 60 + float(s)
                    except (ValueError, TypeError):
                        pass
            
            # Use format duration as last resort
            if duration is None:
                duration = format_duration
            
            # 3. Try to find bitrate in tags if missing (VBR AAC often lacks bit_rate but has BPS tag)
            if not bit_rate:
                bit_rate = tags.get('BPS') or tags.get('BPS-eng')
            
            # Calculate stream size if we have both values
            if bit_rate and duration:
                try:
                    size_bytes = (float(bit_rate) * float(duration)) / 8
                    total_size_bytes += size_bytes
                except (ValueError, TypeError):
                    pass
        
        
        # Sanity check: Total audio size cannot exceed file size
        try:
            file_stats = video_path.stat()
            file_size_bytes = file_stats.st_size
            if total_size_bytes > file_size_bytes:
                print(f"[WARN] Metadata audio size ({total_size_bytes / 1024**2:.1f} MB) > File size ({file_size_bytes / 1024**2:.1f} MB). Refusing to trust metadata.")
                # Fallback: estimate audio size as 20% of file size* 
                # Or just return 0 to assume "video size ≈ file size" (safest for max-encoded-percent calculation)
                # Be conservative: if we don't know audio size, assume it's small/negligible relative to video,
                # so the calculated video bitrate target will be based on the full file size.
                total_size_bytes = 0.0 
        except Exception:
            pass

        # Convert to MB
        total_size_mb = total_size_bytes / (1024 * 1024)
        return total_size_mb
        
    except subprocess.TimeoutExpired:
        print(f"[WARN] FFprobe timeout (>30s) during get_audio_streams_total_size: {video_path.name}")
        return 0.0
    except Exception as e:
        print(f"[ERROR] FFprobe audio size calculation error: {e}")
        return 0.0

def get_audio_stream_details(video_path):
    """Get detailed audio stream information (for menus, removal)."""
    try:
        # First query: stream info
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'stream=index,codec_name,codec_type,channels,channel_layout,bit_rate',
            '-show_entries', 'stream_tags=language,title',
            '-show_entries', 'format=duration',
            '-of', 'json',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=False, timeout=600, startupinfo=get_startup_info())
        if result.returncode != 0:
            # FFprobe failed, return empty list
            return []
        data = json.loads(result.stdout) if result.stdout else {}
        streams = data.get('streams', [])
        format_info = data.get('format', {})
        duration = float(format_info.get('duration', 0)) if format_info.get('duration') else 0
        
        details = []
        audio_index = 0
        for stream in streams:
            if stream.get('codec_type') != 'audio':
                continue
            codec = (stream.get('codec_name') or '').upper() or 'UNKNOWN'
            channels = stream.get('channels', 0)
            channel_layout = stream.get('channel_layout') or ''
            tags = stream.get('tags', {})
            language = tags.get('language') or tags.get('lang') or 'unknown'
            title = tags.get('title', '')
            lang_normalized = normalize_audio_lang(language)
            display_lang = language or lang_normalized or 'unknown'
            
            # Calculate audio size based on bitrate and duration
            bit_rate = stream.get('bit_rate')
            audio_size_mb = None
            if bit_rate and duration:
                try:
                    bit_rate_int = int(bit_rate)
                    # bit_rate is in bps, duration in seconds
                    audio_size_bytes = (bit_rate_int * duration) / 8
                    audio_size_mb = audio_size_bytes / (1024 * 1024)
                except (ValueError, TypeError):
                    pass
            
            if channels == 6:
                channel_label = '5.1'
            elif channels == 2:
                channel_label = '2.0'
            elif channels and isinstance(channels, int):
                channel_label = f'{channels} ch'
            elif channel_layout:
                channel_label = channel_layout
            else:
                channel_label = '*'
            description_parts = [display_lang.upper(), channel_label, codec]
            description = ' | '.join(part for part in description_parts if part)
            if title:
                description = f"{description} - {title}"
            if audio_size_mb is not None:
                # Localized format: Hungarian = comma, English = dot
                size_str = format_localized_number(audio_size_mb, decimals=2)
                description = f"{description} ({size_str} MB)"
            details.append({
                'ffmpeg_audio_index': audio_index,
                'language': display_lang,
                'language_normalized': lang_normalized,
                'channels': channels,
                'channel_layout': channel_layout,
                'codec': codec,
                'title': title,
                'description': description,
                'size_mb': audio_size_mb
            })
            audio_index += 1
        return details
    except Exception as e:
        print(f"[ERROR] Audio stream info error: {e}")
        return []

def get_51_audio_stream_index(video_path, default_lang):
    """Get index of 5.1 audio stream matching the default language.
    
    Args:
        video_path: Path to the video file.
        default_lang: Default language code.
        
    Returns:
        int: Stream index or None.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index,channels',
            '-show_entries', 'stream_tags=language,title',
            '-of', 'json',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        
        if 'streams' not in data or len(data['streams']) == 0:
            return None
        
        # Count in order of audio streams (0, 1, 2, ...)
        audio_stream_index = 0
        for stream in data['streams']:
            channels = stream.get('channels', 0)
            tags = stream.get('tags', {})
            lang = tags.get('language', '') or tags.get('lang', '') or ''
            
            # Normalize language code for comparison
            lang_normalized = normalize_audio_lang(lang)
            
            # If 5.1 stream (6 channels) and same normalized language as default
            if channels == 6 and lang_normalized == default_lang:
                return audio_stream_index
            
            # Next audio stream index
            audio_stream_index += 1
        
        return None
    except Exception as e:
        print(f"[ERROR] 5.1 audio stream search error: {e}")
        return None

def check_audio_compression_needed(video_path):
    """Check if audio dynamic range compression is needed.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        bool: True if compression is recommended.
    """
    try:
        default_lang, lang_51_count, lang_20_count = get_audio_streams_info(video_path)
        
        if default_lang is None:
            return False
        
        # If no 5.1 stream for default language, no compression needed
        if lang_51_count.get(default_lang, 0) == 0:
            return False
        
        # If 2.0 stream exists for default language, no compression needed
        if lang_20_count.get(default_lang, 0) > 0:
            return False
        
        # If 5.1 exists but no 2.0, compression is needed
        return True
    except Exception as e:
        print(f"[ERROR] Audio compression check error: {e}")
        return False

def get_audio_conversion_title(method):
    """Generate metadata title for selected conversion method."""
    method_key = (method or 'fast').lower()
    if method_key == 'dialogue':
        return t('audio_convert_title_dialogue')
    return t('audio_convert_title_fast')

def build_audio_conversion_filter(method='fast'):
    """Build FFmpeg filter chain for 5.1 to 2.0 audio conversion.

    Args:
        method: 'fast' (simple mix with compression) or 'dialogue' (dialogue-centered with normalization).

    Returns:
        str: FFmpeg filter string.
    """
    method_key = (method or 'fast').lower()
    if method_key == 'dialogue':
        # Dialogue-centered: boost center channel (dialogue), dynamic normalization
        return 'pan=stereo|FL<FL+0.5*FC+BL+0.6*SL|FR<FR+0.5*FC+BR+0.6*SR,dynaudnorm=f=250:g=8:m=7.0,alimiter=limit=0.98:level=disabled'
    # Fast/cinema: standard downmix with compression for dynamic range
    return 'pan=stereo|FL<FL+0.5*FC+BL+0.6*SL|FR<FR+0.5*FC+BR+0.6*SR,acompressor=threshold=-18dB:ratio=4:attack=10:release=200:makeup=6:knee=2,alimiter=limit=0.9:level=disabled'


def apply_audio_offset_to_filter_chain(filter_chain, offset_ms):
    """Apply a per-track offset to an audio filter chain."""
    base_chain = str(filter_chain or '').strip()
    try:
        normalized_offset_ms = int(round(float(offset_ms or 0)))
    except (TypeError, ValueError):
        normalized_offset_ms = 0

    if normalized_offset_ms == 0:
        return base_chain, 0

    if normalized_offset_ms > 0:
        delay_filter = f'adelay={normalized_offset_ms}:all=1'
        return (f'{base_chain},{delay_filter}' if base_chain else delay_filter), normalized_offset_ms

    trim_seconds = abs(normalized_offset_ms) / 1000.0
    trim_filter = f'atrim=start={trim_seconds:.3f},asetpts=PTS-STARTPTS'
    return (f'{base_chain},{trim_filter}' if base_chain else trim_filter), normalized_offset_ms


def get_relative_audio_offset_ms(container_metadata, audio_stream_index):
    """Return an audio stream offset relative to the video stream."""
    if not isinstance(container_metadata, dict):
        return 0

    try:
        audio_idx = int(audio_stream_index)
    except (TypeError, ValueError):
        return 0

    audio_start_times = container_metadata.get('audio_start_times') or []
    if audio_idx < 0 or audio_idx >= len(audio_start_times):
        return 0

    try:
        audio_start = float(audio_start_times[audio_idx] or 0.0)
    except (TypeError, ValueError):
        audio_start = 0.0

    try:
        video_start = float(container_metadata.get('video_start_time') or 0.0)
    except (TypeError, ValueError):
        video_start = 0.0

    return int(round((audio_start - video_start) * 1000.0))

def find_vdub2_path():
    """Find VirtualDub2 executable in PATH or common locations.
    
    Returns:
        Path: Path to vdub2.exe/vdub64.exe or None.
    """
    if VDUB2_PATH:
        vdub_path = VDUB2_PATH if isinstance(VDUB2_PATH, Path) else Path(VDUB2_PATH)
        if vdub_path.exists():
            return vdub_path
    
    detected = find_virtualdub()
    if detected:
        detected_path = Path(detected)
        if detected_path.exists():
            return detected_path
    
    # 1. Direkt path-ok (aktuális mappa)
    possible_paths = [
        Path("vdub2.exe"),
        Path("vdub64.exe"),
    ]
    for path in possible_paths:
        if path.exists():
            return path
    
    # 2. Preferált helyek dinamikus meghajtókkal
    try:
        from .core_paths_tools_logging import _get_preferred_search_paths
        preferred_paths = _get_preferred_search_paths('virtualdub')
        for base_path in preferred_paths:
            for exe in ['vdub2.exe', 'vdub64.exe']:
                path = base_path / exe
                if path.exists():
                    return path
    except Exception:
        pass
    
    vdub2_path = shutil.which("vdub2.exe") or shutil.which("vdub64.exe")
    if vdub2_path:
        return Path(vdub2_path)
    
    return None

def extract_frames_with_vdub2(video_path, output_dir, num_frames=5, stop_event=None, use_percentage_positions=False):
    """Export sample frames from video using VirtualDub2.
    
    Args:
        video_path: Path to the video file.
        output_dir: Directory to save frames.
        num_frames: Number of frames to export (if use_percentage_positions is False).
        stop_event: Event to stop the process.
        use_percentage_positions: If True, exports frames at 30%, 50%, 70%, and end.
        
    Returns:
        list: List of paths to extracted frames.
    """
    vdub2_path = find_vdub2_path()
    if not vdub2_path:
        print("  [WARN] VirtualDub2 not found")
        return []
    
    duration, fps = get_video_info(video_path)
    if not duration or duration < 1:
        print("  [WARN] Failed to determine video duration")
        return []
    
    # Use actual FPS if available, otherwise default to 30
    if not fps or fps <= 0:
        fps = 30.0
    
    if use_percentage_positions:
        # 30%, 50%, 70% and end (4 frames)
        frame_times = [
            duration * 0.30,  # 30%
            duration * 0.50,  # 50%
            duration * 0.70,  # 70%
            duration - 0.1    # End (0.1s before end to ensure frame exists)
        ]
        print(f"  📹 VirtualDub2 frame export: 4 frames (30%, 50%, 70%, end)")
    else:
        # Old method: random positions
        frame_times = sorted([random.uniform(0, duration) for _ in range(num_frames)])
        print(f"  📹 VirtualDub2 frame export: {num_frames} frames")
    
    duration_str = format_localized_number(duration, decimals=1)
    print(f"  Video duration: {duration_str}s")
    print(f"  VirtualDub2: {vdub2_path}")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    source_frame_count = None
    encoded_frame_count = None
    frame_count_warning = False
    last_frame_ok = True

    if stop_event.is_set():
        raise EncodingStopped()

    extracted_frames = []
    
    for i, t in enumerate(frame_times, start=1):
        if stop_event.is_set():
            raise EncodingStopped()
        frame_filename = "frame0000.png"
        frame_path = output_dir / frame_filename
        
        time_str = format_localized_number(t, decimals=2)
        print(f"    Frame #{i} @ {time_str}s...")
        
        # Create VirtualDub2 script
        script_path = output_dir / f"script_{i}.vdscript"
        frame_index = int(t * fps)  # Use actual FPS
        
        # Paths with double backslash (Windows format)
        video_path_str = str(video_path.absolute()).replace("\\", "\\\\")
        output_dir_str = str(output_dir.absolute()).replace("\\", "\\\\")
        
        script_content = f"""VirtualDub.Open("{video_path_str}");
VirtualDub.subset.Clear();
VirtualDub.subset.AddRange({frame_index}, 1);
VirtualDub.SaveImageSequence("{output_dir_str}\\\\frame", ".png", 4, 3);
VirtualDub.Close();
"""
        
        try:
            if stop_event.is_set():
                raise EncodingStopped()
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(script_content)
        except (OSError, PermissionError, IOError) as e:
            print(f"      [ERROR] Script írási hiba: {e}")
            continue
        
        # Print script content to console
        print(f"\n{'='*80}")
        print("VDUB2 SCRIPT CONTENT:")
        print(f"{'='*80}")
        print(script_content)
        print(f"{'='*80}\n")
        
        cmd = [
            os.fspath(vdub2_path),
            "/s", os.fspath(script_path),
            "/x"
        ]
        
        print(f"\n{'='*80}")
        print("VDUB2 COMMAND:")
        print(f"{'='*80}")
        print(format_cmd_for_windows(cmd))
        print(f"{'='*80}\n")
        
        try:
            if stop_event.is_set():
                raise EncodingStopped()
            
            full_output = []
            # Use context manager for process management
            with managed_subprocess(cmd, cwd=output_dir, stop_event=stop_event, timeout=600) as process:
                try:
                    for line in process.stdout:
                        if stop_event and stop_event.is_set():
                            raise EncodingStopped()
                        print(line.rstrip())
                        full_output.append(line)
                except EncodingStopped:
                    raise
                except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
                    print(f"VirtualDub2 output reading error: {e}")
                
                # Wait for process completion
                try:
                    process.wait(timeout=600)
                except subprocess.TimeoutExpired:
                    # Context manager finally block handles cleanup
                    raise
            
            full_output_text = ''.join(full_output)
            
            if stop_event.is_set():
                raise EncodingStopped()
            
            print(f"\n{'='*80}")
            print(f"VDUB2 FINISHED - Return code: {process.returncode}")
            print(f"{'='*80}\n")
            
            # Detect "unexpected end of stream"
            if "unexpected end of stream" in full_output_text.lower():
                print(f"      [ERROR] 'unexpected end of stream' - is video playable*")
                return None  # Special signal for re-encoding
            
            # Find and rename frame
            if frame_path.exists():
                # Rename to unique name
                final_frame_path = output_dir / f"frame_{i:04d}.png"
                frame_path.rename(final_frame_path)
                extracted_frames.append(final_frame_path)
                print(f"      [OK] Frame saved: {final_frame_path.name}")
            else:
                print(f"      [ERROR] Frame not created")
                
        except EncodingStopped:
            raise
        except subprocess.TimeoutExpired:
            print(f"      [ERROR] Timeout")
        except (OSError, subprocess.SubprocessError, ValueError, AttributeError) as e:
            print(f"      [ERROR] Error: {e}")
        except Exception as e:
            # Log unexpected errors but don't block
            print(f"      [ERROR] Unexpected error: {type(e).__name__}: {e}")
        finally:
            # Delete script
            try:
                script_path.unlink()
            except (OSError, PermissionError, FileNotFoundError):
                pass
    
    print(f"  Total {len(extracted_frames)}/{num_frames} frames exported")
    
    debug_pause(
        f"VirtualDub2 frame export done: {len(extracted_frames)} frames",
        "Frame content check (black/empty detection)",
        f"Frames: {output_dir}"
    )
    
    return extracted_frames

def is_frame_black_or_empty(frame_path, max_mean_brightness=MAX_MEAN_BRIGHTNESS, min_std_dev=MIN_STD_DEV):
    try:
        from PIL import ImageStat
        with Image.open(frame_path) as img:
            
            file_size = frame_path.stat().st_size
            # MINOR FIX #16: Use constant
            if file_size < MIN_FRAME_FILE_SIZE:
                print(f"      [WARN] Too small: {file_size} bytes")
                return True
            
            # Convert to grayscale
            img_gray = img.convert('L')
            
            # Calculate statistics using PIL
            stat = ImageStat.Stat(img_gray)
            mean_brightness = stat.mean[0]
            std_dev = stat.stddev[0]
            
            brightness_str = format_localized_number(mean_brightness, decimals=1)
            stddev_str = format_localized_number(std_dev, decimals=1)
            print(f"      Brightness: {brightness_str}, StdDev: {stddev_str}")
            
            is_black = mean_brightness < max_mean_brightness and std_dev < min_std_dev
            
            if is_black:
                print(f"      [ERROR] Black/Empty")
                return True
            else:
                print(f"      [OK] Real content")
                return False
    except Exception as e:
        print(f"      [ERROR] Error: {e}")
        return True

def _write_vdub_script(script_path, video_path, start_frame, frame_count, output_prefix):
    video_path_str = str(video_path.absolute()).replace("\\", "\\\\")
    output_prefix_str = str(output_prefix).replace("\\", "\\\\")
    script_content = (
        f'VirtualDub.Open("{video_path_str}");\n'
        "VirtualDub.subset.Clear();\n"
        f"VirtualDub.subset.AddRange({start_frame}, {frame_count});\n"
        f'VirtualDub.SaveImageSequence("{output_prefix_str}", ".png", 4, 3);\n'
        "VirtualDub.Close();\n"
    )
    script_path.write_text(script_content, encoding='utf-8')

def export_specific_frame_with_vdub2(video_path, frame_index, output_path, stop_event=None):
    """Export a specific frame using VirtualDub2."""
    vdub2_path = find_vdub2_path()
    if not vdub2_path:
        print("    [ERROR] VirtualDub2 not found for last frame export")
        return False

    if frame_index is None or frame_index < 0:
        print("    [ERROR] Invalid frame index for VirtualDub2 export")
        return False

    temp_dir = Path(tempfile.mkdtemp())
    try:
        if stop_event is not None and stop_event.is_set():
            raise EncodingStopped()

        script_path = temp_dir / "single_frame.vdscript"
        output_prefix = temp_dir / "frame_last"
        _write_vdub_script(script_path, video_path, frame_index, 1, output_prefix)

        # Check script content (debug)
        try:
            script_content = script_path.read_text(encoding='utf-8')
            print(f"    VirtualDub2 last frame export (frame {frame_index})...")
            print(f"      Script: {script_path}")
            print(f"      Script content:\n        " + "\n        ".join(script_content.strip().splitlines()))
        except Exception as e:
            print(f"    [WARN] Script reading error: {e}")

        cmd = [
            os.fspath(vdub2_path),
            "/s", os.fspath(script_path),
            "/x"
        ]
        print(f"      Command: {format_cmd_for_windows(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            startupinfo=get_startup_info(),
            timeout=600
        )
        if result.stdout:
            print("      VDUB2 STDOUT:")
            print("        " + "\n        ".join(result.stdout.strip().splitlines()))
        if result.stderr:
            print("      VDUB2 STDERR:")
            print("        " + "\n        ".join(result.stderr.strip().splitlines()))
        if result.returncode != 0:
            print(f"    [ERROR] VirtualDub2 error ({result.returncode}): {result.stderr.strip() or result.stdout.strip()}")
            return False

        # VirtualDub2 usually creates "frame_last0000.png" (without underscore)
        generated_frame = temp_dir / "frame_last0000.png"
        if not generated_frame.exists():
            # Fallback: try "frame_last_0000.png" (with underscore)
            generated_frame = temp_dir / "frame_last_0000.png"
            if not generated_frame.exists():
                # Search for any PNG file in temp directory
                png_files = list(temp_dir.glob("*.png"))
                if png_files:
                    # If PNG found, use it
                    generated_frame = png_files[0]
                    print(f"      [WARN] VirtualDub2 created file with different name: {generated_frame.name}")
                else:
                    # If no PNG found, list directory content
                    try:
                        contents = os.listdir(temp_dir)
                        print(f"      [WARN] VirtualDub2 output directory content: {contents}")
                    except OSError:
                        pass
                    print("    [ERROR] VirtualDub2 did not create last frame image")
                    return False

        shutil.copy2(generated_frame, output_path)
        return True
    except EncodingStopped:
        raise
    except Exception as e:
        print(f"    [ERROR] VirtualDub2 last frame export error: {e}")
        return False
    finally:
        if not DEBUG_MODE:
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass
        else:
            print(f"  [STOP] DEBUG: Last frame temp KEPT: {temp_dir}")

def export_last_frame_with_vdub2(video_path, output_path, frame_count_hint=None, stop_event=None):
    """Attempt to extract last frame using VirtualDub2."""
    frame_count = frame_count_hint
    if frame_count is None:
        try:
            frame_count = get_frame_count(video_path)
        except Exception:
            frame_count = None

    if not frame_count or frame_count <= 0:
        print("    [ERROR] VirtualDub2: failed to determine frame count")
        return False

    last_frame_index = max(frame_count - 1, 0)
    return export_specific_frame_with_vdub2(video_path, last_frame_index, output_path, stop_event=stop_event)

def check_source_video_with_vdub2(video_path, stop_event=None):
    """Probe source video with VirtualDub2 to check for corruption.
    
    Args:
        video_path: Path to the source video.
        stop_event: Event to stop the process.
        
    Returns:
        bool: True if video is valid (VDub2 can open and extract a frame), False otherwise.
    """
    print(f"\n{'='*60}")
    print(f"Source video probe (VirtualDub2): {video_path.name}")
    print(f"{'='*60}")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    if stop_event.is_set():
        raise EncodingStopped()
        
    vdub2_path = find_vdub2_path()
    if not vdub2_path:
        print("  [WARN] VirtualDub2 not found - skipping probe")
        return True # Assume valid if tool not found (or fail* User said "vdub2 probe-olva", implies requirement)
        # But if VDub2 is missing, we probably shouldn't block everything unless it's critical.
        # However, for NVENC validation it IS used.
        # Let's return True but log warning.
    
    temp_dir = Path(tempfile.mkdtemp())
    try:
        # Try to extract just 1 frame from the middle (50%)
        # This proves VDub2 can open the file and seek to a frame.
        print(f"   Probing: 1 frame test (50%)")
        
        # We use extract_frames_with_vdub2 but with specific parameters
        # We manually construct the call to avoid printing too much if we want, 
        # but extract_frames_with_vdub2 is already verbose which is fine for logs.
        
        # We'll use a simplified version of logic here to avoid overhead of 4 frames
        duration, fps = get_video_info(video_path)
        if not duration or duration < 1:
            print("  [WARN] Failed to determine video duration")
            # If we can't get duration, VDub2 might also fail.
            # But let's try VDub2 anyway if possible, or fail here*
            # Usually if ffprobe fails, it's bad.
            # But let's proceed to VDub2 check.
            duration = 10 # Dummy
            
        mid_point = duration * 0.5
        frame_index = int(mid_point * (fps if fps else 30))
        
        script_path = temp_dir / "probe.vdscript"
        output_prefix = temp_dir / "probe_frame"
        
        _write_vdub_script(script_path, video_path, frame_index, 1, output_prefix)
        
        cmd = [
            os.fspath(vdub2_path),
            "/s", os.fspath(script_path),
            "/x"
        ]
        
        print(f"   Command: {format_cmd_for_windows(cmd)}")
        
        with managed_subprocess(cmd, cwd=temp_dir, stop_event=stop_event, timeout=600) as process:
            full_output = []
            try:
                for line in process.stdout:
                    if stop_event and stop_event.is_set():
                        raise EncodingStopped()
                    # print(line.rstrip()) # Keep it quiet unless error* Or log it*
                    # User wants to see it in logs probably.
                    full_output.append(line)
            except Exception:
                pass
            
            process.wait(timeout=600)
            
            full_output_text = ''.join(full_output)
            
            if process.returncode != 0:
                print(f"  [ERROR] VirtualDub2 returned error code: {process.returncode}")
                return False
                
            if "unexpected end of stream" in full_output_text.lower():
                print(f"  [ERROR] 'unexpected end of stream' detected")
                return False
                
            # Check if frame was created
            # VDub2 creates probe_frame0000.png
            expected_frame = temp_dir / "probe_frame0000.png"
            if not expected_frame.exists():
                 # Try with underscore
                expected_frame = temp_dir / "probe_frame_0000.png"
            
            if expected_frame.exists():
                print(f"  [OK] Probe successful: Frame extracted")
                return True
            else:
                print(f"  [ERROR] Probe failed: Frame not created")
                return False

    except EncodingStopped:
        raise
    except Exception as e:
        print(f"  [ERROR] Probe error: {e}")
        return False
    finally:
        if not DEBUG_MODE:
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass
        else:
            print(f"  [STOP] DEBUG: Probe temp KEPT: {temp_dir}")

def validate_encoded_video_vlc(video_path, encoder='av1_nvenc', stop_event=None, source_path=None):
    """Validate encoded video using various checks (VLC-like validation).
    
    Checks for:
    - File existence and size
    - Duration match with source (if provided)
    - Frame count match (if source provided)
    - Black/empty frames in sample export
    - Last frame readability
    
    Args:
        video_path: Path to the encoded video.
        encoder: Encoder name used.
        stop_event: Event to stop validation.
        source_path: Path to source video (optional, for comparison).
        
    Returns:
        bool: True if validation passes, False otherwise.
    """
    print(f"\n{'='*60}")
    print(f"Video validation (VirtualDub2 frame): {video_path.name}")
    print(f"Encoder: {encoder}")
    print(f"{'='*60}")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    if stop_event.is_set():
        raise EncodingStopped()
    
    # CRITICAL FIX: Initialize variables
    source_frame_count = None
    encoded_frame_count = None
    is_valid = True
    success_rate = 0.0
    min_success_rate = 0.4
    frame_count_warning = False
    
    if not video_path.exists():
        print(f"[ERROR] File not found: {video_path}")
        return False

    file_size = video_path.stat().st_size
    file_size_mb = file_size / (1024**2)
    print(f"\n1. File size: {format_localized_number(file_size_mb, decimals=2)} MB")
    
    # MINOR FIX #16: Use constant
    if file_size < MIN_FILE_SIZE_BYTES:
        print("  [ERROR] File too small!")
        return False
    
    print("\n2. VirtualDub2 frame export and content check...")
    
    temp_dir = Path(tempfile.mkdtemp())
    try:
        # 30%, 50%, 70% and end (4 frames total)
        print(f"   {encoder}: 4 frame test (30%, 50%, 70%, end)")
        
        extracted_frames = extract_frames_with_vdub2(video_path, temp_dir, num_frames=4, stop_event=stop_event, use_percentage_positions=True)
        
        # Detect "unexpected end of stream" - special signal (None)
        if extracted_frames is None:
            print("  [ERROR] VirtualDub2: 'unexpected end of stream' - video needs re-encoding!")
            return None  # Special value: re-encoding needed
        
        if not extracted_frames:
            print("  [ERROR] Failed to export frames!")
            return False
        
        if stop_event.is_set():
            raise EncodingStopped()
        
        print(f"\n3. Frame content check ({len(extracted_frames)} pcs)...")
        valid_frames = 0
        black_frames = 0
        
        for frame_path in extracted_frames:
            if stop_event.is_set():
                raise EncodingStopped()
            print(f"    Checking: {frame_path.name}")
            is_black = is_frame_black_or_empty(frame_path)
            
            if not is_black:
                valid_frames += 1
            else:
                black_frames += 1
        
        success_rate = valid_frames / len(extracted_frames)
        min_success_rate = 0.33 if encoder == 'svt-av1' else 0.4
        
        is_valid = success_rate >= min_success_rate

        print(f"\n4. Frame statistics (source/dest)...")
        if source_path:
            try:
                source_path_obj = Path(source_path)
            except (OSError, ValueError, TypeError):
                source_path_obj = None
            if source_path_obj and source_path_obj.exists():
                try:
                    source_frame_count = get_frame_count(source_path_obj)
                    if source_frame_count is not None:
                        print(f"    Source frames: {source_frame_count}")
                    else:
                        print("    Source frames: unknown")
                except Exception as e:
                    print(f"    [ERROR] Source frame query error: {e}")
            else:
                print("    Source frames: source file not available")
        else:
            print("    Source frames: not specified")

        try:
            encoded_frame_count = get_frame_count(video_path)
            if encoded_frame_count is not None:
                print(f"    Dest frames: {encoded_frame_count}")
            else:
                print("    Dest frames: unknown")
        except Exception as e:
            print(f"    [ERROR] Dest frame query error: {e}")
            encoded_frame_count = None

        if source_frame_count is not None and encoded_frame_count is not None:
            allowed_diff = max(FRAME_MISMATCH_MIN_DIFF, int(source_frame_count * FRAME_MISMATCH_RATIO))
            diff = encoded_frame_count - source_frame_count
            if frames_significantly_different(source_frame_count, encoded_frame_count):
                frame_count_warning = True
                print(f"    [WARN] Significant frame count mismatch (diff: {diff:+d}, tolerance: +/-{allowed_diff})")
            else:
                print(f"    [OK] Frame mismatch within tolerance (diff: {diff:+d})")

        print(f"\n5. Last frame check...")
        last_frame_ok = False
        last_frame_warning = False
        last_frame_path = temp_dir / "frame_last.png"

        if last_frame_path.exists():
            try:
                last_frame_path.unlink()
            except OSError:
                pass

        vdub_success = export_last_frame_with_vdub2(
            video_path,
            last_frame_path,
            frame_count_hint=encoded_frame_count,
            stop_event=stop_event
        )

        if vdub_success:
            try:
                with Image.open(last_frame_path) as last_img:
                    last_img.verify()
                print(f"    [OK] Last frame readable with VirtualDub2 ({last_frame_path.name})")
                last_frame_ok = True
            except Exception as exc:
                print(f"    [ERROR] VirtualDub2 exported last frame is corrupted: {exc}")
                last_frame_ok = False
        else:
            print("    [WARN] VirtualDub2 could not extract last frame – FFmpeg fallback")

        def run_last_frame_attempt(cmd, description):
            print(description)
            print(f"      Command: {format_cmd_for_windows(cmd)}")
            try:
                if stop_event is not None and stop_event.is_set():
                    raise EncodingStopped()
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, startupinfo=get_startup_info())
                if result.stdout:
                    print("      FFmpeg STDOUT:")
                    print("        " + "\n        ".join(result.stdout.strip().splitlines()))
                if result.stderr:
                    print("      FFmpeg STDERR:")
                    print("        " + "\n        ".join(result.stderr.strip().splitlines()))
                if result.returncode != 0:
                    err_text = result.stderr.strip() or result.stdout.strip() or str(result.returncode)
                    print(f"    [ERROR] FFmpeg error: {err_text}")
                    return "error"
                if not last_frame_path.exists():
                    print("    [WARN] Last frame file not created – check skipped")
                    try:
                        parent_dir = Path(last_frame_path).parent
                        if parent_dir.exists():
                            contents = os.listdir(parent_dir)
                            print(f"      [WARN] Dest directory content: {contents}")
                    except OSError:
                        pass
                    return "missing"
                try:
                    with Image.open(last_frame_path) as last_img:
                        last_img.verify()
                    print(f"    [OK] Last frame readable ({last_frame_path.name})")
                    return "success"
                except Exception as exc:
                    print(f"    [ERROR] Last frame reading error: {exc}")
                    return "error"
            except EncodingStopped:
                raise
            except Exception as exc:
                print(f"    [ERROR] Last frame export failed: {exc}")
                return "error"

        if not last_frame_ok:
            primary_cmd = [
                FFMPEG_PATH, '-hide_banner', '-loglevel', 'error', '-y',
                '-sseof', '-0.1',
                '-i', os.fspath(video_path),
                '-frames:v', '1',
                os.fspath(last_frame_path)
            ]
            attempt_result = run_last_frame_attempt(primary_cmd, "    Starting FFmpeg last frame export...")
            if attempt_result == "success":
                last_frame_ok = True
            else:
                if attempt_result == "missing":
                    last_frame_warning = True
                fallback_cmd = [
                    FFMPEG_PATH, '-hide_banner', '-loglevel', 'error', '-y',
                    '-i', os.fspath(video_path),
                    '-vf'
                ]
                encoded_count = encoded_frame_count
                if encoded_count and encoded_count > 0:
                    last_index = max(encoded_count - 1, 0)
                    fallback_cmd.append(f"select=eq(n\\,{last_index})")
                else:
                    fallback_cmd.extend(['-sseof', '-0.5'])
                fallback_cmd.extend([
                    '-vsync', '0',
                    '-frames:v', '1',
                    os.fspath(last_frame_path)
                ])
                fallback_result = run_last_frame_attempt(fallback_cmd, "    FFmpeg last frame export (full scan)...")
                if fallback_result == "success":
                    last_frame_ok = True
                    last_frame_warning = False
                elif fallback_result == "missing":
                    last_frame_warning = True
                else:
                    last_frame_ok = False

        if not last_frame_ok and not last_frame_warning:
            is_valid = False

        print(f"\n{'='*60}")
        print(f"VALIDATION RESULT:")
        file_size_mb = file_size / (1024**2)
        print(f"  File size: OK ({format_localized_number(file_size_mb, decimals=2)} MB)")
        print(f"  VirtualDub2 frame export: {len(extracted_frames)}/4 OK")
        success_percent = format_localized_number(success_rate * 100, decimals=1)
        print(f"  Real content: {valid_frames}/{len(extracted_frames)} ({success_percent}%)")
        print(f"  Black/Empty: {black_frames}")
        print(f"  Requirement: ≥{min_success_rate*100:.0f}% real content")
        if source_frame_count is not None or encoded_frame_count is not None:
            src_display = str(source_frame_count) if source_frame_count is not None else "unknown"
            dst_display = str(encoded_frame_count) if encoded_frame_count is not None else "unknown"
            print(f"  Frames (source/dest): {src_display} / {dst_display}")
            if source_frame_count is not None and encoded_frame_count is not None:
                diff = encoded_frame_count - source_frame_count
                status_text = "[WARN] mismatch" if frame_count_warning else "OK"
                print(f"  Frame difference: {diff:+d} ({status_text})")
        if last_frame_warning:
            print(f"  Last frame: [WARN] Could not verify (file not created)")
        else:
            print(f"  Last frame: {'OK' if last_frame_ok else '[ERROR] ERROR'}")
        print(f"  Final result: {'[OK] VALID' if is_valid else '[ERROR] INVALID'}")
        print(f"{'='*60}")
        
        debug_pause(
            f"Validation done: {'VALID' if is_valid else 'INVALID'} ({valid_frames}/{len(extracted_frames)} good)",
            "Delete temp directory" if not DEBUG_MODE else "Temp KEPT (debug)",
            f"Temp: {temp_dir}, Video: {video_path}"
        )
        
        return is_valid
    except EncodingStopped:
        raise
    finally:
        if not DEBUG_MODE:
            try:
                shutil.rmtree(temp_dir)
            except (OSError, PermissionError, FileNotFoundError):
                pass
        else:
            print(f"  [STOP] DEBUG: Temp KEPT: {temp_dir}")

def find_video_files(root_dir, include_av1=False):
    """Recursively find video files in a directory.
    
    Args:
        root_dir: Path to the root directory.
        include_av1: If False, skips files ending with .av1.
        
    Returns:
        list: List of found video files as Path objects.
    """
    video_files = []
    root_path = Path(root_dir)
    for file_path in root_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
            # Kihagyjuk a .ab-av1-* almappákban lévő fájlokat (ab-av1 temp fájlok)
            path_parts = file_path.parts
            if any('.ab-av1-' in part for part in path_parts):
                continue
            
            if include_av1:
                # Ha include_av1=True, akkor minden videó fájlt hozzáadunk
                video_files.append(file_path)
            else:
                # Alapértelmezett: kihagyjuk az .av1 fájlokat
                if not file_path.stem.endswith('.av1'):
                    video_files.append(file_path)
    return video_files

def get_output_filename(input_path, source_root, dest_root):
    """Determine the output file path.
    
    Args:
        input_path: Path to the input video (Path).
        source_root: Source root directory (Path or None).
        dest_root: Destination root directory (Path or None).
        
    Returns:
        Path: Output file path (with .av1.mkv extension).
    """
    if dest_root is None:
        return input_path.parent / f"{input_path.stem}.av1.mkv"
    else:
        source_path = Path(source_root)
        dest_path = Path(dest_root)
        relative_path = input_path.relative_to(source_path)
        new_filename = f"{input_path.stem}.av1.mkv"
        output_path = dest_path / relative_path.parent / new_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path


def get_copy_filename(input_path, source_root, dest_root):
    """Generate output path with ORIGINAL extension (for copy fallback).
    
    Used when encoding fails and video must be copied unchanged.
    Preserves original filename and extension (e.g. video.mp4 stays video.mp4).
    
    Args:
        input_path: Path to input video (Path).
        source_root: Source root directory (Path or None).
        dest_root: Destination root directory (Path or None).
        
    Returns:
        Path: Output path with original extension.
    """
    if dest_root is None:
        # Same directory: keep original path
        return input_path
    else:
        source_path = Path(source_root)
        dest_path = Path(dest_root)
        relative_path = input_path.relative_to(source_path)
        # Preserve original filename AND extension
        output_path = dest_path / relative_path
        parent = output_path.parent
        if not parent.exists():
            os.makedirs(parent, exist_ok=True)
        return output_path


def is_misnamed_copy(source_path, dest_av1_path):
    """Check if .av1.mkv file is actually unchanged copy of source.
    
    Detects cases where a video was copied (not encoded) but incorrectly
    saved with .av1.mkv extension. Uses file size comparison as the
    primary indicator.
    
    Args:
        source_path: Original source video path (Path).
        dest_av1_path: Destination .av1.mkv file path (Path).
        
    Returns:
        bool: True if dest file is unchanged copy (same size as source).
    """
    # Check both files exist
    if not source_path.exists() or not dest_av1_path.exists():
        return False
    
    # Check extension is .av1.mkv
    if dest_av1_path.suffix.lower() != '.mkv':
        return False
    if '.av1' not in dest_av1_path.stem:
        return False
    
    # Compare file sizes (most reliable indicator of unchanged copy)
    try:
        source_size = source_path.stat().st_size
        dest_size = dest_av1_path.stat().st_size
        
        # Same size = likely unchanged copy  
        # Encoded files are typically 50-75% of original size
        return source_size == dest_size
    except (OSError, PermissionError):
        return False


def rename_misnamed_copy_file(dest_av1_path, source_path, logger=None):
    """Rename misnamed .av1.mkv copy to original extension.
    
    Takes a .av1.mkv file that is actually an unchanged copy and renames
    it to match the original source file's extension.
    
    Args:
        dest_av1_path: Current .av1.mkv file path (Path).
        source_path: Original source file for extension reference (Path).
        logger: Optional logger for console output (ConsoleLogger).
        
    Returns:
        Path: New path with original extension, or None on error.
    """
    try:
        # Generate new name with original extension
        original_ext = source_path.suffix
        # Remove .av1 from stem
        new_stem = dest_av1_path.stem
        if new_stem.endswith('.av1'):
            new_stem = new_stem[:-4]
        new_name = new_stem + original_ext
        new_path = dest_av1_path.with_name(new_name)
        
        # Check if target already exists
        if new_path.exists() and new_path != dest_av1_path:
            if logger:
                with console_redirect(logger):
                    print(f"[WARN] Cél fájl már létezik: {new_path.name}")
            return None
        
        # Atomic rename
        dest_av1_path.rename(new_path)
        
        if logger:
            with console_redirect(logger):
                print(f"[OK] Átnevezve: {dest_av1_path.name} -> {new_path.name}")
        
        return new_path
        
    except (OSError, PermissionError) as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Átnevezés hiba: {e}")
        return None


def verify_and_copy_subtitles(source_path, dest_path, logger=None):
    """Verify and copy missing subtitle files from source to dest location.
    
    NOTE: This function should ONLY be called during video loading, NOT after encoding!
    After encoding, valid subtitles are embedded in the video, and only invalid subtitles
    should be copied using _copy_invalid_subtitles().
    
    Ensures all subtitle files associated with the source video are also
    present at the destination location. Copies any missing subtitles.
    
    Args:
        source_path: Source video path (Path).
        dest_path: Destination video path (Path).
        logger: Optional logger for console output (ConsoleLogger).
        
    Returns:
        int: Number of subtitles copied.
    """
    # Find source subtitles
    source_subs = find_subtitle_files(source_path)
    if not source_subs:
        return 0
    
    # IMPORTANT: Only copy invalid subtitles (valid ones should be embedded in video)
    # This function is called during video loading when a file is renamed/copied,
    # but after encoding, only invalid subtitles should be copied.
    from .core_subtitles_and_metadata import split_valid_invalid_subtitles, is_valid_subtitle_file
    
    valid_subs, invalid_subs = split_valid_invalid_subtitles(source_subs)
    
    # Only copy invalid subtitles (valid ones are embedded in video after encoding)
    copied_count = 0
    
    for sub_path, lang_code, reason in invalid_subs:
        # Generate destination subtitle name
        if lang_code:
            # Insert language code before extension
            dest_sub_name = f"{dest_path.stem}.{lang_code}{sub_path.suffix}"
        else:
            dest_sub_name = dest_path.stem + sub_path.suffix
        
        dest_sub_path = dest_path.parent / dest_sub_name
        
        # Copy if doesn't exist
        if not dest_sub_path.exists():
            try:
                shutil.copy2(sub_path, dest_sub_path)
                copied_count += 1
                if logger:
                    with console_redirect(logger):
                        print(f"[WARN] Invalid felirat másolva (nem beágyazható): {dest_sub_path.name} ({reason})")
            except (OSError, PermissionError) as e:
                if logger:
                    with console_redirect(logger):
                        print(f"[WARN] Felirat másolás hiba: {e}")
    
    return copied_count


def apply_faststart(video_path, logger=None, stop_event=None):
    """Apply faststart optimization to a video file.
    
    Remuxes the video file with -movflags +faststart to move the moov atom
    to the beginning of the file for faster streaming. Preserves all streams
    (video, audio, subtitles) and metadata.
    
    Args:
        video_path: Path to the video file (Path object).
        logger: Optional logger for console output.
        stop_event: Event to stop the process.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    if stop_event is None:
        from .core_paths_tools_logging import STOP_EVENT
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        from .core_paths_tools_logging import EncodingStopped
        raise EncodingStopped()
    
    video_path = Path(video_path)
    if not video_path.exists():
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Faststart: File not found: {video_path}")
        return False
    
    # Create temp file path with .faststart suffix before extension
    source_suffix = video_path.suffix or '.mkv'
    safe_stem = video_path.stem or video_path.name
    temp_output = video_path.with_name(f"{safe_stem}.faststart{source_suffix}")
    backup_path = video_path.with_name(video_path.name + ".faststart.bak")
    
    # Clean up any leftover temp files
    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass
    
    # Build FFmpeg command for remux (faststart optimization no longer used)
    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(video_path),
        '-map', '0',              # Copy all streams
        '-c', 'copy',             # Stream copy
        '-map_chapters', '0',     # Preserve chapters
        '-map_metadata', '0',     # Preserve all metadata
        os.fspath(temp_output)
    ]
    
    if logger:
        with console_redirect(logger):
            print(f"\n{'='*80}")
            print(f"[RUN] FASTSTART OPTIMIZATION (FFmpeg): {video_path.name}")
            print(f"{'='*80}")
            print(f"Command: {format_cmd_for_windows(cmd)}")
            print(f"{'='*80}\n")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            universal_newlines=True,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    from .core_paths_tools_logging import EncodingStopped
                    raise EncodingStopped()
                
                # Log output if available
                if logger:
                     with console_redirect(logger):
                         # Log FFmpeg output lines (errors/warnings)
                         if "Error" in line or "Warning" in line:
                             print(line.strip())
                             
        except EncodingStopped:
            raise
        except Exception as e:
            if logger:
                with console_redirect(logger):
                    print(f"Error reading process output: {e}")
            
        try:
            process.wait(timeout=600)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            if logger:
                with console_redirect(logger):
                    print(f"[ERROR] Faststart optimization timed out")
        
        success = process.returncode == 0
        if not success:
            if logger:
                with console_redirect(logger):
                    print(f"Optimization failed with return code {process.returncode}")
        
    except FileNotFoundError:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] FFmpeg not found at {FFMPEG_PATH}")
        return False
        
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if 'process' in locals() and process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)

    if not success:
        if temp_output.exists() and not DEBUG_MODE:
            try:
                temp_output.unlink()
            except OSError:
                pass
        elif temp_output.exists() and DEBUG_MODE:
            print(f"  [STOP] DEBUG: Faststart temp KEPT (not success): {temp_output}")
        return False

    # Replace original file logic remains the same...
    if stop_event.is_set():
        if temp_output.exists() and not DEBUG_MODE:
            try: # Clean up temp file on stop
                temp_output.unlink()
            except OSError:
                pass
        elif temp_output.exists() and DEBUG_MODE:
            print(f"  [STOP] DEBUG: Faststart temp KEPT (stopped): {temp_output}")
        return False

    original_replaced = False
    try:
        # Windows-specific: sometimes file locks prevent immediate replacement
        # Try-catch block with backups
        if backup_path.exists():
            backup_path.unlink()
            
        os.rename(video_path, backup_path)
        original_replaced = True
        os.rename(temp_output, video_path)
        
        # Validation succesful -> delete backup
        if not DEBUG_MODE:
            backup_path.unlink()
        else:
            if logger:
                with console_redirect(logger):
                    print(f"  [STOP] DEBUG: Backup KEPT: {backup_path}")
        
        if logger:
            with console_redirect(logger):
                print(f"[OK] Optimization completed successfully.")
        
        debug_pause(
            f"Faststart optimization done (FFmpeg)",
            "Next video in queue",
            f"File: {video_path.name}"
        )
                
        return True

    except (OSError, PermissionError) as e:
        if logger:
            with console_redirect(logger):
                 print(f"[ERROR] File replacement error: {e}")
        
        # Rollback
        if original_replaced and backup_path.exists():
            try:
                if video_path.exists():
                    video_path.unlink()
                os.rename(backup_path, video_path)
            except OSError:
                pass
        
        if temp_output.exists() and not DEBUG_MODE:
            try:
                temp_output.unlink()
            except OSError:
                pass
        elif temp_output.exists() and DEBUG_MODE:
            print(f"  [STOP] DEBUG: Faststart temp KEPT (error): {temp_output}")
                
        return False



def get_video_streams_for_editor(video_path, quick=False):
    """Retrieve detailed stream information for the track editor.

    Args:
        video_path: Path to the video file.
        quick: If True, skip slow fallbacks (packet-size, pipe extraction). 
               Size/bitrate will be None for streams without direct metadata.

    Returns:
        dict: {
            'video': {'index': int, 'codec': str, 'width': int, 'height': int, 'start_time_ms': int},
            'audio': [{'index': int, 'codec': str, 'lang': str, 'title': str, 'default': bool, 'channels': int, 'start_time_ms': int}, ...],
            'subtitle': [{'index': int, 'codec': str, 'lang': str, 'title': str, 'default': bool, 'forced': bool, 'start_time_ms': int}, ...]
        }
    """
    video_path = Path(video_path)
    if not video_path.exists():
        return None

    format_duration = None
    try:
        dur_cmd = [
            FFPROBE_PATH, '-v', 'quiet',
            '-show_entries', 'format=duration',
            '-of', 'csv=p=0',
            os.fspath(video_path)
        ]
        dur_result = subprocess.run(
            dur_cmd, capture_output=True, text=True,
            timeout=10, startupinfo=get_startup_info()
        )
        dur_text = (dur_result.stdout or '').strip()
        if dur_text and dur_text.lower() not in ('n/a', 'unknown', ''):
            format_duration = float(dur_text)
    except Exception:
        pass
    
    cmd = [
        FFPROBE_PATH,
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        os.fspath(video_path)
    ]
    
    try:
        print(f"FFprobe Streams Editor CMD: {format_cmd_for_windows(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', startupinfo=get_startup_info())
        data = json.loads(result.stdout)

        streams = {'video': None, 'audio': [], 'subtitle': []}

        for stream in data.get('streams', []):
            stype = stream.get('codec_type')
            if stype not in ('video', 'audio', 'subtitle'):
                continue

            index = stream.get('index')
            codec = stream.get('codec_name', 'unknown')
            tags = stream.get('tags', {})
            lang = tags.get('language', 'und')
            title = tags.get('title', '')
            disposition = stream.get('disposition', {})
            is_default = disposition.get('default', 0) == 1
            is_forced = disposition.get('forced', 0) == 1

            # Parse start_time to milliseconds
            start_time_str = stream.get('start_time', '0')
            try:
                start_time_ms = int(round(float(start_time_str) * 1000)) if start_time_str and start_time_str.lower() not in ('n/a', 'unknown') else 0
            except (ValueError, TypeError):
                start_time_ms = 0

            # Debug: print stream start_time parsing
            print(f"  [STATS] Stream #{index} ({stype}): start_time_str='{start_time_str}' -> start_time_ms={start_time_ms}")

            # Handle video stream (first one only)
            if stype == 'video' and streams['video'] is None:
                # Read rotation: prefer display matrix side data, fallback to ROTATE metadata tag
                rotation_value = get_video_rotation(video_path)
                if rotation_value == 0:
                    for tag_key in ('rotate', 'ROTATE', 'Rotate'):
                        rotate_tag = tags.get(tag_key)
                        if rotate_tag:
                            try:
                                rotation_value = int(float(rotate_tag))
                                break
                            except (ValueError, TypeError):
                                pass

                video_bit_rate = stream.get('bit_rate')
                if not video_bit_rate:
                    video_bit_rate = tags.get('BPS') or tags.get('BPS-eng')
                streams['video'] = {
                    'index': index,
                    'codec': codec,
                    'width': stream.get('width', 0),
                    'height': stream.get('height', 0),
                    'start_time_ms': start_time_ms,
                    'original_index': index,
                    'rotation': rotation_value,
                    'bit_rate': video_bit_rate
                }
                continue

            info = {
                'index': index,
                'codec': codec,
                'lang': lang,
                'title': title,
                'default': is_default,
                'original_index': index,  # Store original absolute index
                'start_time_ms': start_time_ms  # Offset in milliseconds
            }
            
            if stype == 'audio':
                info['channels'] = stream.get('channels', 0)
                bit_rate = stream.get('bit_rate')
                duration = stream.get('duration')
                if duration is None:
                    duration = tags.get('DURATION')
                    if duration:
                        try:
                            parts = duration.split(':')
                            if len(parts) == 3:
                                h, m, s = parts
                                duration = float(h) * 3600 + float(m) * 60 + float(s)
                        except (ValueError, TypeError):
                            duration = None
                if duration is None:
                    duration = format_duration
                stream_size_mb = None
                
                size_bytes_tag = tags.get('NUMBER_OF_BYTES') or tags.get('NUMBER_OF_BYTES-eng')
                if size_bytes_tag:
                    try:
                        stream_size_mb = float(size_bytes_tag) / (1024 * 1024)
                    except (ValueError, TypeError):
                        pass
                
                if not bit_rate:
                    bit_rate = tags.get('BPS') or tags.get('BPS-eng')

                if stream_size_mb is None and bit_rate and duration:
                    try:
                        size_bytes = (float(bit_rate) * float(duration)) / 8
                        stream_size_mb = size_bytes / (1024 * 1024)
                    except (ValueError, TypeError):
                        pass
                if stream_size_mb is None and not quick:
                    try:
                        pkt_size_cmd = [
                            FFPROBE_PATH, '-v', 'quiet',
                            '-select_streams', str(index),
                            '-show_entries', 'packet=size',
                            '-of', 'csv=p=0',
                            os.fspath(video_path)
                        ]
                        pkt_result = subprocess.run(pkt_size_cmd, capture_output=True, text=True, timeout=30, startupinfo=get_startup_info())
                        total_bytes = 0
                        for line in pkt_result.stdout.strip().split('\n'):
                            line = line.strip()
                            if line and line not in ('N/A', ''):
                                try:
                                    total_bytes += int(line)
                                except (ValueError, TypeError):
                                    pass
                        if total_bytes > 0:
                            stream_size_mb = total_bytes / (1024 * 1024)
                    except Exception:
                        pass
                if stream_size_mb is None and not quick:
                    try:
                        extract_cmd = [
                            FFMPEG_PATH, '-y',
                            '-i', os.fspath(video_path),
                            '-map', f'0:{index}',
                            '-c:a', 'copy',
                            '-f', 'matroska',
                            'pipe:1'
                        ]
                        extract_result = subprocess.run(extract_cmd, capture_output=True, timeout=60, startupinfo=get_startup_info())
                        if extract_result.stdout and len(extract_result.stdout) > 0:
                            stream_size_mb = len(extract_result.stdout) / (1024 * 1024)
                    except Exception:
                        pass
                info['size_mb'] = stream_size_mb
                info['bit_rate'] = bit_rate
                streams['audio'].append(info)
            elif stype == 'subtitle':
                info['forced'] = is_forced
                bit_rate = stream.get('bit_rate')
                duration = stream.get('duration')
                if duration is None:
                    duration = tags.get('DURATION')
                    if duration:
                        try:
                            parts = duration.split(':')
                            if len(parts) == 3:
                                h, m, s = parts
                                duration = float(h) * 3600 + float(m) * 60 + float(s)
                        except (ValueError, TypeError):
                            duration = None
                if duration is None:
                    duration = format_duration
                stream_size_mb = None
                size_bytes_tag = tags.get('NUMBER_OF_BYTES') or tags.get('NUMBER_OF_BYTES-eng')
                if size_bytes_tag:
                    try:
                        stream_size_mb = float(size_bytes_tag) / (1024 * 1024)
                    except (ValueError, TypeError):
                        pass
                if not bit_rate:
                    bit_rate = tags.get('BPS') or tags.get('BPS-eng')
                if stream_size_mb is None and bit_rate and duration:
                    try:
                        size_bytes = (float(bit_rate) * float(duration)) / 8
                        stream_size_mb = size_bytes / (1024 * 1024)
                    except (ValueError, TypeError):
                        pass
                if stream_size_mb is None and not quick:
                    try:
                        pkt_size_cmd = [
                            FFPROBE_PATH, '-v', 'quiet',
                            '-select_streams', str(index),
                            '-show_entries', 'packet=size',
                            '-of', 'csv=p=0',
                            os.fspath(video_path)
                        ]
                        pkt_result = subprocess.run(pkt_size_cmd, capture_output=True, text=True, timeout=15, startupinfo=get_startup_info())
                        total_bytes = 0
                        for line in pkt_result.stdout.strip().split('\n'):
                            line = line.strip()
                            if line and line not in ('N/A', ''):
                                try:
                                    total_bytes += int(line)
                                except (ValueError, TypeError):
                                    pass
                        if total_bytes > 0:
                            stream_size_mb = total_bytes / (1024 * 1024)
                    except Exception:
                        pass
                if stream_size_mb is None and not quick:
                    try:
                        extract_cmd = [
                            FFMPEG_PATH, '-y',
                            '-i', os.fspath(video_path),
                            '-map', f'0:{index}',
                            '-c:s', 'copy',
                            '-f', 'matroska',
                            'pipe:1'
                        ]
                        extract_result = subprocess.run(extract_cmd, capture_output=True, timeout=30, startupinfo=get_startup_info())
                        if extract_result.stdout and len(extract_result.stdout) > 0:
                            stream_size_mb = len(extract_result.stdout) / (1024 * 1024)
                    except Exception:
                        pass
                info['size_mb'] = stream_size_mb
                info['bit_rate'] = bit_rate
                streams['subtitle'].append(info)
                
        if format_duration is not None:
            streams['format_duration'] = format_duration
        return streams
    except Exception as e:
        print(f"Error reading streams: {e}")
        return None


def remux_video_with_track_selection(video_path, audio_tracks, subtitle_tracks, logger=None, stop_event=None, video_offset_ms=0, rotation_degrees=None):
    """Remux video with reordered and modified tracks.

    Args:
        video_path: Path to info video.
        audio_tracks: List of audio track dicts (in desired order).
        subtitle_tracks: List of subtitle track dicts (in desired order).
        logger: Logger for output.
        stop_event: Stop event.
        video_offset_ms: User-specified video offset in milliseconds.
        rotation_degrees: Video rotation metadata in degrees (0, 90, 180, 270) or None to keep original.

    Each track dict must contain:
        - original_index: The absolute index in the source file.
        - default: bool (is default*)
        - forced: bool (is forced* - only for subs)

    Optional track fields:
        - user_offset_ms: User-specified offset in milliseconds (for audio re-encoding)

    Returns:
        bool: True if successful.
    """
    if stop_event is None:
        stop_event = STOP_EVENT
        
    if logger:
        logger.write(f"DEBUG: remux_video_with_track_selection called for {video_path}\n")
        
    if not FFMPEG_PATH:
        msg = "CRITICAL ERROR: FFMPEG_PATH is None or empty. Cannot run FFmpeg."
        if logger:
            logger.write(msg + "\n")
        print(msg)
        return False

    video_path = Path(video_path)
    source_suffix = video_path.suffix or '.mkv'
    safe_stem = video_path.stem or video_path.name
    temp_output = video_path.with_name(f"{safe_stem}.remux{source_suffix}")
    backup_path = video_path.with_name(video_path.name + ".remux.bak")

    # Clean up any leftover temp files
    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    # Validate external subtitle files exist
    for track in subtitle_tracks:
        if track.get('is_external'):
            sub_path = track.get('external_path')
            if not sub_path or not Path(sub_path).exists():
                msg = f"ERROR: External subtitle file not found: {sub_path}"
                if logger:
                    logger.write(msg + "\n")
                return False

    # Query stream metadata for offset preservation during audio re-encoding
    # When audio is re-encoded (5.1->stereo), we need to preserve the original start_time
    # Copy mode preserves timestamps automatically, but re-encoding needs adelay filter
    source_metadata = get_video_color_metadata(video_path)
    video_start_time = source_metadata.get('video_start_time', 0.0) or 0.0

    # Build mapping: absolute stream index -> start_time for audio streams
    # This is needed because track['original_index'] is absolute, not audio-relative
    audio_stream_start_times = {}  # {absolute_index: start_time}
    try:
        # Query all streams with index and codec_type
        stream_query_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'stream=index,codec_type,start_time',
            '-of', 'json',
            os.fspath(video_path)
        ]
        stream_result = subprocess.run(
            stream_query_cmd, capture_output=True, text=True, encoding='utf-8',
            errors='replace', check=True, timeout=60, startupinfo=get_startup_info()
        )
        stream_data = json.loads(stream_result.stdout)
        for stream in stream_data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                idx = stream.get('index')
                start_str = stream.get('start_time', '0')
                try:
                    start_val = float(start_str) if start_str and start_str.lower() not in ('n/a', 'unknown') else 0.0
                except (ValueError, TypeError):
                    start_val = 0.0
                if idx is not None:
                    audio_stream_start_times[idx] = start_val
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Stream metadata query failed: {e}")
        # Continue without delay correction

    # Build FFmpeg command with STREAM-BY-STREAM offset control
    # CRITICAL: FFmpeg requires ALL inputs FIRST, then ALL output options (maps, codecs)
    # Structure: [global] [input1_opts] -i input1 [input2_opts] -i input2 ... [output_opts] output
    
    cmd = [FFMPEG_PATH, '-y']
    
    # Track input indices for later mapping
    current_input_idx = 0
    
    # =========================================================================
    # PHASE 1: ADD ALL INPUTS (with their -itsoffset options)
    # =========================================================================
    
    # INPUT 0: VIDEO STREAM (with video-specific offset)
    # CRITICAL: -itsoffset is RELATIVE to the stream's original start_time!
    # Formula: itsoffset = user_requested_offset - original_start_time
    video_original_start_ms = video_start_time * 1000.0  # Convert to ms
    video_relative_offset_ms = video_offset_ms - video_original_start_ms
    video_offset_sec = video_relative_offset_ms / 1000.0

    # Read current display rotation for clear debug output.
    source_display_rotation = get_video_rotation(video_path)
    if logger:
        with console_redirect(logger):
            print(f"[INFO] Source display rotation detected: {source_display_rotation} deg")
    
    if video_relative_offset_ms != 0:
        cmd.extend(['-itsoffset', f'{video_offset_sec:.3f}'])
        if logger:
            with console_redirect(logger):
                print(f"📐 Video offset: {video_offset_ms}ms (original: {video_original_start_ms:.0f}ms, relative: {video_relative_offset_ms:.0f}ms)")
    
    # IMPORTANT: VLC and most players use Display Matrix side data for orientation.
    # `-metadata rotate=...` alone often leaves effective orientation unchanged.
    if rotation_degrees is not None:
        normalized_rotation = int(rotation_degrees) % 360
        cmd.extend(['-display_rotation:v:0', str(normalized_rotation)])
        if logger:
            with console_redirect(logger):
                print(f"[INFO] Video display rotation override: {normalized_rotation} deg")
                source_norm = source_display_rotation % 360
                if source_norm == normalized_rotation:
                    print("[INFO] Rotation override equals current source rotation (no visible orientation change expected)")

    cmd.extend(['-i', os.fspath(video_path)])
    video_input_idx = current_input_idx
    current_input_idx += 1
    
    # AUDIO INPUTS: Each audio track gets its OWN input with specific offset
    audio_input_map = {}  # {track_index_in_list: input_idx}
    
    for audio_idx, track in enumerate(audio_tracks):
        # Get user-specified ABSOLUTE offset for this audio track
        audio_offset_ms = track.get('user_offset_ms', track.get('start_time_ms', 0))
        
        # Get original start_time for this audio stream
        audio_original_start_ms = track.get('start_time_ms', 0)
        
        # Convert ABSOLUTE offset to RELATIVE offset for FFmpeg
        # Formula: itsoffset = user_requested_offset - original_start_time
        audio_relative_offset_ms = audio_offset_ms - audio_original_start_ms
        audio_offset_sec = audio_relative_offset_ms / 1000.0
        
        # Apply offset ONLY to this audio input
        if audio_relative_offset_ms != 0:
            cmd.extend(['-itsoffset', f'{audio_offset_sec:.3f}'])
            if logger:
                with console_redirect(logger):
                    print(f"📐 Audio track {audio_idx} (stream {track['original_index']}): offset={audio_offset_ms}ms (original: {audio_original_start_ms}ms, relative: {audio_relative_offset_ms:.0f}ms)")
        
        # Add source file as input
        cmd.extend(['-i', os.fspath(video_path)])
        audio_input_map[audio_idx] = current_input_idx
        current_input_idx += 1
    
    # SUBTITLE INPUTS: Each subtitle gets its OWN input
    subtitle_input_map = {}  # {track_index_in_list: input_idx}
    subtitle_codec_settings = []  # Store codec settings for later application
    external_subtitle_count = 0
    
    for sub_idx, track in enumerate(subtitle_tracks):
        if track.get('is_external'):
            # External subtitle file
            external_path = track['external_path']
            subtitle_offset_ms = track.get('user_offset_ms', 0)
            
            # External subtitles typically start at 0, but check for original offset
            subtitle_original_start_ms = track.get('start_time_ms', 0)
            subtitle_relative_offset_ms = subtitle_offset_ms - subtitle_original_start_ms
            subtitle_offset_sec = subtitle_relative_offset_ms / 1000.0
            
            # Apply offset
            if subtitle_relative_offset_ms != 0:
                cmd.extend(['-itsoffset', f'{subtitle_offset_sec:.3f}'])
                if logger:
                    with console_redirect(logger):
                        print(f"📐 External subtitle {external_subtitle_count} ({Path(external_path).name}): offset={subtitle_offset_ms}ms (original: {subtitle_original_start_ms}ms, relative: {subtitle_relative_offset_ms:.0f}ms)")
            
            # Detect encoding
            language_hint = track.get('lang', None)
            detected_encoding = detect_subtitle_encoding(external_path, language_hint=language_hint)
            
            if detected_encoding != 'UTF-8':
                cmd.extend(['-sub_charenc', detected_encoding])
                if logger:
                    hint_msg = f" (language hint: {language_hint})" if language_hint and language_hint != 'und' else ""
                    logger.write(f"📝 External subtitle encoding detected: {Path(external_path).name} -> {detected_encoding}{hint_msg}\n")
            
            cmd.extend(['-i', os.fspath(external_path)])
            subtitle_input_map[sub_idx] = current_input_idx
            current_input_idx += 1
            external_subtitle_count += 1
            
            # Store codec settings for external subtitle
            codec = track.get('codec', 'srt').lower()
            codec_map = {
                'srt': 'srt',
                'ass': 'ass',
                'ssa': 'ass',
                'vtt': 'webvtt',
                'sub': 'microdvd'
            }
            ffmpeg_codec = codec_map.get(codec, 'srt')
            subtitle_codec_settings.append({
                'idx': sub_idx,
                'codec': ffmpeg_codec,
                'lang': track.get('lang', 'und'),
                'title': track.get('title', ''),
                'default': track.get('default', False),
                'forced': track.get('forced', False)
            })
        else:
            # Embedded subtitle from source file
            subtitle_offset_ms = track.get('user_offset_ms', track.get('start_time_ms', 0))

            # Get original start_time for this subtitle stream
            subtitle_original_start_ms = track.get('start_time_ms', 0)

            # Convert ABSOLUTE offset to RELATIVE offset for FFmpeg
            subtitle_relative_offset_ms = subtitle_offset_ms - subtitle_original_start_ms
            subtitle_offset_sec = subtitle_relative_offset_ms / 1000.0

            # DEBUG: Always log subtitle offset info
            if logger:
                with console_redirect(logger):
                    print(f"[SCAN] DEBUG Subtitle {sub_idx}: user_offset_ms={track.get('user_offset_ms', 'NOT SET')}, start_time_ms={track.get('start_time_ms', 'NOT SET')}")
                    print(f"[SCAN] DEBUG Subtitle {sub_idx}: calculated offset={subtitle_offset_ms}ms, original={subtitle_original_start_ms}ms, relative={subtitle_relative_offset_ms}ms")

            # Apply offset using -itsoffset (for demuxer level timestamp shift)
            if subtitle_relative_offset_ms != 0:
                cmd.extend(['-itsoffset', f'{subtitle_offset_sec:.3f}'])
                if logger:
                    with console_redirect(logger):
                        print(f"📐 Embedded subtitle {sub_idx} (stream {track['original_index']}): offset={subtitle_offset_ms}ms (original: {subtitle_original_start_ms}ms, relative: {subtitle_relative_offset_ms:.0f}ms)")

            # Add source file as input
            cmd.extend(['-i', os.fspath(video_path)])
            subtitle_input_map[sub_idx] = current_input_idx
            current_input_idx += 1

            # Use COPY mode for subtitles - same as video/audio
            # Video and audio offsets work with copy mode, so subtitle should too
            ffmpeg_codec = 'copy'

            subtitle_codec_settings.append({
                'idx': sub_idx,
                'codec': ffmpeg_codec,
                'lang': track.get('lang', 'und'),
                'title': track.get('title', ''),
                'default': track.get('default', False),
                'forced': track.get('forced', False),
                'offset_sec': subtitle_offset_sec if subtitle_relative_offset_ms != 0 else 0
            })
    
    # METADATA INPUT: Source file WITHOUT offset for metadata/chapters/attachments
    cmd.extend(['-i', os.fspath(video_path)])
    metadata_input_idx = current_input_idx
    current_input_idx += 1
    
    # =========================================================================
    # PHASE 2: ADD ALL MAPPINGS (now that all inputs are defined)
    # =========================================================================
    
    # Map video from video input
    cmd.extend(['-map', f'{video_input_idx}:v'])
    
    # Map audio streams (in order). Converted tracks are mapped from a
    # filtergraph label instead of copying the original stream unchanged.
    audio_filter_chains = []
    for audio_idx in range(len(audio_tracks)):
        track = audio_tracks[audio_idx]
        input_idx = audio_input_map[audio_idx]
        if track.get('needs_conversion'):
            filter_label = f'audio_conv_{audio_idx}'
            method = track.get('conversion_method', 'fast')
            filter_chain = build_audio_conversion_filter(method)
            audio_filter_chains.append(
                f'[{input_idx}:{track["original_index"]}]{filter_chain}[{filter_label}]'
            )
            cmd.extend(['-map', f'[{filter_label}]'])
        else:
            # Map ONLY this specific audio stream (use ABSOLUTE stream index)
            cmd.extend(['-map', f'{input_idx}:{track["original_index"]}'])
    
    # Map subtitle streams (in order)
    for sub_idx in range(len(subtitle_tracks)):
        track = subtitle_tracks[sub_idx]
        input_idx = subtitle_input_map[sub_idx]
        if track.get('is_external'):
            # External subtitle: map stream 0 from that input
            cmd.extend(['-map', f'{input_idx}:0'])
        else:
            # Embedded subtitle: map specific stream index
            cmd.extend(['-map', f'{input_idx}:{track["original_index"]}'])
    
    # Map attachments and metadata from metadata input
    try:
        attachment_info = get_attachment_info(video_path, logger)
        
        if attachment_info:
            if logger:
                with console_redirect(logger):
                    print(f"📎 Found {len(attachment_info)} attachment(s) in source")
            
            # Map each attachment individually using ABSOLUTE STREAM INDEX from metadata input
            for att in attachment_info:
                cmd.extend(['-map', f'{metadata_input_idx}:{att["index"]}'])
            
            # Apply filename metadata for each attachment (AFTER -map_metadata)
            # Store these for later application
            attachment_metadata = []
            for output_idx, att in enumerate(attachment_info):
                filename = att['filename']
                if not filename:
                    filename = generate_attachment_filename(output_idx, att['mimetype'])
                    if logger:
                        with console_redirect(logger):
                            print(f"[WARN] Attachment {output_idx} missing filename tag, using: {filename}")
                
                attachment_metadata.append({
                    'idx': output_idx,
                    'filename': filename,
                    'mimetype': att.get('mimetype')
                })
        else:
            # No attachments found, use fallback mapping
            cmd.extend(['-map', f'{metadata_input_idx}:t?'])
            attachment_metadata = []
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Attachment metadata error (using fallback mapping): {e}")
        # Fallback to simple mapping
        cmd.extend(['-map', f'{metadata_input_idx}:t?'])
        attachment_metadata = []
        
    cmd.extend([
        '-map_chapters', str(metadata_input_idx),  # Preserve chapters from metadata input
        '-map_metadata', str(metadata_input_idx),  # Preserve global metadata from metadata input
    ])

    if audio_filter_chains:
        cmd.extend(['-filter_complex', ';'.join(audio_filter_chains)])
    
    # =========================================================================
    # CODEC SETTINGS
    # =========================================================================
    
    # Video codec - always copy
    cmd.extend(['-c:v', 'copy'])
    
    # Audio codec: default to copy, then override converted output streams.
    cmd.extend(['-c:a', 'copy'])
    for i, track in enumerate(audio_tracks):
        if track.get('needs_conversion'):
            cmd.extend([f'-c:a:{i}', 'aac', f'-b:a:{i}', '192k', f'-ac:a:{i}', '2'])

    # Subtitle codec - per-stream settings (external subtitles may need different codecs)
    for sub_settings in subtitle_codec_settings:
        idx = sub_settings['idx']
        codec = sub_settings['codec']
        cmd.extend([f'-c:s:{idx}', codec])

    # Attachments (fonts) and Data - always copy
    cmd.extend(['-c:t', 'copy', '-c:d', 'copy'])

    # =========================================================================
    # METADATA AND DISPOSITIONS
    # =========================================================================

    # Audio Dispositions
    for i, track in enumerate(audio_tracks):
        # -disposition:a:<index>
        is_default = 1 if track.get('default') else 0
        cmd.extend([f'-disposition:a:{i}', 'default' if is_default else '0'])

        lang = track.get('lang') or track.get('language') or 'und'
        cmd.extend([f'-metadata:s:a:{i}', f'language={lang}'])

        title = track.get('output_title') if track.get('needs_conversion') else track.get('title', '')
        if track.get('needs_conversion') and not title:
            title = get_audio_conversion_title(track.get('conversion_method', 'fast'))
        if title:
            cmd.extend([f'-metadata:s:a:{i}', f'title={title}'])

    # Subtitle Dispositions and Metadata
    for sub_settings in subtitle_codec_settings:
        i = sub_settings['idx']
        is_default = sub_settings.get('default', False)
        is_forced = sub_settings.get('forced', False)

        # FIXED: Combine disposition flags properly
        # FFmpeg only accepts ONE -disposition per stream, must combine with '+'
        flags = []
        if is_default:
            flags.append('default')
        if is_forced:
            flags.append('forced')
        disposition_value = '+'.join(flags) if flags else '0'

        cmd.extend([f'-disposition:s:{i}', disposition_value])

        # Metadata (language and title)
        lang = sub_settings.get('lang', 'und')
        cmd.extend([f'-metadata:s:s:{i}', f'language={lang}'])

        title = sub_settings.get('title', '')
        if title:
            cmd.extend([f'-metadata:s:s:{i}', f'title={title}'])
    
    # Apply attachment metadata (filename and mimetype tags)
    # CRITICAL: Must be done AFTER -map_metadata to prevent being overwritten
    if 'attachment_metadata' in locals() and attachment_metadata:
        for att_meta in attachment_metadata:
            idx = att_meta['idx']
            cmd.extend([f'-metadata:s:t:{idx}', f'filename={att_meta["filename"]}'])
            if att_meta.get('mimetype'):
                cmd.extend([f'-metadata:s:t:{idx}', f'mimetype={att_meta["mimetype"]}'])

    # Video rotation metadata (AFTER -map_metadata to override original value)
    # MKV uses uppercase ROTATE tag, MP4 uses lowercase rotate tag.
    # Set BOTH cases to ensure the original tag from -map_metadata is properly overridden
    # regardless of what case the source file used.
    if rotation_degrees is not None:
        cmd.extend(['-metadata:s:v:0', f'rotate={rotation_degrees}'])
        cmd.extend(['-metadata:s:v:0', f'ROTATE={rotation_degrees}'])

    cmd.append(os.fspath(temp_output))

    if logger:
        # Use simple printing to stream as the logger might be connected to the window
        logger.write(f"\n{'='*80}\n")
        logger.write(f"🎬 TRACK REMUXING: {video_path.name}\n")

        # Log external subtitles
        external_count = sum(1 for t in subtitle_tracks if t.get('is_external'))
        if external_count > 0:
            logger.write(f"📎 External subtitles: {external_count}\n")
            for track in subtitle_tracks:
                if track.get('is_external'):
                    logger.write(f"   - {track['external_path'].name} ({track['lang']})\n")

        try:
            cmd_str = format_cmd_for_windows(cmd)
            logger.write(f"Command: {cmd_str}\n")
        except Exception as e:
            logger.write(f"Error formulating command string: {e}\n")
        logger.write(f"{'='*80}\n\n")
        logger.flush()

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            universal_newlines=True,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
            
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    raise EncodingStopped()
                if logger:
                    logger.write(line)
        except EncodingStopped:
            raise
        except Exception:
             pass 
        finally:
            try:
                process.wait(timeout=600)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                if logger:
                    with console_redirect(logger):
                        print(f"[ERROR] Faststart optimization timed out")
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
                    
        if process.returncode != 0:
            if logger:
                logger.write(f"[ERROR] Remux error (rc={process.returncode})\\n")
            if temp_output.exists():
                try:
                    temp_output.unlink()
                except (OSError, PermissionError):
                    if logger:
                        logger.write(f"[WARN] Failed to delete temp file: {temp_output}\n")
            return False
            
        # Replace original
        original_replaced = False
        try:
            if backup_path.exists():
                backup_path.unlink()
            video_path.replace(backup_path)
            original_replaced = True
            temp_output.replace(video_path)
            
            if logger:
                logger.write(f"[OK] Track editing complete: {video_path.name}\\n")
        except Exception as e:
            if logger:
                logger.write(f"[ERROR] File replace error: {e}\\n")
            # Try recover
            if original_replaced and backup_path.exists():
                try:
                    backup_path.replace(video_path)
                except (OSError, PermissionError) as e:
                    if logger:
                        logger.write(f"[WARN] Failed to restore backup: {e}\n")
            return False
        finally:
            if backup_path.exists():
                try:
                    backup_path.unlink()
                except (OSError, PermissionError):
                    pass  # Cleanup best effort
                
        return True

    except Exception as e:
        if logger:
             logger.write(f"[ERROR] Exception during remux: {e}\\n")
        return False


def get_video_color_metadata(video_path):
    """Query video color/pixel format metadata using ffprobe.
    
    Returns a dict with pix_fmt, color_space, color_primaries, color_transfer,
    and HDR metadata if present.
    Returns None for any value that cannot be determined.
    
    Args:
        video_path: Path to the video file
        
    Returns:
        dict: {'pix_fmt': str, 'color_space': str, 'color_primaries': str, 'color_transfer': str,
               'master_display': str, 'max_content': int, 'max_average': int,
               'sample_aspect_ratio': str, 'display_aspect_ratio': str,
               'video_start_time': float, 'audio_start_time': float, 'audio_delay_ms': int,
               'audio_start_times': list[float], 'subtitle_start_times': list[float]}
    """
    default_metadata = {
        'pix_fmt': None, 'color_space': None, 'color_primaries': None, 'color_transfer': None,
        'master_display': None, 'max_content': None, 'max_average': None,
        'sample_aspect_ratio': None, 'display_aspect_ratio': None,
        'video_start_time': None, 'audio_start_time': None, 'audio_delay_ms': 0,
        'audio_start_times': [], 'subtitle_start_times': []
    }
    try:
        if not FFPROBE_PATH:
            return copy.deepcopy(default_metadata)

        video_path = Path(video_path)
        if not video_path.exists():
            return copy.deepcopy(default_metadata)
        
        # Query video stream metadata including HDR side data
        cmd = [
            FFPROBE_PATH,
            '-v', 'error',
            '-select_streams', 'v:0',
            '-read_intervals', '%+#1',
            '-show_entries', 'stream=pix_fmt,color_space,color_primaries,color_transfer,color_range,sample_aspect_ratio,display_aspect_ratio',
            '-show_entries', 'side_data=mastering_display_luminance,max_content,max_average',
            '-of', 'default=noprint_wrappers=1',
            str(video_path)
        ]
        
        # Log command if running in worker (stdout redirected) or debug
        print(f"FFprobe Color CMD: {format_cmd_for_windows(cmd)}")
        
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            startupinfo=get_startup_info(),
            timeout=600
        )
        
        metadata = {
            'pix_fmt': None,
            'color_space': None,
            'color_primaries': None,
            'color_transfer': None,
            'color_range': None,     # Limited/Full range (tv/pc)
            'master_display': None,  # HDR mastering display metadata
            'max_content': None,     # MaxCLL (content light level)
            'max_average': None,     # MaxFALL (frame average light level)
            'sample_aspect_ratio': None,   # SAR for anamorphic videos (e.g., "4:3")
            'display_aspect_ratio': None,  # DAR for anamorphic videos (e.g., "16:9")
            'video_start_time': None,      # Video stream start_time in seconds
            'audio_start_time': None,      # First audio stream start_time in seconds
            'audio_delay_ms': 0,           # Calculated audio delay: (audio - video) * 1000
            'audio_start_times': [],       # List of start_time for ALL audio streams
            'subtitle_start_times': []     # List of start_time for ALL subtitle streams
        }
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key in metadata and value and value.lower() != 'unknown':
                        metadata[key] = value
        
        # Also try to get HDR metadata via separate query for side_data_list
        try:
            hdr_cmd = [
                FFPROBE_PATH,
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_frames', '-read_intervals', '%+#1',
                '-show_entries', 'frame=side_data_list',
                '-of', 'json',
                str(video_path)
            ]
            
            hdr_result = subprocess.run(
                hdr_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                startupinfo=get_startup_info(),
                timeout=600  # Timeout for large files (10 minutes)
            )
            
            if hdr_result.returncode == 0 and hdr_result.stdout:
                import json as json_module
                try:
                    data = json_module.loads(hdr_result.stdout)
                    frames = data.get('frames', [])
                    if frames:
                        side_data_list = frames[0].get('side_data_list', [])
                        for sd in side_data_list:
                            sd_type = sd.get('side_data_type', '')
                            
                            # Mastering display metadata
                            if 'Mastering display' in sd_type:
                                # Build mastering display string for FFmpeg
                                parts = []
                                if 'red_x' in sd:
                                    parts.append(f"R({sd['red_x']},{sd['red_y']})")
                                if 'green_x' in sd:
                                    parts.append(f"G({sd['green_x']},{sd['green_y']})")
                                if 'blue_x' in sd:
                                    parts.append(f"B({sd['blue_x']},{sd['blue_y']})")
                                if 'white_point_x' in sd:
                                    parts.append(f"WP({sd['white_point_x']},{sd['white_point_y']})")
                                if 'min_luminance' in sd:
                                    parts.append(f"L({sd['min_luminance']},{sd['max_luminance']})")
                                if parts:
                                    metadata['master_display'] = ''.join(parts)
                            
                            # Content light level
                            if 'Content light level' in sd_type:
                                if 'max_content' in sd:
                                    metadata['max_content'] = sd['max_content']
                                if 'max_average' in sd:
                                    metadata['max_average'] = sd['max_average']
                except (json_module.JSONDecodeError, KeyError, TypeError):
                    pass
        except (subprocess.TimeoutExpired, Exception):
            pass  # HDR metadata query failed, continue without it

        # Query stream start_time for audio/video sync (delay detection)
        try:
            delay_cmd = [
                FFPROBE_PATH,
                '-v', 'error',
                '-show_entries', 'stream=codec_type,start_time',
                '-of', 'default=noprint_wrappers=1',
                str(video_path)
            ]

            delay_result = subprocess.run(
                delay_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                startupinfo=get_startup_info(),
                timeout=60
            )

            if delay_result.returncode == 0:
                video_start = None
                audio_starts = []
                subtitle_starts = []
                current_codec_type = None

                for line in delay_result.stdout.strip().split('\n'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        if key == 'codec_type':
                            current_codec_type = value
                        elif key == 'start_time':
                            # Default to 0.0 for N/A values (stream exists but no explicit start_time)
                            start_val = 0.0
                            if value and value.lower() not in ('n/a', 'unknown'):
                                try:
                                    start_val = float(value)
                                except (ValueError, TypeError):
                                    pass

                            if current_codec_type == 'video' and video_start is None:
                                video_start = start_val
                            elif current_codec_type == 'audio':
                                audio_starts.append(start_val)
                            elif current_codec_type == 'subtitle':
                                subtitle_starts.append(start_val)

                metadata['video_start_time'] = video_start
                metadata['audio_start_time'] = audio_starts[0] if audio_starts else None
                metadata['audio_start_times'] = audio_starts
                metadata['subtitle_start_times'] = subtitle_starts

                # Calculate audio delay in milliseconds (positive = audio starts later)
                if video_start is not None and audio_starts:
                    delay_sec = audio_starts[0] - video_start
                    metadata['audio_delay_ms'] = int(round(delay_sec * 1000))

        except (subprocess.TimeoutExpired, Exception):
            pass  # Stream delay query failed, continue without it

        return metadata
        
    except Exception:
        return copy.deepcopy(default_metadata)


def _parse_ratio_components(value):
    """Parse SAR/DAR strings like '16:9' or '4/3'."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in ('n/a', 'unknown', '0:1', '1:0', '0/1', '1/0'):
        return None
    separator = ':' if ':' in text else '/' if '/' in text else None
    if separator is None:
        return None
    parts = text.split(separator, 1)
    if len(parts) != 2:
        return None
    try:
        numerator = float(parts[0].strip())
        denominator = float(parts[1].strip())
    except (TypeError, ValueError):
        return None
    if numerator <= 0 or denominator <= 0:
        return None
    return numerator, denominator


def _format_ratio_string(numerator, denominator):
    if numerator <= 0 or denominator <= 0:
        return None
    if abs(numerator - round(numerator)) < 1e-9 and abs(denominator - round(denominator)) < 1e-9:
        return f"{int(round(numerator))}:{int(round(denominator))}"
    numerator_str = f"{numerator:.6f}".rstrip('0').rstrip('.')
    denominator_str = f"{denominator:.6f}".rstrip('0').rstrip('.')
    return f"{numerator_str}:{denominator_str}"


def get_adjusted_display_aspect_ratio(color_metadata, width=None, height=None, rotation_degrees=0):
    """Return effective output DAR, optionally adjusted for 90/270 degree rotation."""
    if not isinstance(color_metadata, dict):
        return None

    dar_components = _parse_ratio_components(color_metadata.get('display_aspect_ratio'))
    if dar_components is not None:
        numerator, denominator = dar_components
    else:
        sar_components = _parse_ratio_components(color_metadata.get('sample_aspect_ratio'))
        if sar_components is None or not width or not height:
            return None
        sar_num, sar_den = sar_components
        numerator = float(width) * sar_num
        denominator = float(height) * sar_den

    normalized_rotation = int(rotation_degrees or 0) % 360
    if normalized_rotation in (90, 270):
        numerator, denominator = denominator, numerator

    return _format_ratio_string(numerator, denominator)



def get_video_rotation(input_path):
    """Forrás videó Display Matrix rotáció szögének lekérdezése ffprobe-bal.

    A telefon videók a pixeleket gyakran landscape-ben tárolják, és egy Display Matrix
    side_data-ban jelzik a tényleges megjelenítési tájolást (pl. rotation=-90 = portrait).

    Args:
        input_path: Path to the video file.

    Returns:
        int: Rotation angle (0, 90, -90, 180). 0 ha nincs rotáció vagy hiba.
    """
    def _parse_rotation_from_ffprobe_json(stdout_text):
        """Return rotation from ffprobe JSON or None if not found."""
        if not stdout_text or not stdout_text.strip():
            return None
        data = json.loads(stdout_text)
        streams = data.get('streams', [])
        if not streams:
            return None

        stream0 = streams[0]

        for side_data in stream0.get('side_data_list', []):
            rotation = side_data.get('rotation')
            if rotation is not None:
                return int(round(float(rotation)))

        tags = stream0.get('tags', {}) or {}
        for tag_key in ('rotate', 'ROTATE', 'Rotate'):
            if tag_key in tags:
                return int(round(float(tags[tag_key])))

        return None

    # 1) ffprobe JSON with explicit side_data request
    try:
        cmd_probe_json = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream_side_data=rotation:stream_tags=rotate,ROTATE,Rotate',
            '-print_format', 'json',
            os.fspath(input_path)
        ]
        result_probe_json = subprocess.run(
            cmd_probe_json, capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=30, startupinfo=get_startup_info()
        )
        if result_probe_json.returncode == 0:
            parsed = _parse_rotation_from_ffprobe_json(result_probe_json.stdout)
            if parsed is not None:
                return parsed
    except (subprocess.SubprocessError, ValueError, TypeError, OSError, json.JSONDecodeError, KeyError):
        pass

    # 2) ffprobe JSON full stream dump (legacy fallback)
    try:
        cmd_json = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_streams',
            '-print_format', 'json',
            os.fspath(input_path)
        ]
        result_json = subprocess.run(
            cmd_json, capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=30, startupinfo=get_startup_info()
        )
        if result_json.returncode == 0:
            parsed = _parse_rotation_from_ffprobe_json(result_json.stdout)
            if parsed is not None:
                return parsed
    except (subprocess.SubprocessError, ValueError, TypeError, OSError, json.JSONDecodeError, KeyError):
        pass

    # 3) ffprobe plain text fallback
    try:
        cmd_text = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream_side_data=rotation',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(input_path)
        ]
        result_text = subprocess.run(
            cmd_text, capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=30, startupinfo=get_startup_info()
        )
        if result_text.returncode == 0 and result_text.stdout.strip():
            for line in result_text.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    return int(round(float(line)))
                except (ValueError, TypeError):
                    continue
    except (subprocess.SubprocessError, ValueError, TypeError, OSError):
        pass

    # 4) Final fallback: parse ffmpeg stream info `displaymatrix: rotation of ...`
    try:
        cmd_ffmpeg_probe = [FFMPEG_PATH, '-hide_banner', '-i', os.fspath(input_path)]
        result_ffmpeg_probe = subprocess.run(
            cmd_ffmpeg_probe, capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=30, startupinfo=get_startup_info()
        )
        combined_output = (result_ffmpeg_probe.stdout or '') + '\n' + (result_ffmpeg_probe.stderr or '')
        match = re.search(r'displaymatrix:\s*rotation of\s*([-+]?\d+(?:\.\d+)?)\s*degrees', combined_output, re.IGNORECASE)
        if match:
            return int(round(float(match.group(1))))
    except (subprocess.SubprocessError, ValueError, TypeError, OSError, re.error):
        pass

    return 0


def get_effective_output_rotation_degrees(input_path, hard_rotate_degrees=0):
    """Return effective output rotation after source display rotation and hard-rotate."""
    try:
        source_rotation = int(round(float(get_video_rotation(input_path) or 0))) % 360
    except (ValueError, TypeError):
        source_rotation = 0

    try:
        hard_rotate = int(round(float(hard_rotate_degrees or 0))) % 360
    except (ValueError, TypeError):
        hard_rotate = 0

    if hard_rotate not in (0, 90, 180, 270):
        hard_rotate = 0

    return (source_rotation + hard_rotate) % 360


def _parse_br_to_int(bit_rate):
    if bit_rate is None:
        return None
    try:
        return int(float(bit_rate))
    except (ValueError, TypeError):
        return None


def _calc_size_bytes_from_mb(size_mb):
    if size_mb is None:
        return None
    try:
        return int(float(size_mb) * 1024 * 1024)
    except (ValueError, TypeError):
        return None


def get_video_extra_metadata(video_path):
    """Collect compact tooltip/cache metadata for a video file."""
    video_path = Path(video_path)

    try:
        from .core_preamble_and_imports import LOG_WRITER
        def _meta_log(msg):
            if LOG_WRITER and not LOG_WRITER.closed:
                try:
                    LOG_WRITER.write(msg + "\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, ValueError):
                    pass
    except ImportError:
        def _meta_log(msg):
            pass

    _t0 = time.time()
    _name = video_path.name
    color_metadata = get_video_color_metadata(video_path) or {}

    try:
        width, height = get_video_resolution(video_path)
    except Exception:
        width, height = None, None

    try:
        rotation = int(round(float(get_video_rotation(video_path) or 0)))
    except (ValueError, TypeError):
        rotation = 0

    streams = {'video': [], 'audio': [], 'subtitle': []}
    try:
        editor_streams = get_video_streams_for_editor(video_path, quick=False)

        if editor_streams:
            vs = editor_streams.get('video')
            if vs:
                streams['video'].append({
                    'index': vs.get('index'),
                    'codec': vs.get('codec'),
                    'width': vs.get('width'),
                    'height': vs.get('height'),
                    'rotation': vs.get('rotation', 0),
                    'start_time_ms': vs.get('start_time_ms', 0),
                    'bit_rate': _parse_br_to_int(vs.get('bit_rate')),
                    'size_bytes': None,
                })

            for a in editor_streams.get('audio') or []:
                streams['audio'].append({
                    'index': a.get('index'),
                    'codec': a.get('codec'),
                    'lang': a.get('lang', 'und'),
                    'title': a.get('title', ''),
                    'default': bool(a.get('default')),
                    'forced': bool(a.get('forced', False)),
                    'start_time_ms': a.get('start_time_ms', 0),
                    'channels': a.get('channels'),
                    'bit_rate': _parse_br_to_int(a.get('bit_rate')),
                    'size_bytes': _calc_size_bytes_from_mb(a.get('size_mb')),
                })

            for s in editor_streams.get('subtitle') or []:
                streams['subtitle'].append({
                    'index': s.get('index'),
                    'codec': s.get('codec'),
                    'lang': s.get('lang', 'und'),
                    'title': s.get('title', ''),
                    'default': bool(s.get('default')),
                    'forced': bool(s.get('forced', False)),
                    'start_time_ms': s.get('start_time_ms', 0),
                    'bit_rate': _parse_br_to_int(s.get('bit_rate')),
                    'size_bytes': _calc_size_bytes_from_mb(s.get('size_mb')),
                })
    except Exception:
        streams = {'video': [], 'audio': [], 'subtitle': []}

    file_size = None
    try:
        file_size = video_path.stat().st_size
    except (OSError, PermissionError):
        pass

    if streams['video'] and file_size:
        non_video_bytes = 0
        for stype in ('audio', 'subtitle'):
            for t in streams.get(stype) or []:
                sb = t.get('size_bytes')
                if sb:
                    non_video_bytes += sb
        video_est = file_size - non_video_bytes
        if video_est > 0:
            streams['video'][0]['size_bytes'] = video_est

    duration_seconds = None
    if editor_streams and editor_streams.get('format_duration') is not None:
        duration_seconds = float(editor_streams['format_duration'])
    if duration_seconds is None:
        try:
            duration_seconds, _ = get_video_info(video_path)
        except Exception:
            pass

    bit_rate = None
    if file_size and duration_seconds and duration_seconds > 0:
        bit_rate = int(file_size * 8 / duration_seconds)
    else:
        try:
            cmd_br = [
                FFPROBE_PATH, '-v', 'error',
                '-show_entries', 'format=bit_rate',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                os.fspath(video_path)
            ]
            br_result = subprocess.run(
                cmd_br,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                startupinfo=get_startup_info(),
                timeout=30
            )
            br_text = (br_result.stdout or '').strip()
            if br_text and br_text.lower() not in ('n/a', 'unknown', ''):
                bit_rate = int(float(br_text))
        except Exception:
            pass

    if streams['video']:
        vs = streams['video'][0]
        if vs.get('size_bytes') and duration_seconds and duration_seconds > 0:
            try:
                vs['bit_rate'] = int(vs['size_bytes'] * 8 / duration_seconds)
            except Exception:
                pass

    _dt = (time.time() - _t0) * 1000
    _br_src = 'calculated' if (file_size and duration_seconds and duration_seconds > 0) else 'ffprobe_fallback' if bit_rate else 'none'
    _meta_log(
        f"  [EXTRA-META] {_name}: {_dt:.0f}ms | "
        f"file_size={file_size} duration={duration_seconds} bitrate={bit_rate} ({_br_src}) | "
        f"video_streams={len(streams['video'])} audio_streams={len(streams['audio'])} sub_streams={len(streams['subtitle'])}"
    )

    return {
        'width': width,
        'height': height,
        'rotation': rotation,
        'file_size': file_size,
        'bit_rate': bit_rate,
        'color_metadata': {
            'pix_fmt': color_metadata.get('pix_fmt'),
            'color_space': color_metadata.get('color_space'),
            'color_primaries': color_metadata.get('color_primaries'),
            'color_transfer': color_metadata.get('color_transfer'),
            'color_range': color_metadata.get('color_range'),
            'sample_aspect_ratio': color_metadata.get('sample_aspect_ratio'),
            'display_aspect_ratio': color_metadata.get('display_aspect_ratio'),
        },
        'streams': streams
    }


def get_processing_color_range(color_metadata):
    """Resolve explicit processing range for filtergraphs and matching ffmpeg metadata."""
    color_range = ""
    pix_fmt = ""
    if color_metadata:
        color_range = str(color_metadata.get('color_range') or '').strip().lower()
        pix_fmt = str(color_metadata.get('pix_fmt') or '').strip().lower()

    if color_range == 'pc' or pix_fmt.startswith('yuvj'):
        return 'full', 'pc'

    return 'limited', 'tv'


def get_processing_color_range_with_reason(color_metadata):
    """Resolve processing range and explain the decision for debug logging."""
    color_range = ""
    pix_fmt = ""
    if color_metadata:
        color_range = str(color_metadata.get('color_range') or '').strip().lower()
        pix_fmt = str(color_metadata.get('pix_fmt') or '').strip().lower()

    if color_range == 'pc':
        return 'full', 'pc', 'color_range=pc'
    if color_range == 'tv':
        return 'limited', 'tv', 'color_range=tv'
    if pix_fmt.startswith('yuvj'):
        return 'full', 'pc', f'pix_fmt={pix_fmt}'

    return 'limited', 'tv', 'fallback=limited_default'


def build_explicit_range_filter_chain(filter_body, range_name):
    """Wrap a filter chain with explicit input/output range preservation."""
    return (
        f"scale=iw:ih:in_range={range_name}:out_range={range_name},"
        f"{filter_body},"
        f"scale=iw:ih:in_range={range_name}:out_range={range_name}"
    )


def merge_color_metadata(primary_metadata, fallback_metadata):
    """Merge color metadata dicts, filling missing primary values from fallback."""
    merged = {}
    for metadata in (fallback_metadata or {}, primary_metadata or {}):
        if not isinstance(metadata, dict):
            continue
        for key, value in metadata.items():
            if value is None:
                continue
            if isinstance(value, str) and value.strip().lower() in ('', 'n/a', 'unknown'):
                continue
            merged[key] = value
    return merged


def build_av1_metadata_bsf_args(color_metadata):
    """Build AV1 bitstream metadata args for primaries/transfer/matrix/range.

    Generic `-color_*` output options are not always enough to persist primaries and
    transfer characteristics in the final AV1 stream. This supplements them with an
    `av1_metadata` bitstream filter whenever the source values are known.
    """
    if not isinstance(color_metadata, dict):
        return []

    color_primaries_map = {
        'bt709': 1,
        'bt470m': 4,
        'bt470bg': 5,
        'smpte170m': 6,
        'smpte240m': 7,
        'film': 8,
        'bt2020': 9,
        'smpte428': 10,
        'smpte431': 11,
        'smpte432': 12,
        'ebu3213': 22,
        'jedec-p22': 22,
    }
    transfer_characteristics_map = {
        'bt709': 1,
        'gamma22': 4,
        'gamma28': 5,
        'smpte170m': 6,
        'smpte240m': 7,
        'linear': 8,
        'log100': 9,
        'log316': 10,
        'iec61966-2-4': 11,
        'bt1361': 12,
        'iec61966-2-1': 13,
        'bt2020-10': 14,
        'bt2020-12': 15,
        'smpte2084': 16,
        'smpte428': 17,
        'arib-std-b67': 18,
    }
    matrix_coefficients_map = {
        'rgb': 0,
        'bt709': 1,
        'fcc': 4,
        'bt470bg': 5,
        'smpte170m': 6,
        'smpte240m': 7,
        'ycgco': 8,
        'bt2020nc': 9,
        'bt2020_ncl': 9,
        'bt2020c': 10,
        'bt2020_cl': 10,
        'smpte2085': 11,
        'chroma-derived-nc': 12,
        'chroma-derived-c': 13,
        'ictcp': 14,
    }

    def _norm(value):
        if value is None:
            return ''
        return str(value).strip().lower()

    bsf_parts = []

    primaries = color_primaries_map.get(_norm(color_metadata.get('color_primaries')))
    if primaries is not None:
        bsf_parts.append(f'color_primaries={primaries}')

    transfer = transfer_characteristics_map.get(_norm(color_metadata.get('color_transfer')))
    if transfer is not None:
        bsf_parts.append(f'transfer_characteristics={transfer}')

    matrix = matrix_coefficients_map.get(_norm(color_metadata.get('color_space')))
    if matrix is not None:
        bsf_parts.append(f'matrix_coefficients={matrix}')

    color_range = _norm(color_metadata.get('color_range'))
    if color_range == 'pc':
        bsf_parts.append('color_range=pc')
    elif color_range == 'tv':
        bsf_parts.append('color_range=tv')

    if not bsf_parts:
        return []

    return ['-bsf:v', f"av1_metadata={':'.join(bsf_parts)}"]


def log_color_range_debug(print_func, source_range, range_reason, output_color_range, prefix=""):
    """Emit a consistent color-range debug block to the active log sink."""
    lead = prefix or ""
    print_func(f"{lead}Detected Source Range: {source_range}")
    print_func(f"{lead}Range Detection Reason: {range_reason}")
    print_func(f"{lead}Processing Range: {source_range}->{source_range}")
    print_func(f"{lead}Output Color Range Metadata: {output_color_range}")


def create_denoised_lossless_master(input_path, output_path, use_nvenc=True, logger=None, stop_event=None,
                                     progress_callback=None, total_frames=None, total_duration_sec=None,
                                     denoise_level=1, force_8bit_master=False):
    from .core_paths_tools_logging import EncodingStopped

    """Create a denoised lossless master copy of a video.
    
    Applies the vaguedenoiser+cas filter and encodes to lossless format.
    This master copy is used for CRF search and final encoding, then deleted.
    
    Args:
        input_path: Path to the source video file.
        output_path: Path for the lossless output (e.g., video_denoised_master.mkv).
        use_nvenc: True = use NVENC HEVC lossless, False = use x264 CRF 0 (CPU).
        logger: Optional logger for console output.
        stop_event: Event to stop the process.
        progress_callback: Optional callback(frame, total_frames, fps, speed) for progress display.
        total_frames: Total frame count for percentage calculation.
        denoise_level: 1 = strong (threshold=4), 2 = light (threshold=2),
            3 = very strong (threshold=6), 4 = ultra strong (threshold=8). Default: 1.
        force_8bit_master: If True, create an 8-bit denoised master for playback diagnostics.
        
    Returns:
        tuple: (bool, str) - (True if successful, denoise_params_str)
    """
    if stop_event is None:
        from .core_paths_tools_logging import STOP_EVENT
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        from .core_paths_tools_logging import EncodingStopped
        raise EncodingStopped()
    
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    if not input_path.exists():
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Denoised master: Source not found: {input_path}")
        return False, ""
    
    # Denoise filter: vaguedenoiser + contrast adaptive sharpening
    # denoise_level: 2 = light, 1 = strong, 3 = very strong, 4 = ultra strong
    if denoise_level == 2:
        threshold = 2
        cas_strength = 0.3
        denoise_mode = "LIGHT"
    elif denoise_level == 3:
        threshold = 6
        cas_strength = 0.7
        denoise_mode = "VERY_STRONG"
    elif denoise_level == 4:
        threshold = 8
        cas_strength = 0.9
        denoise_mode = "ULTRA_STRONG"
    else:
        threshold = 4
        cas_strength = 0.5
        denoise_mode = "STRONG"
    
    # Query original video color metadata to preserve it
    color_metadata = get_video_color_metadata(input_path)
    processing_range_name, ffmpeg_color_range, range_reason = get_processing_color_range_with_reason(color_metadata)
    denoise_filter_body = f"vaguedenoiser=threshold={threshold}:method=soft:nsteps=5,cas=strength={cas_strength}"
    DENOISE_FILTER_FFMPEG = build_explicit_range_filter_chain(denoise_filter_body, processing_range_name)
    DENOISE_FILTER = f"Filters:[vaguedenoiser(threshold={threshold}, method=soft, nsteps=5) + cas(strength={cas_strength})]"
    
    # Choose codec: NVENC lossless or x264 lossless
    if use_nvenc:
        video_codec = ['-c:v', 'hevc_nvenc', '-preset', 'lossless']
    else:
        # Use FFV1 for CPU lossless - supports 10-bit natively (standard x264 is 8-bit only)
        video_codec = ['-c:v', 'ffv1', '-level', '3', '-threads', '0']
    
    # Build FFmpeg command with color metadata preservation
    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(input_path),
        '-vf', DENOISE_FILTER_FFMPEG,
        *video_codec,
    ]
    
    
    # Determine target pixel format.
    # Diagnostic mode can force 8-bit output to isolate player/range handling issues.
    source_pix_fmt = color_metadata.get('pix_fmt', '')
    target_pix_fmt = source_pix_fmt
    
    if source_pix_fmt:
        base_fmt = source_pix_fmt.lower()
        if force_8bit_master:
            if '444' in base_fmt:
                target_pix_fmt = 'yuv444p'
            elif '422' in base_fmt:
                target_pix_fmt = 'yuv422p'
            else:
                target_pix_fmt = 'yuv420p'
        elif '10le' not in base_fmt and '12le' not in base_fmt and '10bit' not in base_fmt:
            if '444' in base_fmt:
                target_pix_fmt = 'yuv444p10le'
            elif '422' in base_fmt:
                target_pix_fmt = 'yuv422p10le'
            else:
                # Default to 420p10le (covers yuv420p, yuvj420p, nv12, etc.)
                target_pix_fmt = 'yuv420p10le'
    else:
        # Fallback default
        target_pix_fmt = 'yuv420p' if force_8bit_master else 'yuv420p10le'

    # Apply pixel format
    cmd.extend(['-pix_fmt', target_pix_fmt])

    source_width = None
    source_height = None
    try:
        source_width, source_height = get_video_resolution(input_path)
    except Exception:
        source_width, source_height = None, None
    output_rotation = get_effective_output_rotation_degrees(input_path)
    output_dar = get_adjusted_display_aspect_ratio(
        color_metadata,
        width=source_width,
        height=source_height,
        rotation_degrees=output_rotation
    )
    if output_dar:
        cmd.extend(['-aspect', output_dar])
    
    # Preserve color space metadata
    if color_metadata['color_space'] or color_metadata['color_primaries'] or color_metadata['color_transfer']:
        if color_metadata['color_space']:
            cmd.extend(['-colorspace', color_metadata['color_space']])
        if color_metadata['color_primaries']:
            cmd.extend(['-color_primaries', color_metadata['color_primaries']])
        if color_metadata['color_transfer']:
            cmd.extend(['-color_trc', color_metadata['color_transfer']])
    
    cmd.extend(['-color_range', ffmpeg_color_range])
    
    # Preserve HDR metadata if present
    # CRITICAL: These options are ONLY supported by HEVC encoder, NOT by FFV1!
    # FFV1 does not support -master_display and -max_cll flags.
    if use_nvenc:
        if color_metadata.get('master_display'):
            cmd.extend(['-master_display', color_metadata['master_display']])
        if color_metadata.get('max_content') and color_metadata.get('max_average'):
            cmd.extend(['-max_cll', f"{color_metadata['max_content']},{color_metadata['max_average']}"])
    
    # Video stream only - no audio, subtitles, chapters, or metadata
    # The final encode step will copy non-video streams from the original source
    cmd.extend([
        '-an',                    # No audio
        '-sn',                    # No subtitles
        '-dn',                    # No data streams
        '-map', '0:v',            # Map only video stream
        '-map_chapters', '-1',    # Remove chapters
        '-map_metadata', '-1',    # Remove global metadata
        os.fspath(output_path)
    ])
    
    if logger:
        with console_redirect(logger):
            print(f"\n{'='*80}")
            print(f"🔇 DENOISED LOSSLESS MASTER (vaguedenoiser - {denoise_mode}): {input_path.name}")
            print(f"{'='*80}")
            print(f"Denoise Level: {denoise_mode} (threshold={threshold}, cas={cas_strength})")
            print(f"Denoised Master Output: {'8-bit test mode' if force_8bit_master else '10-bit default'}")
            log_color_range_debug(print, processing_range_name, range_reason, ffmpeg_color_range)
            print(f"Filter: {DENOISE_FILTER}")
            print(f"FFmpeg Filter: {DENOISE_FILTER_FFMPEG}")
            print(f"Codec: {'NVENC HEVC Lossless' if use_nvenc else 'FFV1 (Lossless)'}")
            print(f"Pixel Format: {target_pix_fmt} (source: {source_pix_fmt or 'unknown'})")
            print(f"Color Space: {color_metadata['color_space'] or 'auto'}")
            print(f"Color Primaries: {color_metadata['color_primaries'] or 'auto'}")
            print(f"Color Transfer: {color_metadata['color_transfer'] or 'auto'}")
            print(f"Color Range: {ffmpeg_color_range}")
            if color_metadata.get('master_display') or color_metadata.get('max_content'):
                print(f"HDR Master Display: {color_metadata.get('master_display') or 'N/A'}")
                print(f"HDR MaxCLL/MaxFALL: {color_metadata.get('max_content') or 'N/A'}/{color_metadata.get('max_average') or 'N/A'}")
            print(f"Output: {output_path.name}")
            
            # Print full command
            print(f"Command: {format_cmd_for_windows(cmd)}")
            print(f"{'='*80}\n")
    
    try:
        process = subprocess.Popen(
            cmd,
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
        
        # Process output and check for stop
        while True:
            if stop_event.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                # Clean up partial output
                if output_path.exists() and not DEBUG_MODE:
                    try:
                        output_path.unlink()
                    except OSError:
                        pass
                elif output_path.exists() and DEBUG_MODE:
                     if logger:
                        with console_redirect(logger):
                            print(f"  [STOP] DEBUG: Partial master KEPT: {output_path}")
                raise EncodingStopped()
            
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            
            if line:
                line = line.strip()
                if line:
                    # Parse FFmpeg progress output: frame=XXXX fps=XX speed=X.XXx
                    if progress_callback and line.startswith('frame='):
                        try:
                            # Parse frame count, fps, speed, and time
                            frame_match = re.search(r'frame=\s*(\d+)', line)
                            fps_match = re.search(r'fps=\s*([\d.]+)', line)
                            speed_match = re.search(r'speed=\s*([\d.]+)x', line)
                            time_match = re.search(r'time=\s*(\d{2}:\d{2}:\d{2}\.\d{2}|\d{2}:\d{2}:\d{2})', line)
                            
                            if frame_match:
                                current_frame = int(frame_match.group(1))
                                fps = float(fps_match.group(1)) if fps_match else 0
                                speed = float(speed_match.group(1)) if speed_match else 0
                                current_time_str = time_match.group(1) if time_match else None
                                progress_callback(current_frame, total_frames, fps, speed, current_time_str, total_duration_sec)
                        except (ValueError, AttributeError):
                            pass
                    
                    if logger:
                        with console_redirect(logger):
                            print(line)
        
        # Check result
        return_code = process.poll()
        
        if return_code == 0 and output_path.exists():
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            if logger:
                with console_redirect(logger):
                    print(f"\n[OK] Denoised master created: {output_path.name} ({file_size_mb:.1f} MB)")
            return True, DENOISE_FILTER
        else:
            if logger:
                with console_redirect(logger):
                    print(f"\n[ERROR] Denoised master failed (return code: {return_code})")
            # Clean up partial output
            if output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
            return False, ""
            
    except EncodingStopped:
        raise
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Exception during denoised master creation: {e}")
        # Clean up partial output
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        return False, ""
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if 'process' in locals() and process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)


def get_hybrid_paths(hybrid_base_path):
    r"""Get Hybrid component paths for VapourSynth SMDegrain.
    
    Args:
        hybrid_base_path: Path to Hybrid installation directory (e.g., C:\Program Files\Hybrid)
        
    Returns:
        dict: {'vsscripts': Path, 'vsPlugins': Path, 'vspipe': Path} or None if not found
    """
    try:
        base = Path(hybrid_base_path)
        if not base.exists():
            return None
        
        # Hybrid stores 64-bit scripts and plugins in 64bit subfolder
        vsscripts = base / '64bit' / 'vsscripts'
        
        # Collect all plugin folders (Standard + vsfilters subfolders)
        all_plugin_folders = []
        
        # 1. Standard vsPlugins
        p1 = base / '64bit' / 'vsPlugins'
        if p1.exists():
            all_plugin_folders.append(p1)
            
        # 2. vsfilters subfolders (Supported recursively)
        # Walk through vsfilters and find ALL folders containing DLLs
        vsfilters = base / '64bit' / 'vsfilters'
        if vsfilters.exists():
            try:
                for root, dirs, files in os.walk(vsfilters):
                    # Check if folder contains any DLL
                    if any(f.lower().endswith('.dll') for f in files):
                        all_plugin_folders.append(Path(root))
            except OSError:
                pass
                
        # Use first found as primary vsPlugins for legacy checks
        vsPlugins = all_plugin_folders[0] if all_plugin_folders else None

        # Check if essential paths exist
        if not vsscripts.exists():
            return None
        if not all_plugin_folders:
            return None
        
        # Search for vspipe.exe in multiple locations
        vspipe = None
        vspipe_search_paths = [
            # 1. Hybrid's own VapourSynth folder (most common for Hybrid users)
            base / '64bit' / 'vapoursynth' / 'vspipe.exe',
            base / '64bit' / 'VapourSynth' / 'vspipe.exe',
            # 2. User's local programs VapourSynth installation
            Path(os.environ.get('LOCALAPPDATA', '')) / 'Programs' / 'VapourSynth' / 'core' / 'vspipe.exe',
            # 3. System-wide VapourSynth installation
            Path(os.environ.get('PROGRAMFILES', '')) / 'VapourSynth' / 'core' / 'vspipe.exe',
        ]
        
        # Check each path
        for path in vspipe_search_paths:
            if path.exists():
                vspipe = path
                break
        
        # If not found, try Everything SDK for fast search
        if vspipe is None:
            try:
                from .everything_sdk import everything_find_file, is_everything_available
                if is_everything_available():
                    # Prefer Hybrid's vspipe.exe if multiple are found
                    found_path = everything_find_file('vspipe.exe', prefer_path_contains='Hybrid')
                    if found_path and found_path.exists():
                        vspipe = found_path
            except (ImportError, Exception):
                pass  # Everything SDK not available
        
        # Final check
        if vspipe is None:
            return None
        
        return {
            'vsscripts': vsscripts,
            'vsPlugins': vsPlugins,
            'all_plugin_folders': all_plugin_folders,
            'vspipe': vspipe
        }
    except Exception:
        return None


def generate_smdegrain_vpy(input_path, output_vpy_path, hybrid_paths, tr=2, thSAD=300, target_folder=None, color_metadata=None, deband_enabled=True, force_8bit_master=False):
    """Generate temporary VapourSynth script for SMDegrain denoising.
    
    Args:
        input_path: Path to the source video
        output_vpy_path: Path where to save the .vpy script
        hybrid_paths: Dict from get_hybrid_paths()
        tr: Temporal radius (2 = 2 frames forward/backward)
        thSAD: Denoising strength (200-300 for light, 400-600 for heavy)
        target_folder: Optional target folder for index files (.lwi)
        color_metadata: Optional dict with color_space, color_primaries, color_transfer
        deband_enabled: Whether to append neo_f3kdb debanding after SMDegrain
        force_8bit_master: Whether the output clip should be converted to 8-bit instead of 10-bit
        
    Returns:
        tuple: (bool, str) - (True if script was created successfully, filter_info_str)
    """
    try:
        # Escape backslashes for Python string in VPY
        # Prepare paths for VPY
        vsscripts_str = str(hybrid_paths['vsscripts']).replace('\\', '\\\\')
        # Serialize list of folders for VPY
        plugin_folders_json = json.dumps([str(p).replace('\\', '/') for p in hybrid_paths['all_plugin_folders']])
        input_str = str(input_path).replace('\\', '\\\\')
        
        # Determine cache file path (for .lwi index)
        input_path_obj = Path(input_path)
        if target_folder:
            cache_path = Path(target_folder) / f"{input_path_obj.stem}.lwi"
        else:
            cache_path = input_path_obj.parent / f"{input_path_obj.stem}.lwi"
        cache_str = str(cache_path).replace('\\', '\\\\')
        
        # Map FFmpeg color names to VapourSynth matrix names
        # FFmpeg: bt709, bt470bg, smpte170m, bt2020nc, etc.
        # VapourSynth: 709, 470bg, 170m, 2020ncl, etc.
        ffmpeg_to_vs_matrix = {
            'bt709': '709',
            'bt470bg': '470bg',
            'smpte170m': '170m',
            'smpte240m': '240m',
            'bt2020nc': '2020ncl',
            'bt2020_ncl': '2020ncl',
            'bt2020c': '2020cl',
            'bt2020_cl': '2020cl',
        }
        
        ffmpeg_to_vs_transfer = {
            'bt709': '709',
            'bt470m': '470m',
            'bt470bg': '470bg',
            'smpte170m': '601',
            'smpte240m': '240m',
            'linear': 'linear',
            'smpte2084': 'st2084',
            'arib-std-b67': 'arib-b67',
            'bt2020-10': '2020_10',
            'bt2020-12': '2020_12',
        }
        
        ffmpeg_to_vs_primaries = {
            'bt709': '709',
            'bt470m': '470m',
            'bt470bg': '470bg',
            'smpte170m': '170m',
            'smpte240m': '240m',
            'bt2020': '2020',
            'smpte428': 'st428',
            'smpte431': 'st431-2',
            'smpte432': 'st432-1',
        }
        
        # Determine color parameters conservatively.
        # If metadata is missing, avoid forcing bt709 into the resize path.
        matrix_s = None
        transfer_s = None
        primaries_s = None
        range_s = None
        
        if color_metadata:
            cs = color_metadata.get('color_space')
            if cs and cs in ffmpeg_to_vs_matrix:
                matrix_s = ffmpeg_to_vs_matrix[cs]
            
            ct = color_metadata.get('color_transfer')
            if ct and ct in ffmpeg_to_vs_transfer:
                transfer_s = ffmpeg_to_vs_transfer[ct]
            
            cp = color_metadata.get('color_primaries')
            if cp and cp in ffmpeg_to_vs_primaries:
                primaries_s = ffmpeg_to_vs_primaries[cp]
            elif cp:
                # Pass through unknown primaries as-is (let VapourSynth handle it)
                primaries_s = cp

            color_range = color_metadata.get('color_range')
            if color_range == 'pc':
                range_s = 'full'
            elif color_range == 'tv':
                range_s = 'limited'
        
        # Determine source format info for chroma subsampling preservation
        pix_fmt = color_metadata.get('pix_fmt', '') if color_metadata else ''
        if not range_s and isinstance(pix_fmt, str) and pix_fmt.lower().startswith('yuvj'):
            range_s = 'full'
        
        # Detect chroma subsampling from pix_fmt (yuv420p, yuv422p, yuv444p, etc.)
        # Default to 420 if unknown
        chroma_subsample = '420'
        if '444' in pix_fmt:
            chroma_subsample = '444'
        elif '422' in pix_fmt:
            chroma_subsample = '422'
        
        # Build VapourSynth format strings
        fmt_16bit = f'vs.YUV{chroma_subsample}P16'
        fmt_10bit = f'vs.YUV{chroma_subsample}P10'
        fmt_8bit = f'vs.YUV{chroma_subsample}P8'
        output_format_symbol = 'fmt_8' if force_8bit_master else 'fmt_10'

        resize_color_args_parts = []
        if matrix_s:
            resize_color_args_parts.append(f'matrix_in_s="{matrix_s}"')
            resize_color_args_parts.append(f'matrix_s="{matrix_s}"')
        if transfer_s:
            resize_color_args_parts.append(f'transfer_in_s="{transfer_s}"')
            resize_color_args_parts.append(f'transfer_s="{transfer_s}"')
        if primaries_s:
            resize_color_args_parts.append(f'primaries_in_s="{primaries_s}"')
            resize_color_args_parts.append(f'primaries_s="{primaries_s}"')
        if range_s:
            resize_color_args_parts.append(f'range_in_s="{range_s}"')
            resize_color_args_parts.append(f'range_s="{range_s}"')
        resize_color_args = ""
        if resize_color_args_parts:
            resize_color_args = ",\n        " + ",\n        ".join(resize_color_args_parts)

        # Deband tuning: keep VERY_STRONG denoise strength, but make post-deband less aggressive.
        deband_range = 15
        deband_y = 48
        deband_cb = 48
        deband_cr = 48
        if tr >= 3 or thSAD >= 450:
            deband_range = 12
            deband_y = 32
            deband_cb = 32
            deband_cr = 32

        deband_script = ""
        filter_info = f"Filters:[smdegrain.SMDegrain(clip, tr={tr}, thSAD={thSAD}, RefineMotion=True)]"
        if deband_enabled:
            deband_script = f"""
# Apply Debanding after denoising
clip = core.neo_f3kdb.Deband(clip, range={deband_range}, y={deband_y}, cb={deband_cb}, cr={deband_cr}, grainy=0, grainc=0, sample_mode=2)
"""
            filter_info = (
                f"Filters:[smdegrain.SMDegrain(clip, tr={tr}, thSAD={thSAD}, RefineMotion=True) + "
                f"core.neo_f3kdb.Deband(clip, range={deband_range}, y={deband_y}, cb={deband_cb}, cr={deband_cr}, grainy=0, grainc=0, sample_mode=2)]"
            )
        
        vpy_content = f'''import vapoursynth as vs
import sys
import os
import json

# Hybrid paths configuration
hybrid_scripts = r"{vsscripts_str}"
plugin_folders = {plugin_folders_json}

# Add scripts path so smdegrain.py can be imported
sys.path.append(hybrid_scripts)

import smdegrain
core = vs.core
# Set max cache to 4GB (avoids memory issues on 4K)
core.max_cache_size = 4096

# Load all plugins from all folders
for folder in plugin_folders:
    try:
        core.std.LoadAllPlugins(folder)
    except Exception:
        pass


# Input file
input_file = r"{input_str}"
cache_file = r"{cache_str}"

# Try lsmas first (better compatibility), fallback to ffms2
try:
    clip = core.lsmas.LWLibavSource(source=input_file, cachefile=cache_file)
except AttributeError:
    clip = core.ffms2.Source(source=input_file)

# Get source properties for smart format handling
src_format = clip.format
src_bits = src_format.bits_per_sample if src_format else 8
src_subsampling = (src_format.subsampling_w, src_format.subsampling_h) if src_format else (1, 1)

# Determine target 16-bit format matching source chroma subsampling
if src_subsampling == (0, 0):
    fmt_16 = vs.YUV444P16
    fmt_10 = vs.YUV444P10
    fmt_8 = vs.YUV444P8
elif src_subsampling == (1, 0):
    fmt_16 = vs.YUV422P16
    fmt_10 = vs.YUV422P10
    fmt_8 = vs.YUV422P8
else:
    fmt_16 = vs.YUV420P16
    fmt_10 = vs.YUV420P10
    fmt_8 = vs.YUV420P8

# Convert to 16-bit for processing (preserving chroma subsampling and color metadata)
if src_bits != 16 or clip.format.id != fmt_16.value:
    clip = core.resize.Bicubic(
        clip, 
        format=fmt_16{resize_color_args}
    )

# Apply SMDegrain
# tr: temporal radius (2 = consider 2 frames before and after)
# thSAD: motion detection threshold/denoising strength
# RefineMotion: True for better motion estimation (slower but better)
clip = smdegrain.SMDegrain(clip, tr={tr}, thSAD={thSAD}, RefineMotion=True)

{deband_script}

# Convert to output bit depth (10-bit default, 8-bit in test mode) - preserve color metadata and chroma subsampling
clip = core.resize.Bicubic(
    clip, 
    format={output_format_symbol}{resize_color_args}
)

clip.set_output()
'''
        
        # Write the VPY script
        with open(output_vpy_path, 'w', encoding='utf-8') as f:
            f.write(vpy_content)
        
        return True, filter_info
        
    except Exception as e:
        # Log error to help debugging
        try:
            from .core_preamble_and_imports import LOG_WRITER
            if LOG_WRITER:
                LOG_WRITER.write(f"[WARN] [generate_smdegrain_vpy] Error: {e}\n")
                LOG_WRITER.flush()
        except Exception:
            pass  # Silent fallback if LOG_WRITER not available
        return False, ""


def cleanup_smdegrain_files(input_path, vpy_path=None, logger=None, target_folder=None):
    """Clean up temporary files created by SMDegrain processing.
    
    Args:
        input_path: Path to the source video (for .lwi file)
        vpy_path: Path to the .vpy script file
        logger: Optional logger
        target_folder: Optional target folder where index files may have been created
        
    Returns:
        int: Number of files deleted
    """
    deleted = 0
    
    # Delete .vpy script
    if vpy_path:
        vpy_file = Path(vpy_path)
        if vpy_file.exists():
            try:
                vpy_file.unlink()
                deleted += 1
            except (OSError, PermissionError):
                pass
    
    # Delete .lwi index file (created by lsmas)
    # Check both source folder and target folder
    input_path = Path(input_path)
    stem = input_path.stem
    
    # Possible .lwi locations
    lwi_paths = [
        Path(str(input_path) + '.lwi'),  # Source folder (original behavior)
    ]
    if target_folder:
        lwi_paths.append(Path(target_folder) / f"{stem}.lwi")  # Target folder
    
    for lwi_path in lwi_paths:
        if lwi_path.exists():
            try:
                lwi_path.unlink()
                deleted += 1
            except (OSError, PermissionError):
                pass

    # Delete .ffindex file (created by ffms2 fallback)
    ffindex_paths = [
        Path(str(input_path) + '.ffindex'),  # Source folder
    ]
    if target_folder:
        ffindex_paths.append(Path(target_folder) / f"{stem}.ffindex")  # Target folder
    
    for ffindex_path in ffindex_paths:
        if ffindex_path.exists():
            try:
                ffindex_path.unlink()
                deleted += 1
            except (OSError, PermissionError):
                pass
    
    if deleted > 0 and logger:
        with console_redirect(logger):
            print(t('smdegrain_cleanup'))
    
    return deleted


def create_smdegrain_master(input_path, output_path, hybrid_base_path, use_nvenc=True, 
                            logger=None, stop_event=None,
                            progress_callback=None, total_frames=None, total_duration_sec=None,
                            denoise_level=1, deband_enabled=True, force_8bit_master=False):
    from .core_paths_tools_logging import EncodingStopped

    """Create a denoised lossless master using SMDegrain via VapourSynth.
    
    This function uses Hybrid's VapourSynth plugins to apply SMDegrain denoising,
    which provides significantly better quality than FFmpeg's vaguedenoiser.
    
    Args:
        input_path: Path to the source video file
        output_path: Path for the lossless output (e.g., video_denoised_master.mkv)
        hybrid_base_path: Path to Hybrid installation directory
        use_nvenc: True = use NVENC HEVC lossless, False = use x264 CRF 0 (CPU)
        logger: Optional logger for console output
        stop_event: Event to stop the process
        progress_callback: Optional callback(frame, total_frames, fps, speed) for progress display
        total_frames: Total frame count for percentage calculation
        denoise_level: 1 = strong (tr=2, thSAD=300), 2 = light (tr=1, thSAD=150),
            3 = very strong (tr=3, thSAD=450), 4 = ultra strong (tr=4, thSAD=600). Default: 1.
        deband_enabled: Whether to apply post-denoise debanding in the temporary master.
        force_8bit_master: If True, create an 8-bit denoised master for playback diagnostics.
        
    Returns:
        tuple: (bool, str) - (True if successful, denoise_params_str)
    """
    # Determine SMDegrain parameters based on denoise_level
    if denoise_level == 2:
        # Light denoising - less aggressive
        tr = 1
        thSAD = 150
        denoise_mode = "LIGHT"
    elif denoise_level == 3:
        # Very strong denoising
        tr = 3
        thSAD = 450
        denoise_mode = "VERY_STRONG"
    elif denoise_level == 4:
        # Ultra strong denoising - most aggressive
        tr = 4
        thSAD = 600
        denoise_mode = "ULTRA_STRONG"
    else:
        # Strong denoising (default)
        tr = 2
        thSAD = 300
        denoise_mode = "STRONG"
    
    if stop_event is None:
        from .core_paths_tools_logging import STOP_EVENT
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        raise EncodingStopped()
    
    input_path = Path(input_path)
    output_path = Path(output_path)
    vpy_path = None
    
    try:
        # Get Hybrid paths
        hybrid_paths = get_hybrid_paths(hybrid_base_path)
        if not hybrid_paths:
            if logger:
                with console_redirect(logger):
                    print(t('smdegrain_fallback'))
            return False, ""
        
        # Query source color metadata for preservation
        color_metadata = get_video_color_metadata(input_path)
        processing_range_name, ffmpeg_color_range, range_reason = get_processing_color_range_with_reason(color_metadata)
        
        if logger:
            with console_redirect(logger):
                print(f"\n{'='*80}")
                print(f"🔇 SMDegrain MASTER ({denoise_mode}): {input_path.name}")
                print(f"{'='*80}")
                print(f"Source: {input_path.name}")
                print(f"Denoise Level: {denoise_mode}")
                print(f"SMDegrain: tr={tr}, thSAD={thSAD}")
                print(f"Deband: {'enabled' if deband_enabled else 'disabled'}")
                print(f"Denoised Master Output: {'8-bit test mode' if force_8bit_master else '10-bit default'}")
                log_color_range_debug(print, processing_range_name, range_reason, ffmpeg_color_range)
                print(f"Codec: {'NVENC HEVC Lossless' if use_nvenc else 'FFV1 (Lossless)'}")
                print(f"Pixel Format: {color_metadata.get('pix_fmt') or 'unknown'} -> {'8-bit' if force_8bit_master else '10-bit'} output")
                print(f"Color Space: {color_metadata.get('color_space') or 'bt709 (default)'}")
                print(f"Color Primaries: {color_metadata.get('color_primaries') or 'bt709 (default)'}")
                print(f"Color Transfer: {color_metadata.get('color_transfer') or 'bt709 (default)'}")
                print(f"Color Range: {ffmpeg_color_range}")
                if color_metadata.get('master_display') or color_metadata.get('max_content'):
                    print(f"HDR Master Display: {color_metadata.get('master_display') or 'N/A'}")
                    print(f"HDR MaxCLL/MaxFALL: {color_metadata.get('max_content') or 'N/A'}/{color_metadata.get('max_average') or 'N/A'}")
                print(f"{'='*80}\n")
        
        # Generate VPY script in target folder (output_path.parent)
        vpy_path = output_path.parent / f"{input_path.stem}_smdegrain.vpy"
        vpy_success, denoise_info = generate_smdegrain_vpy(
            input_path,
            vpy_path,
            hybrid_paths,
            tr=tr,
            thSAD=thSAD,
            target_folder=output_path.parent,
            color_metadata=color_metadata,
            deband_enabled=deband_enabled,
            force_8bit_master=force_8bit_master,
        )
        if not vpy_success:
            if logger:
                with console_redirect(logger):
                    print(t('smdegrain_vpy_error').format(error='Failed to generate VPY script'))
            return False, ""
        
        # Build vspipe command
        vspipe_cmd = [
            str(hybrid_paths['vspipe']),
            '-c', 'y4m',
            str(vpy_path),
            '-'
        ]
        
        # Build FFmpeg command
        # Choose codec
        if use_nvenc:
            video_codec_args = ['-c:v', 'hevc_nvenc', '-preset', 'lossless']
        else:
            # Use FFV1 for CPU lossless - supports 10-bit natively
            video_codec_args = ['-c:v', 'ffv1', '-level', '3', '-threads', '0']
        
        # Build color metadata args for FFmpeg output
        color_args = []
        if color_metadata.get('color_space'):
            color_args.extend(['-colorspace', color_metadata['color_space']])
        if color_metadata.get('color_primaries'):
            color_args.extend(['-color_primaries', color_metadata['color_primaries']])
        if color_metadata.get('color_transfer'):
            color_args.extend(['-color_trc', color_metadata['color_transfer']])
        color_args.extend(['-color_range', ffmpeg_color_range])

        # Determine target pixel format (Must match VapourSynth output which is forced to 10-bit)
        source_pix_fmt = color_metadata.get('pix_fmt', '')
        target_pix_fmt = 'yuv420p' if force_8bit_master else 'yuv420p10le' # Default
        
        if source_pix_fmt:
             if '444' in source_pix_fmt:
                 target_pix_fmt = 'yuv444p' if force_8bit_master else 'yuv444p10le'
             elif '422' in source_pix_fmt:
                 target_pix_fmt = 'yuv422p' if force_8bit_master else 'yuv422p10le'
        
        # Add pixel format to Ensure 10-bit encoding
        color_args.extend(['-pix_fmt', target_pix_fmt])
        
        # Add HDR metadata if present (for HDR10 content)
        # CRITICAL: These options are ONLY supported by HEVC encoder, NOT by FFV1!
        # FFV1 does not support -master_display and -max_cll flags.
        if use_nvenc:
            if color_metadata.get('master_display'):
                # Note: Mastering display metadata for HEVC
                color_args.extend(['-master_display', color_metadata['master_display']])
            if color_metadata.get('max_content') and color_metadata.get('max_average'):
                # MaxCLL (max content light level) and MaxFALL (max frame average light level)
                color_args.extend(['-max_cll', f"{color_metadata['max_content']},{color_metadata['max_average']}"])

        # VapourSynth/lsmas does NOT handle Display Matrix rotation metadata.
        # Phone videos store pixels in landscape with a rotation tag (e.g. -90 = portrait).
        # We must apply rotation manually via transpose filter on the pipe output.
        rotation = get_effective_output_rotation_degrees(input_path)
        source_width = None
        source_height = None
        try:
            source_width, source_height = get_video_resolution(input_path)
        except Exception:
            source_width, source_height = None, None
        output_dar = get_adjusted_display_aspect_ratio(
            color_metadata,
            width=source_width,
            height=source_height,
            rotation_degrees=rotation
        )
        if output_dar:
            color_args.extend(['-aspect', output_dar])

        rotate_args = []
        filter_steps = [
            f"scale=iw:ih:in_range={processing_range_name}:out_range={processing_range_name}"
        ]
        if rotation == -90 or rotation == 270:
            filter_steps.append('transpose=1')
        elif rotation == 90 or rotation == -270:
            filter_steps.append('transpose=2')
        elif abs(rotation) == 180:
            filter_steps.extend(['transpose=1', 'transpose=1'])
        filter_steps.append(f"scale=iw:ih:in_range={processing_range_name}:out_range={processing_range_name}")
        rotate_args = ['-vf', ",".join(filter_steps)]
        if logger:
            with console_redirect(logger):
                print(f"[INFO] Processing Range: {processing_range_name}->{processing_range_name}")
                print(f"[INFO] Output Color Range Metadata: {ffmpeg_color_range}")

        ffmpeg_cmd = [
            FFMPEG_PATH,
            '-y',
            '-i', 'pipe:',                    # Video from vspipe (y4m format)
            # NO second input - master file is video-only
            '-map', '0:v',                    # Video from pipe (denoised)
            *rotate_args,                     # Rotation fix (if source has Display Matrix)
            # NO audio, subtitles, attachments - these will come from original source at final encode
            *video_codec_args,
            *color_args,                      # Preserve color metadata
            # Explicitly exclude non-video streams
            '-an',                            # No audio
            '-sn',                            # No subtitles
            '-dn',                            # No data streams
            os.fspath(output_path)
        ]
        
        # Run vspipe | ffmpeg
        if logger:
            with console_redirect(logger):
                # Print full commands for debugging
                print(f"\n[RUN] VSPIPE CMD: {format_cmd_for_windows(vspipe_cmd)}")
                
                print(f"[RUN] FFMPEG CMD (pipe input): {format_cmd_for_windows(ffmpeg_cmd)}\n")
        
        managed_processes = []

        def _register_managed_process(process):
            if process is None:
                return
            managed_processes.append(process)
            with ACTIVE_PROCESSES_LOCK:
                ACTIVE_PROCESSES.append(process)

        def _unregister_managed_process(process):
            if process is None:
                return
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)

        # Start vspipe process
        vspipe_process = subprocess.Popen(
            vspipe_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            startupinfo=get_startup_info()
        )
        _register_managed_process(vspipe_process)

        # Thread to capture vspipe stderr without blocking / deadlocking
        vspipe_errors = []
        def read_vspipe_stderr():
            try:
                if vspipe_process.stderr:
                    for line in vspipe_process.stderr:
                        decoded_line = line.decode('utf-8', errors='replace').strip()
                        if decoded_line:
                            vspipe_errors.append(decoded_line)
                            # Log immediately for real-time debug
                            if logger: 
                                with console_redirect(logger): 
                                    print(f"📝 VSPIPE LOG: {decoded_line}")
            except Exception:
                pass

        stderr_thread = threading.Thread(target=read_vspipe_stderr, daemon=True)
        stderr_thread.start()
        
        # Start ffmpeg process with vspipe output as input
        ffmpeg_process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=vspipe_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            startupinfo=get_startup_info()
        )
        _register_managed_process(ffmpeg_process)
        
        # Allow vspipe to receive SIGPIPE if ffmpeg exits
        vspipe_process.stdout.close()
        
        # Monitor FFmpeg output
        while True:
            if stop_event.is_set():
                ffmpeg_process.terminate()
                vspipe_process.terminate()
                try:
                    ffmpeg_process.wait(timeout=5)
                    vspipe_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    ffmpeg_process.kill()
                    vspipe_process.kill()
                # Clean up
                cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except OSError:
                        pass
                raise EncodingStopped()
            
            line = ffmpeg_process.stdout.readline()
            if not line and ffmpeg_process.poll() is not None:
                break
            
            if line:
                line = line.strip()
                if line:
                    # Parse FFmpeg progress output: frame=XXXX fps=XX speed=X.XXx
                    if progress_callback and line.startswith('frame='):
                        try:
                            # Parse frame count, fps, speed, and time
                            frame_match = re.search(r'frame=\s*(\d+)', line)
                            fps_match = re.search(r'fps=\s*([\d.]+)', line)
                            speed_match = re.search(r'speed=\s*([\d.]+)x', line)
                            time_match = re.search(r'time=\s*(\d{2}:\d{2}:\d{2}\.\d{2}|\d{2}:\d{2}:\d{2})', line)
                            
                            if frame_match:
                                current_frame = int(frame_match.group(1))
                                fps = float(fps_match.group(1)) if fps_match else 0
                                speed = float(speed_match.group(1)) if speed_match else 0
                                current_time_str = time_match.group(1) if time_match else None
                                progress_callback(current_frame, total_frames, fps, speed, current_time_str, total_duration_sec)
                        except (ValueError, AttributeError):
                            pass
                    
                    if logger:
                        with console_redirect(logger):
                            print(line)
        
        # Wait for both processes
        ffmpeg_return = ffmpeg_process.wait()
        vspipe_return = vspipe_process.wait()
        
        # Wait for stderr reader to finish
        stderr_thread.join(timeout=1.0)
        
        # Check for vspipe errors
        vspipe_had_errors = False
        if vspipe_return != 0 or vspipe_errors:
            # Combine errors
            error_msg = "\n".join(vspipe_errors)
            if logger and error_msg:
                with console_redirect(logger):
                    print(t('smdegrain_vspipe_error').format(error=error_msg[:1000])) # Show up to 1000 chars

            if vspipe_return != 0:
                # vspipe failed - but check if ffmpeg still produced usable output
                if ffmpeg_return == 0 and output_path.exists() and output_path.stat().st_size > 0:
                    # ffmpeg succeeded despite vspipe errors (e.g. lsmas failed on last 1-2 frames)
                    # Treat as partial success - the master is usable but needs_check should be set
                    vspipe_had_errors = True
                    if logger:
                        with console_redirect(logger):
                            print(f"[WARN] vspipe hibával lépett ki (rc={vspipe_return}), de az ffmpeg sikeresen befejezte a master fájlt - részleges siker")
                else:
                    cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
                    return False, ""

        # Check result
        if ffmpeg_return == 0 and output_path.exists():
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            if logger:
                with console_redirect(logger):
                    print(f"\n{t('smdegrain_master_created')}: {output_path.name} ({file_size_mb:.1f} MB)")

            # Clean up temp files
            cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
            # Return 'partial' instead of True when vspipe had errors but output is usable
            # 'partial' is truthy so existing boolean checks still work correctly
            return ('partial' if vspipe_had_errors else True), denoise_info
        else:
            if logger:
                with console_redirect(logger):
                    print(f"\n[ERROR] SMDegrain master failed (ffmpeg rc: {ffmpeg_return})")
                    if not vspipe_errors and vspipe_return == 0:
                        print(f"  (No vspipe errors detected, but output file missing or ffmpeg failed)")
            cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
            if output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
            return False, ""
        
    except EncodingStopped:
        cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
        raise
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] SMDegrain exception: {e}")
        cleanup_smdegrain_files(input_path, vpy_path, target_folder=output_path.parent)
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        return False, ""
    finally:
        for process_name in ('ffmpeg_process', 'vspipe_process'):
            process = locals().get(process_name)
            if process is None:
                continue
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
            except Exception:
                pass
            try:
                if getattr(process, 'stdout', None):
                    process.stdout.close()
            except Exception:
                pass
            try:
                if getattr(process, 'stderr', None):
                    process.stderr.close()
            except Exception:
                pass
            _unregister_managed_process(process)


def extract_audio_tracks_with_metadata(source_path, output_dir, logger=None, stop_event=None):
    """Extract all audio tracks from source file to separate .mka files.
    
    Preserves all metadata (language, title, default, forced flags).
    
    Args:
        source_path: Path to source video file.
        output_dir: Directory to save extracted audio files.
        logger: Optional logger for console output.
        stop_event: Event to stop the process.
        
    Returns:
        list: List of dicts with extracted audio info:
              [{'path': Path, 'index': int, 'language': str, 'title': str, 
                'default': bool, 'forced': bool, 'codec': str, 'channels': int}, ...]
              Empty list on failure.
    """
    if stop_event is None:
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        raise EncodingStopped()
    
    source_path = Path(source_path)
    output_dir = Path(output_dir)
    
    if not source_path.exists():
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Source file not found: {source_path}")
        return []
    
    # Get audio stream details with dispositions
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index,codec_name,channels,channel_layout,start_time',
            '-show_entries', 'stream_tags=language,title',
            '-show_entries', 'stream_disposition=default,forced',
            '-of', 'json',
            os.fspath(source_path)
        ]
        if logger:
            with console_redirect(logger):
                print(f"[SCAN] FFprobe audio streams: {format_cmd_for_windows(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', 
                               errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        streams = data.get('streams', [])
        
        if not streams:
            if logger:
                with console_redirect(logger):
                    print(f"[INFO] No audio streams found in {source_path.name}")
            return []
            
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] FFprobe audio info error: {e}")
        return []
    
    extracted_tracks = []
    base_name = source_path.stem
    
    for audio_idx, stream in enumerate(streams):
        if stop_event.is_set():
            # Cleanup already extracted files
            for track in extracted_tracks:
                if track['path'].exists() and not DEBUG_MODE:
                    try:
                        track['path'].unlink()
                    except OSError:
                        pass
            raise EncodingStopped()
        
        tags = stream.get('tags', {})
        disposition = stream.get('disposition', {})
        
        track_info = {
            'index': audio_idx,
            'source_stream_index': stream.get('index'),
            'language': tags.get('language', '') or 'und',
            'title': tags.get('title', ''),
            'default': disposition.get('default', 0) == 1,
            'forced': disposition.get('forced', 0) == 1,
            'codec': stream.get('codec_name', 'unknown'),
            'channels': stream.get('channels', 0),
            'channel_layout': stream.get('channel_layout', ''),
        }

        start_time_str = stream.get('start_time', '0')
        try:
            start_time_sec = float(start_time_str) if start_time_str and str(start_time_str).lower() not in ('n/a', 'unknown') else 0.0
        except (TypeError, ValueError):
            start_time_sec = 0.0
        start_time_ms = int(round(start_time_sec * 1000.0))
        track_info['start_time_sec'] = start_time_sec
        track_info['start_time_ms'] = start_time_ms
        track_info['user_offset_ms'] = start_time_ms
        
        # Generate unique filename
        output_file = output_dir / f"{base_name}_audio_{audio_idx}.mka"
        track_info['path'] = output_file
        
        # Extract audio track
        cmd = [
            FFMPEG_PATH,
            '-y',
            '-i', os.fspath(source_path),
            '-map', f'0:a:{audio_idx}',
            '-c:a', 'copy',
            os.fspath(output_file)
        ]
        
        if logger:
            with console_redirect(logger):
                print(f"🎵 Extracting audio track {audio_idx}: {track_info['language']} | {track_info['codec']} | {track_info['channels']}ch")
                print(f"   -> {output_file.name}")
        
        try:
            process = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8',
                                    errors='replace', timeout=600, startupinfo=get_startup_info())
            
            if process.returncode != 0:
                if logger:
                    with console_redirect(logger):
                        print(f"[ERROR] Failed to extract audio track {audio_idx}: {process.stderr[:200]}")
                continue
                
            if not output_file.exists() or output_file.stat().st_size == 0:
                if logger:
                    with console_redirect(logger):
                        print(f"[ERROR] Extracted audio file is empty or missing: {output_file.name}")
                continue
                
            extracted_tracks.append(track_info)
            
        except subprocess.TimeoutExpired:
            if logger:
                with console_redirect(logger):
                    print(f"[ERROR] Audio extraction timeout for track {audio_idx}")
            continue
        except Exception as e:
            if logger:
                with console_redirect(logger):
                    print(f"[ERROR] Audio extraction error for track {audio_idx}: {e}")
            continue
    
    if logger:
        with console_redirect(logger):
            print(f"[OK] Extracted {len(extracted_tracks)} audio track(s)")
    
    return extracted_tracks


def get_embedded_subtitle_info(source_path, logger=None):
    """Get metadata for embedded subtitle tracks using FFprobe.

    Args:
        source_path: Path to the source video file.
        logger: Optional logger for warnings.

    Returns:
        list: List of dicts [{'language': str, 'title': str, 'codec': str, ...}, ...] for each subtitle stream.
              Empty list if no subtitles or error.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 's',
            '-show_entries', 'stream=index,codec_name',
            '-show_entries', 'stream_tags=language,title',
            '-show_entries', 'stream_disposition=default,forced',
            '-of', 'json',
            os.fspath(source_path)
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8',
                               errors='replace', timeout=600, startupinfo=get_startup_info())

        if result.returncode != 0:
            return []

        data = json.loads(result.stdout)
        streams = data.get('streams', [])

        sub_info = []
        # FFprobe returns streams in order, which matches -map 0:s? order
        for stream in streams:
            tags = stream.get('tags', {})
            disposition = stream.get('disposition', {})
            codec_name = stream.get('codec_name', 'unknown')
            info = {
                'language': tags.get('language'),
                'title': tags.get('title'),
                'codec': codec_name,
                'default': disposition.get('default', 0) == 1,
                'forced': disposition.get('forced', 0) == 1
            }
            sub_info.append(info)

        return sub_info

    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Failed to get subtitle info: {e}")
        return []


def get_attachment_info(source_path, logger=None):
    """Get information about attachment streams (fonts, images, etc.).
    
    This function queries all attachment streams and their metadata,
    which is critical for preserving embedded fonts in subtitles.
    
    Args:
        source_path: Path to the source video file.
        logger: Optional logger for debug output.
        
    Returns:
        list: List of dicts [{'index': int, 'filename': str or None, 'mimetype': str}, ...]
              Empty list if no attachments or error.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 't',  # t = attachment streams
            '-show_entries', 'stream=index',
            '-show_entries', 'stream_tags=filename,mimetype',
            '-of', 'json',
            os.fspath(source_path)
        ]
        
        result = subprocess.run(
            cmd, capture_output=True, text=True, encoding='utf-8',
            errors='replace', timeout=600, startupinfo=get_startup_info()
        )
        
        if result.returncode != 0:
            return []
        
        data = json.loads(result.stdout)
        streams = data.get('streams', [])
        
        attachment_info = []
        for idx, stream in enumerate(streams):
            tags = stream.get('tags', {})
            # CRITICAL: Use the REAL FFmpeg stream index from FFprobe, NOT enumerate index!
            # FFprobe returns the actual stream index (e.g., 5, 6, 7 for attachments)
            # enumerate would give us 0, 1, 2 which is WRONG for -map commands
            stream_index = stream.get('index', idx)  # Fallback to enumerate if 'index' missing
            info = {
                'index': stream_index,  # Real FFmpeg stream index
                'filename': tags.get('filename'),  # May be None if missing!
                'mimetype': tags.get('mimetype', 'application/octet-stream')
            }
            attachment_info.append(info)
        
        return attachment_info
    
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Failed to get attachment info: {e}")
        return []


def generate_attachment_filename(index, mimetype):
    """Generate a fallback filename for attachments without filename tag.
    
    This is required because Matroska format mandates a filename tag
    for all attachment streams. Without it, muxing will fail.
    
    Args:
        index: Attachment stream index.
        mimetype: MIME type of the attachment (e.g., 'application/x-truetype-font').
        
    Returns:
        str: Generated filename (e.g., 'attachment_0.ttf').
    """
    # MIME type -> extension mapping
    mime_to_ext = {
        'application/x-truetype-font': 'ttf',
        'application/vnd.ms-opentype': 'otf',
        'application/x-font-ttf': 'ttf',
        'application/font-sfnt': 'ttf',
        'font/ttf': 'ttf',
        'font/otf': 'otf',
        'font/sfnt': 'ttf',
        'font/woff': 'woff',
        'font/woff2': 'woff2',
        'image/jpeg': 'jpg',
        'image/png': 'png',
        'image/bmp': 'bmp',
        'image/gif': 'gif',
    }
    
    ext = mime_to_ext.get(mimetype, 'dat')  # default: .dat (generic data)
    return f"attachment_{index}.{ext}"


def merge_video_audio_subtitles(video_path, audio_tracks, source_path, output_path,
                                 logger=None, stop_event=None, external_subtitle_files=None):
    """Merge video-only file with extracted audio tracks and source subtitles/metadata.

    This approach avoids interleave issues by building the file from scratch.

    Args:
        video_path: Path to video-only file (AV1 encoded).
        audio_tracks: List of audio track dicts from extract_audio_tracks_with_metadata().
        source_path: Original source file for subtitles, attachments, chapters, metadata.
        output_path: Path for final merged output file.
        logger: Optional logger for console output.
        stop_event: Event to stop the process.
        external_subtitle_files: Optional list of (Path, language_code) tuples for external subtitles.

    Returns:
        bool: True if successful, False otherwise.
    """
    if stop_event is None:
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        raise EncodingStopped()
    
    video_path = Path(video_path)
    source_path = Path(source_path)
    output_path = Path(output_path)
    
    if not video_path.exists():
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Video file not found: {video_path}")
        return False
    
    if not source_path.exists():
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Source file not found: {source_path}")
        return False
    
    # Build FFmpeg command
    cmd = [FFMPEG_PATH, '-y']

    # Query original source stream start_times for sync preservation
    # Both denoised video AND extracted audio tracks start at 0 (timestamps lost during extraction)
    # We need to offset each stream to match their original start_time values
    source_metadata = get_video_color_metadata(source_path)
    video_start_time = source_metadata.get('video_start_time', 0.0) or 0.0
    audio_start_times = source_metadata.get('audio_start_times', [])

    # Log stream offsets if any are non-zero
    has_nonzero_times = abs(video_start_time) > 0.0005 or any(abs(t) > 0.0005 for t in audio_start_times)
    if has_nonzero_times and logger:
        with console_redirect(logger):
            print(f"📐 Stream offsets from source: video={video_start_time}s, audio={audio_start_times}")

    # Input 0: Video-only file (with original start_time offset if needed)
    # This ensures denoised video (which starts at 0) aligns with original timestamps
    if abs(video_start_time) > 0.0005:
        cmd.extend(['-itsoffset', f'{video_start_time:.3f}'])
    cmd.extend(['-i', os.fspath(video_path)])

    # Inputs 1..N: Audio tracks (each with their original start_time offset)
    # Extracted audio files start at 0, we restore original timestamps with -itsoffset
    added_audio_indices = []
    for track_idx, track in enumerate(audio_tracks):
        if not track['path'].exists():
            if logger:
                with console_redirect(logger):
                    print(f"[WARN] Audio track file missing, skipping: {track['path']}")
            continue
        track_offset_sec = None
        if track.get('user_offset_ms') is not None:
            try:
                track_offset_sec = float(track.get('user_offset_ms')) / 1000.0
            except (TypeError, ValueError):
                track_offset_sec = None
        if track_offset_sec is None and track.get('start_time_ms') is not None:
            try:
                track_offset_sec = float(track.get('start_time_ms')) / 1000.0
            except (TypeError, ValueError):
                track_offset_sec = None
        if track_offset_sec is None and track.get('start_time_sec') is not None:
            try:
                track_offset_sec = float(track.get('start_time_sec'))
            except (TypeError, ValueError):
                track_offset_sec = None
        if track_offset_sec is None and track_idx < len(audio_start_times):
            track_offset_sec = audio_start_times[track_idx]
        if track_offset_sec is None:
            track_offset_sec = 0.0

        if abs(track_offset_sec) > 0.0005:
            cmd.extend(['-itsoffset', f'{track_offset_sec:.3f}'])
            if logger:
                with console_redirect(logger):
                    print(f"[INFO] Restoring audio track {track_idx} offset: {track_offset_sec:.3f}s")
        cmd.extend(['-i', os.fspath(track['path'])])
        added_audio_indices.append(track_idx)

    # Input N+1: Source file (for subtitles, attachments, chapters, metadata)
    source_input_idx = 1 + len(added_audio_indices)
    cmd.extend(['-i', os.fspath(source_path)])

    # Inputs N+2..M: External subtitle files (with encoding detection and offset)
    ext_sub_start_idx = source_input_idx + 1
    if external_subtitle_files:
        for subtitle_path, _ in external_subtitle_files:
            # External subtitles: Apply offset 0 (extracted files start at 0)
            # Offset will be restored when remuxing with source
            # NOTE: No explicit -itsoffset needed since extracted subtitles start at 0
            # and source metadata is preserved via -map_metadata
            
            # Detect and specify source encoding for non-UTF-8 subtitle files
            detected_encoding = detect_subtitle_encoding(subtitle_path)
            if detected_encoding != 'UTF-8':
                cmd.extend(['-sub_charenc', detected_encoding])
                if logger:
                    with console_redirect(logger):
                        print(f"📝 External subtitle encoding detected: {subtitle_path.name} -> {detected_encoding}")
            cmd.extend(['-i', os.fspath(subtitle_path)])

    # Mappings
    # Video from input 0
    cmd.extend(['-map', '0:v:0'])
    
    # Audio from inputs 1..N (only the ones actually added)
    for input_idx in range(len(added_audio_indices)):
        cmd.extend(['-map', f'{input_idx + 1}:a:0'])

    # Subtitles from source (embedded) - check offset compatibility
    # Query subtitle start_times from source for validation
    subtitle_start_times = source_metadata.get('subtitle_start_times', [])
    
    # Check if any embedded subtitle has different offset than video
    # FFmpeg limitation: embedded subtitles from same input inherit that input's -itsoffset
    # Cannot apply different offset to individual embedded subtitle streams in copy mode
    if subtitle_start_times:
        for idx, sub_start_time in enumerate(subtitle_start_times):
            if abs(sub_start_time - video_start_time) > 0.001:  # Tolerance for floating point
                if logger:
                    with console_redirect(logger):
                        print(f"[WARN] Embedded subtitle stream {idx}: offset {sub_start_time:.3f}s differs from video offset {video_start_time:.3f}s")
                        print(f"  Embedded subtitles inherit source input offset (FFmpeg limitation in copy mode)")
                        print(f"  Subtitle may be out of sync after merge!")
    
    cmd.extend(['-map', f'{source_input_idx}:s?'])

    # External subtitles (map each individually)
    if external_subtitle_files:
        for idx in range(len(external_subtitle_files)):
            ext_input_idx = ext_sub_start_idx + idx
            cmd.extend(['-map', f'{ext_input_idx}:0'])

    # Attachments from source (WITH intelligent filename fix)
    # Critical: Matroska format REQUIRES a filename tag for all attachments.
    # Some files have attachments (embedded fonts) without filename tags, causing muxing to fail.
    # We query attachments, check for missing filenames, and auto-generate them if needed.
    try:
        attachment_info = get_attachment_info(source_path, logger)
        
        if attachment_info:
            if logger:
                with console_redirect(logger):
                    print(f"📎 Found {len(attachment_info)} attachment(s) in source")
            
            # Map each attachment individually using ABSOLUTE STREAM INDEX
            # CRITICAL: FFmpeg -map syntax for attachments is: -map input:stream_index
            # NOT -map input:t:stream_index (that syntax doesn't exist!)
            # The stream index is the ABSOLUTE index from FFprobe (e.g., 6, 7, 8)
            for att in attachment_info:
                # Use absolute stream index (e.g., -map 4:6 for Stream #4:6)
                cmd.extend(['-map', f'{source_input_idx}:{att["index"]}'])
            
            # Apply filename metadata for each attachment
            for output_idx, att in enumerate(attachment_info):
                # Use existing filename or generate one
                filename = att['filename']
                if not filename:
                    filename = generate_attachment_filename(output_idx, att['mimetype'])
                    if logger:
                        with console_redirect(logger):
                            print(f"[WARN] Attachment {output_idx} missing filename tag, using: {filename}")
                
                # Set filename metadata (REQUIRED by Matroska)
                cmd.extend([f'-metadata:s:t:{output_idx}', f'filename={filename}'])
                
                # Preserve mimetype if available
                if att['mimetype']:
                    cmd.extend([f'-metadata:s:t:{output_idx}', f'mimetype={att["mimetype"]}'])
        else:
            # No attachments found, or query failed
            # Use fallback simple mapping (may fail for files with missing filename tags)
            cmd.extend(['-map', f'{source_input_idx}:t?'])
            
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Attachment metadata error (using fallback mapping): {e}")
        # Fallback to simple mapping (may fail for files with missing filenames)
        cmd.extend(['-map', f'{source_input_idx}:t?'])
    
    # EXPLICITLY PRESERVE SUBTITLE METADATA (Titles, Languages)
    # This ensures track names are preserved correctly by reading them from source
    # and applying them explicitly to the output streams.
    try:
        sub_info = get_embedded_subtitle_info(source_path, logger)
        for i, sub in enumerate(sub_info):
            # i is the output subtitle stream index (since we map all subs in order)
            
            # Language
            if sub.get('language'):
                 cmd.extend([f'-metadata:s:s:{i}', f"language={sub['language']}"])
            
            # Title
            if sub.get('title'):
                 cmd.extend([f'-metadata:s:s:{i}', f"title={sub['title']}"])
            
            # APPPLY Disposition flags (default, forced) for subtitles
            # Just like audio tracks, we must be explicit
            disp_flags = []
            if sub.get('default'):
                disp_flags.append('default')
            if sub.get('forced'):
                disp_flags.append('forced')
            
            if disp_flags:
                cmd.extend([f'-disposition:s:{i}', '+'.join(disp_flags)])
            else:
                # Explicitly clear flags if none set (to avoid defaults creeping in)
                cmd.extend([f'-disposition:s:{i}', '0'])
                 
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[WARN] Error setting subtitle metadata: {e}")

    # External subtitle metadata (language, title)
    if external_subtitle_files:
        # Get embedded subtitle count (reuse from codec section if possible)
        embedded_subtitle_count = 0
        try:
            sub_info = get_embedded_subtitle_info(source_path, logger)
            embedded_subtitle_count = len(sub_info)
        except Exception:
            embedded_subtitle_count = 0

        for idx, (subtitle_path, lang_part) in enumerate(external_subtitle_files):
            # Normalize language code (e.g., 'eng' -> 'en', 'hu' -> 'hu')
            iso_lang = normalize_language_code(lang_part)

            # External subtitles stream index follows embedded ones
            external_subtitle_stream_idx = embedded_subtitle_count + idx

            # Language metadata
            cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'language={iso_lang}'])

            # Title metadata (preserve original format: 'hu' -> 'HU', 'en-US' -> 'en-US')
            if lang_part:
                title = lang_part if '-' in lang_part else lang_part.upper()
                cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'title={title}'])

    # Chapters and metadata from source
    cmd.extend(['-map_chapters', str(source_input_idx)])
    cmd.extend(['-map_metadata', str(source_input_idx)])
    
    # CRITICAL: Copy video stream metadata from SOURCE (including outdated statistics)
    # The user explicitly requested to keep ALL source metadata, even if it contradicts the new stream properties.
    cmd.extend(['-map_metadata:s:v:0', f'{source_input_idx}:s:v:0'])
    
    # CRITICAL: Re-apply the Settings tag (CRF/VMAF/Params) which lives in the video-only input (0)
    # We extract it from input 0 and apply it manually to both Global and Stream metadata
    # This ensures we have the source metadata + the new encoding settings (overwriting old Settings)
    settings_tag = extract_settings_from_file(video_path)
    if settings_tag:
        # Apply to Global metadata
        cmd.extend(['-metadata', f'Settings={settings_tag}'])
        # Apply to Video Stream metadata
        cmd.extend(['-metadata:s:v:0', f'Settings={settings_tag}'])
    
    # EXTRA METADATA FIX: Extract and copy ALL global tags from source
    # User requested NO filtering (even stats tags), except ensuring new Settings are not overwritten.
    try:
        source_global_tags = extract_all_global_tags(source_path)
        # We MUST exclude 'Settings' here because we set the NEW Settings tag above.
        # If we included it here, the OLD Settings from source would overwrite the NEW ones (as this comes later in cmd).
        for key, value in source_global_tags.items():
            if key == 'Settings':
                continue
            cmd.extend(['-metadata', f'{key}={value}'])
    except Exception:
        pass

    # Stream copy for video, audio, attachments
    cmd.extend(['-c:v', 'copy'])
    cmd.extend(['-c:a', 'copy'])
    cmd.extend(['-c:t', 'copy'])  # Attachments (fonts, images) - copy without re-encoding

    # SUBTITLE CODEC HANDLING (stream-specific with UTF-8 protection)
    # Get embedded subtitle count and codec info
    embedded_subtitle_count = 0
    embedded_sub_info = []
    try:
        embedded_sub_info = get_embedded_subtitle_info(source_path, logger)
        embedded_subtitle_count = len(embedded_sub_info)
    except Exception:
        embedded_subtitle_count = 0

    # EMBEDDED SUBTITLES: Set codec per-stream based on detected codec
    # Text-based codecs must be re-encoded to guarantee UTF-8 encoding (prevents character corruption)
    # Bitmap codecs (PGS, VOBSUB) can be safely copied
    for idx, sub in enumerate(embedded_sub_info):
        codec = sub.get('codec', 'unknown').lower()

        # Define text-based subtitle codecs that need re-encoding for UTF-8 safety
        TEXT_BASED_CODECS = {
            'subrip', 'srt', 'ass', 'ssa', 'webvtt', 'mov_text', 'text',
            'subviewer', 'microdvd', 'subviewer1', 'mpl2', 'pjs', 'vtt'
        }

        # Define bitmap subtitle codecs that should be copied (no encoding issues)
        BITMAP_CODECS = {
            'hdmv_pgs_subtitle', 'dvd_subtitle', 'dvdsub', 'pgssub', 'vobsub'
        }

        if codec in TEXT_BASED_CODECS:
            # Text-based: re-encode to respective codec (guarantees UTF-8 in MKV container)
            # Map common codecs to FFmpeg encoder names
            codec_encoder_map = {
                'subrip': 'srt',
                'srt': 'srt',
                'ass': 'ass',
                'ssa': 'ass',
                'webvtt': 'webvtt',
                'vtt': 'webvtt',
                'mov_text': 'mov_text'
            }
            encoder = codec_encoder_map.get(codec, 'srt')  # Default to SRT if unknown text codec
            cmd.extend([f'-c:s:{idx}', encoder])

            if logger:
                with console_redirect(logger):
                    print(f"📝 Embedded subtitle {idx} ({codec}) -> re-encoding as {encoder} (UTF-8 safe)")

        elif codec in BITMAP_CODECS:
            # Bitmap: safe to copy (no text encoding issues)
            cmd.extend([f'-c:s:{idx}', 'copy'])

            if logger:
                with console_redirect(logger):
                    print(f"🖼 Embedded subtitle {idx} ({codec}) -> copy (bitmap)")

        else:
            # Unknown codec: copy mode (conservative approach)
            cmd.extend([f'-c:s:{idx}', 'copy'])

            if logger:
                with console_redirect(logger):
                    print(f"[WARN] Embedded subtitle {idx} ({codec}) -> copy (unknown codec)")

    # EXTERNAL SUBTITLES: Convert to appropriate codec based on file extension
    if external_subtitle_files:
        for idx, (subtitle_path, lang_part) in enumerate(external_subtitle_files):
            external_subtitle_stream_idx = embedded_subtitle_count + idx

            # Detect codec from file extension
            suffix = subtitle_path.suffix.lower()
            codec_map = {
                '.srt': 'srt',
                '.ass': 'ass',
                '.ssa': 'ass',
                '.vtt': 'webvtt',
                '.sub': 'microdvd'
            }
            encoder = codec_map.get(suffix, 'srt')  # Default to SRT

            cmd.extend([f'-c:s:{external_subtitle_stream_idx}', encoder])

            if logger:
                with console_redirect(logger):
                    print(f"📎 External subtitle {idx} ({subtitle_path.name}) -> {encoder} (UTF-8 safe)")

    # Audio stream metadata (language, title, dispositions)
    # Apply metadata to output audio streams in the order they were added
    for output_idx, original_idx in enumerate(added_audio_indices):
        track = audio_tracks[original_idx]
        
        # Language
        if track['language']:
            cmd.extend([f'-metadata:s:a:{output_idx}', f"language={track['language']}"])
        
        # Title
        if track['title']:
            cmd.extend([f'-metadata:s:a:{output_idx}', f"title={track['title']}"])
        
        # Disposition flags
        disp_flags = []
        if track['default']:
            disp_flags.append('default')
        if track['forced']:
            disp_flags.append('forced')
        
        if disp_flags:
            cmd.extend([f'-disposition:a:{output_idx}', '+'.join(disp_flags)])
        elif track['default'] is False:
            # Explicitly set no default if not default
            cmd.extend([f'-disposition:a:{output_idx}', '0'])
    
    # Output
    cmd.append(os.fspath(output_path))
    
    if logger:
        with console_redirect(logger):
            print(f"\n{'='*80}")
            print(f"[TOOL] MERGE VIDEO + AUDIO + SUBTITLES: {output_path.name}")
            print(f"{'='*80}")
            print(f"Video source: {video_path.name}")
            print(f"Audio tracks: {len(added_audio_indices)}")
            print(f"Source (subs/meta): {source_path.name}")
            if external_subtitle_files:
                print(f"External subtitles: {len(external_subtitle_files)}")
                for sub_path, lang in external_subtitle_files:
                    lang_display = f" [{lang}]" if lang else ""
                    print(f"  • {sub_path.name}{lang_display}")
            print(f"Command: {format_cmd_for_windows(cmd)}")
            print(f"{'='*80}\n")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            universal_newlines=True,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        # Gyűjtjük az ÖSSZES FFmpeg kimenetet
        ffmpeg_output_lines = []
        
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise EncodingStopped()
                
                # Minden sort eltárolunk
                ffmpeg_output_lines.append(line.strip())
                
                # Log errors/warnings azonnal
                if 'Error' in line or 'Warning' in line or 'error' in line:
                    if logger:
                        with console_redirect(logger):
                            print(line.strip())
        
        except EncodingStopped:
            raise
        except Exception as e:
            if logger:
                with console_redirect(logger):
                    print(f"Error reading merge output: {e}")
        
        process.wait()
        
        success = process.returncode == 0
        
        if success and output_path.exists() and output_path.stat().st_size > 0:
            if logger:
                with console_redirect(logger):
                    print(f"[OK] Merge successful: {output_path.name}")
            return True
        else:
            # Hiba esetén részletes jelentés
            import ctypes
            # Signed integer konverzió (4294967274 -> -22)
            signed_rc = ctypes.c_int32(process.returncode).value
            
            if logger:
                with console_redirect(logger):
                    print(f"\n{'='*80}")
                    print(f"[ERROR] Összefűzés sikertelen!")
                    print(f"{'='*80}")
                    print(f"Hibakód: {signed_rc} (unsigned: {process.returncode})")
                    
                    # Utolsó 100 sor kiírása a naplóba (vagy kevesebb, ha nincs 100)
                    if ffmpeg_output_lines:
                        lines_to_show = min(100, len(ffmpeg_output_lines))
                        print(f"\nFFmpeg kimenet (utolsó {lines_to_show} sor):")
                        print("-" * 80)
                        for line in ffmpeg_output_lines[-lines_to_show:]:
                            print(line)
                        print("-" * 80)
                    else:
                        print("\n(Nincs FFmpeg kimenet)")
                    
                    print(f"{'='*80}\n")
            
            return False
            
    except FileNotFoundError:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] FFmpeg not found at {FFMPEG_PATH}")
        return False
    except EncodingStopped:
        raise
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"[ERROR] Merge exception: {e}")
        return False
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if 'process' in locals() and process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)


def cleanup_extracted_audio_files(audio_tracks, logger=None):
    """Delete extracted audio track files.
    
    Args:
        audio_tracks: List of audio track dicts with 'path' key.
        logger: Optional logger for console output.
    """
    if DEBUG_MODE:
        if logger:
            with console_redirect(logger):
                print(f"[STOP] DEBUG: Keeping {len(audio_tracks)} extracted audio file(s)")
        return
    
    deleted_count = 0
    for track in audio_tracks:
        if 'path' in track and track['path'].exists():
            try:
                track['path'].unlink()
                deleted_count += 1
            except OSError as e:
                if logger:
                    with console_redirect(logger):
                        print(f"[WARN] Failed to delete audio file: {track['path'].name} - {e}")
    
    if logger and deleted_count > 0:
        with console_redirect(logger):
            print(f"[DEL] Deleted {deleted_count} temporary audio file(s)")


def get_video_track_size_bytes(video_path):
    """Calculate total size of all video streams in bytes using FFprobe packet scan.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        int: Total video size in bytes, or 0 if calculation fails.
    """
    # User explicitly requested to NEVER use metadata and ALWAYS use exact packet scan.
    # This is slower but guarantees accuracy even with corrupt/outdated metadata.
    return get_video_stream_size_bytes_exact(video_path)
