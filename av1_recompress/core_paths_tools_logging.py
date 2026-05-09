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
import threading
import queue
import time
from datetime import datetime
import json
from contextlib import contextmanager

# === PATH HINTS ÉS DINAMIKUS MEHAJTÓFELISMERÉS ===
# Preferált keresési helyek programonként (relatív utak!)
# Ezeket az összes elérhető meghajtóhoz hozzáfűzzük
PREFERRED_PATHS = {
    'virtualdub': [
        'VirtualDub2',
        'VirtualDub2_v2.4',
        'VirtualDub2_v2.5',
        'Program Files\\VirtualDub2',
        'Program Files (x86)\\VirtualDub2',
    ],
    'ffmpeg': [
        'ffmpeg',
        'FFmpeg',
        'Program Files\\ffmpeg',
    ],
    'abav1': [
        'ab-av1',
        'ab_av1',
    ],
    'hybrid': [
        'Program Files\\Hybrid',
        'Hybrid',
    ],
}


def _get_available_drives():
    """Összes elérhető meghajtó lekérdezése Windows-on.
    
    Returns:
        List[Path]: Elérhető meghajtók listája
    """
    if sys.platform != 'win32':
        return [Path('/')]
    
    drives = []
    try:
        import ctypes
        bitmask = ctypes.windll.kernel32.GetLogicalDrives()
        for letter in range(26):
            if bitmask & (1 << letter):
                drive = f"{chr(65 + letter)}:\\"
                if os.path.exists(drive):
                    drives.append(Path(drive))
    except Exception:
        # Fallback: SystemDrive
        system_drive = os.environ.get('SystemDrive', 'C:')
        drives.append(Path(system_drive))
    
    return drives


def _get_preferred_search_paths(program):
    """Preferált keresési helyek generálása programhoz.
    
    Args:
        program: Program neve (pl. 'virtualdub', 'ffmpeg')
        
    Returns:
        List[Path]: Teljes elérési utak az összes elérhető meghajtón
    """
    paths = []
    hints = PREFERRED_PATHS.get(program, [])
    
    if sys.platform == 'win32':
        drives = _get_available_drives()
        for drive in drives:
            for hint in hints:
                paths.append(drive / hint)
    else:
        # Linux/macOS
        for hint in hints:
            paths.append(Path('/opt') / hint)
    
    return paths


# Import global variables from preamble
from .core_preamble_and_imports import (
    LOG_WRITER, 
    VIDEO_LOADING_LOG, 
    LOAD_DEBUG, 
    DEBUG_MODE,
    ACTIVE_PROCESSES,
    ACTIVE_PROCESSES_LOCK,
    STOP_EVENT,
    GUI_INSTANCE,
    FFMPEG_PATH,
    FFPROBE_PATH,
    ABAV1_PATH,
    VDUB2_PATH,
    DEFAULT_FFMPEG,
    DEFAULT_FFPROBE,
    DEFAULT_ABAV1
)

from . import core_preamble_and_imports

def get_log_writer():
    """Dynamically get the LOG_WRITER from the authoritative core module.
    
    The LOG_WRITER is set in app.py as core.LOG_WRITER = log_file.
    Due to how fragments are loaded and aliased, the local LOG_WRITER
    variable in this module is a stale copy (None). We MUST always
    fetch from sys.modules['av1_recompress.core'] to get the actual value.
    """
    try:
        if 'av1_recompress.core' in sys.modules:
            core_mod = sys.modules['av1_recompress.core']
            return getattr(core_mod, 'LOG_WRITER', None)
    except Exception:
        pass
    return None


_LOG_ICON_REPLACEMENTS = {
    '\U0001f3ac': '[INFO]',
    '\U0001f4dd': '[INFO]',
    '\U0001f4ca': '[STATS]',
    '\U0001f50d': '[SCAN]',
    '\u26a0': '[WARN]',
    '\u2705': '[OK]',
    '\u274c': '[ERROR]',
    '\U0001f4c2': '[DIR]',
    '\U0001f4c1': '[DIR]',
    '\U0001f534': '[STOPPING]',
    '\U0001f512': '[LOCK]',
    '\u23f1': '[TIME]',
    '\u23f3': '[WAIT]',
    '\u26a1': '[INFO]',
    '\u2022': '-',
    '\u2500': '-',
    '\u25b6': '[CONTINUE]',
    '\u23f8': '[PAUSE]',
    '\u23f9': '[STOP]',
    '\u2728': '*',
    '\U0001f507': '[AUDIO]',
    '\U0001f4fd': '[VIDEO]',
    '\U0001f4d0': '[SUB]',
    '\U0001f5bc': '[SUB]',
}

_LOG_MOJIBAKE_MARKERS = ('Ã', 'Â', 'Ä', 'Ă', 'â', 'đ', 'Ë', '\ufffd')
_LOG_MOJIBAKE_REPLACEMENTS = {
    '\u0111\u0178\u017d\u00ac': '[INFO]',
    '\u0111\u0178\u201c\u00a5': '[INFO]',
    '\u0111\u0178\u201c\u201a': '[DIR]',
    '\u0111\u0178\u201d\u2014': '[AUDIO]',
    '\u0111\u0178\u017e\u00bd': '[STEP]',
    '\u0111\u0178\u0178\u2019': '[SUB]',
    '\u00e2\u20ac\u02d8\u00a2': '-',
    '\u00e2\u20ac\u201d': '-',
    '\u00e2\u20ac\u201c': '-',
    '\u00e2\u20ac\u2122': "'",
    '\u00e2\u20ac\u02dc': "'",
    '\u00e2\u20ac\u0153': '"',
    '\u00e2\u20ac\ufffd': '"',
    '\u00e2\u017d\u00b1': '[TIME]',
    '\u00e2\u0161\u02c7': '[INFO]',
    '\u00e2\u0161\u2122': '[INFO]',
}

_VDUB_CACHE = {'value': None, 'checked_at': 0.0}
_VDUB_POSITIVE_TTL = 3600.0
_VDUB_NEGATIVE_TTL = 120.0


def _suspicious_log_score(text):
    if not text:
        return 0
    score = 0
    for marker in _LOG_MOJIBAKE_MARKERS:
        score += text.count(marker)
    return score


def sanitize_log_text(text):
    """Normalize log text to stable UTF-8 friendly ASCII-prefixed output."""
    if text is None:
        return ""
    value = str(text)
    if not value:
        return value

    value = value.replace('\ufeff', '')
    for src, dst in _LOG_ICON_REPLACEMENTS.items():
        value = value.replace(src, dst)
    for src, dst in _LOG_MOJIBAKE_REPLACEMENTS.items():
        value = value.replace(src, dst)

    if any(marker in value for marker in _LOG_MOJIBAKE_MARKERS):
        best = value
        best_score = _suspicious_log_score(best)
        for encoding in ('cp1252', 'cp1250', 'latin1'):
            try:
                candidate = value.encode(encoding, errors='ignore').decode('utf-8', errors='ignore')
            except Exception:
                continue
            if not candidate:
                continue
            score = _suspicious_log_score(candidate)
            if score < best_score:
                best = candidate
                best_score = score
        value = best
        for src, dst in _LOG_MOJIBAKE_REPLACEMENTS.items():
            value = value.replace(src, dst)
        for src, dst in _LOG_ICON_REPLACEMENTS.items():
            value = value.replace(src, dst)

    value = value.replace('\ufffd', '?')
    return value


def _get_dynamic_vdub2_path():
    """Read current VDUB2_PATH from authoritative core module."""
    try:
        core_mod = sys.modules.get('av1_recompress.core')
        if core_mod is None:
            return None
        vdub_path = getattr(core_mod, 'VDUB2_PATH', None)
        if not vdub_path:
            return None
        vdub_path_obj = vdub_path if isinstance(vdub_path, Path) else Path(vdub_path)
        if vdub_path_obj.exists():
            return os.fspath(vdub_path_obj)
    except Exception:
        pass
    return None


def detect_nvidia_gpu():
    """Detect NVIDIA GPU and check for NVENC support (40xx/50xx series).
    
    Checks if an NVIDIA GPU is present using nvidia-smi and verifies
    if it supports NVENC encoding (specifically targeting RTX 40xx/50xx series).
    
    Returns:
        tuple: (bool, str) - (True if supported GPU found, GPU name or None)
    """
    
    def log(msg):
        """Safe log writing"""
        writer = get_log_writer()
        if writer:
            try:
                writer.write(msg + "\n")
                writer.flush()
            except (OSError, IOError, AttributeError) as e:
                # Log writing failed - try stderr as fallback
                try:
                    sys.stderr.write(f"Log write failed: {e}\n")
                except Exception:
                    pass
    
    log("\n=== NVIDIA GPU DETECTION ===")
    
    try:
        log("  Running nvidia-smi command...")
        # Run nvidia-smi command
        result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                              capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and result.stdout.strip():
            gpu_name = result.stdout.strip()
            log(f"  [OK] GPU found: {gpu_name}")
            # Check if it is 40xx or 50xx series
            # RTX 40xx: "RTX 40" or "GeForce RTX 40" or "RTX 4090", "RTX 4080", etc.
            # RTX 50xx: "RTX 50" or "GeForce RTX 50" or "RTX 5090", "RTX 5080", etc.
            gpu_parts = gpu_name.split()
            last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
            if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                # Check more precisely: 40xx or 50xx
                parts = gpu_name.split()
                for part in parts:
                    if part.startswith('40') and len(part) >= 3:  # 4090, 4080, etc.
                        log(f"  [OK] 40xx series detected: {gpu_name}")
                        log("  [OK] NVENC enabled (40xx GPU)\n")
                        return True, gpu_name
                    if part.startswith('50') and len(part) >= 3:  # 5090, 5080, etc.
                        log(f"  [OK] 50xx series detected: {gpu_name}")
                        log("  [OK] NVENC enabled (50xx GPU)\n")
                        return True, gpu_name
            log(f"  [ERROR] GPU is not 40xx or 50xx series: {gpu_name}")
            log("  [ERROR] NVENC not enabled\n")
            return False, gpu_name
        else:
            log("  [ERROR] nvidia-smi returned no result")
    except FileNotFoundError:
        log("  [ERROR] nvidia-smi not found in PATH")
    except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
        log(f"  [ERROR] nvidia-smi error: {e}")
    except Exception as e:
        log(f"  [ERROR] nvidia-smi unexpected error: {e}")
    
    # If nvidia-smi is not available, try Windows WMI (optional)
    if sys.platform == 'win32':
        try:
            log("  Attempting Windows WMI...")
            try:
                import wmi
            except ImportError:
                log("  [ERROR] wmi module not installed")
            else:
                log("  [OK] wmi module available")
                c = wmi.WMI()
                for gpu in c.Win32_VideoController():
                    if 'NVIDIA' in gpu.Name.upper():
                        gpu_name = gpu.Name
                        log(f"  [OK] NVIDIA GPU found: {gpu_name}")
                        # Check if it is 40xx or 50xx series
                        gpu_parts = gpu_name.split()
                        last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
                        if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                            parts = gpu_name.split()
                            for part in parts:
                                if part.startswith('40') and len(part) >= 3:
                                    log(f"  [OK] 40xx series detected: {gpu_name}")
                                    log("  [OK] NVENC enabled (40xx GPU)\n")
                                    return True, gpu_name
                                if part.startswith('50') and len(part) >= 3:
                                    log(f"  [OK] 50xx series detected: {gpu_name}")
                                    log("  [OK] NVENC enabled (50xx GPU)\n")
                                    return True, gpu_name
                        log(f"  [ERROR] GPU is not 40xx or 50xx series: {gpu_name}")
                        log("  [ERROR] NVENC not enabled\n")
                        return False, gpu_name
        except Exception as e:
            log(f"  [ERROR] WMI error: {e}")
    
    log("  [ERROR] NVIDIA GPU not found or not 40xx/50xx series")
    log("  [ERROR] NVENC not enabled\n")
    return False, None

def find_program_in_path(program_name):
    """Search for a program in the system PATH and common locations.
    
    Args:
        program_name: Name of the program to find (e.g., 'ffmpeg.exe').
        
    Returns:
        str: Full path to the program or None if not found.
    """
    def log(msg):
        """Safe log writing"""
        writer = get_log_writer()
        if writer:
            try:
                writer.write(msg + "\n")
                writer.flush()
            except (OSError, IOError, AttributeError):
                pass
    
    log(f"\n=== Searching for {program_name} ===")
    
    # First try in PATH
    log(f"  Checking PATH...")
    try:
        result = subprocess.run(['where' if sys.platform == 'win32' else 'which', program_name], 
                              capture_output=True, text=True, timeout=2)
        if result.returncode == 0:
            stdout_lines = result.stdout.strip().split('\n')
            if stdout_lines and stdout_lines[0]:
                path = stdout_lines[0]
                if path and Path(path).exists():
                    log(f"  [OK] FOUND IN PATH: {path}")
                    return path
        log(f"  [ERROR] Not found in PATH")
    except Exception as e:
        log(f"  [ERROR] PATH check error: {e}")
    
    # Mapping a végrehajtható fájlnévről a program nevére
    program_map = {
        'vdub64.exe': 'virtualdub',
        'vdub2.exe': 'virtualdub',
        'ffmpeg.exe': 'ffmpeg',
        'ffprobe.exe': 'ffprobe',
        'ab-av1.exe': 'abav1',
    }
    program_key = program_map.get(program_name, program_name.replace('.exe', ''))
    
    # Preferált helyek ellenőrzése (DINAMIKUS MEHAJTÓKKAL!)
    log(f"  Checking preferred locations...")
    preferred_paths = _get_preferred_search_paths(program_key)
    
    for path in preferred_paths:
        exe_path = path / program_name
        if exe_path.exists():
            log(f"    [OK] FOUND: {exe_path}")
            return str(exe_path)
        # bin almappa is lehet
        exe_path = path / 'bin' / program_name
        if exe_path.exists():
            log(f"    [OK] FOUND in bin: {exe_path}")
            return str(exe_path)
    
    # 3. VirtualDub2 specifikus: dinamikus meghajtó bejárás
    if program_name in ('vdub64.exe', 'vdub2.exe'):
        log(f"  Scanning all drives for VirtualDub* folders...")
        for drive in _get_available_drives():
            try:
                log(f"    Checking {drive}...")
                items_found = []
                for item in drive.iterdir():
                    if item.is_dir() and item.name.lower().startswith('virtualdub'):
                        items_found.append(item.name)
                        prog_path = item / program_name
                        if prog_path.exists():
                            log(f"      [OK] FOUND: {prog_path}")
                            return str(prog_path)
                if not items_found:
                    log(f"      [ERROR] No VirtualDub* folder found")
            except (PermissionError, OSError) as e:
                log(f"      [ERROR] Access error: {e}")
                continue
        
        log(f"  Checking execution directory...")
        cwd = Path.cwd()
        log(f"    Current directory: {cwd}")
        
        # Determine application base directory (EXE dir if frozen, else script dir)
        app_base = cwd
        if getattr(sys, 'frozen', False):
            app_base = Path(sys.executable).resolve().parent
            log(f"    frozen app detected, base dir: {app_base}")
        else:
            # Try to get script directory if possible
            try:
                import av1_recompress.core
                if hasattr(av1_recompress.core, 'APP_ROOT') and av1_recompress.core.APP_ROOT:
                    app_base = av1_recompress.core.APP_ROOT
                    log(f"    script app detected, base dir: {app_base}")
            except (ImportError, AttributeError):
                pass

        paths_to_check = [cwd]
        if app_base != cwd:
            paths_to_check.append(app_base)

        for search_dir in paths_to_check:
            if not search_dir.exists():
                continue
                
            log(f"  Scanning directory: {search_dir}")
            
            # 1. Check directly in this directory
            direct_path = search_dir / program_name
            if direct_path.exists():
                log(f"    [OK] FOUND directly: {direct_path}")
                return str(direct_path)
            
            # 2. Check in subfolders pattern
            prefix_map = {
                'ffmpeg.exe': ['ffmpeg*', 'FFmpeg*'],
                'vdub64.exe': ['virtualdub*', 'VirtualDub*'],
                'vdub2.exe': ['virtualdub*', 'VirtualDub*'],
                'ab-av1.exe': ['ab_av1*', 'ab-av1*'],
            }
            
            if program_name in prefix_map:
                patterns = prefix_map[program_name]
                log(f"    Search patterns: {patterns}")
                for pattern in patterns:
                    try:
                        found_folders = list(search_dir.glob(pattern))
                        if found_folders:
                            log(f"      Found folders with '{pattern}' pattern: {[f.name for f in found_folders]}")
                        for folder_path in found_folders:
                            if folder_path.is_dir():
                                # Directly in the folder
                                prog_path = folder_path / program_name
                                log(f"        Checking: {prog_path}")
                                if prog_path.exists():
                                    log(f"        [OK] FOUND: {prog_path}")
                                    return str(prog_path)
                                # In bin subfolder
                                prog_path = folder_path / 'bin' / program_name
                                log(f"        Checking: {prog_path}")
                                if prog_path.exists():
                                    log(f"        [OK] FOUND: {prog_path}")
                                    return str(prog_path)
                    except Exception as e:
                        log(f"      [ERROR] Error searching for '{pattern}': {e}")
        
    log(f"  [ERROR] {program_name} not found anywhere\n")
    return None

def find_virtualdub():
    """Search for VirtualDub2 - vdub64.exe or vdub2.exe"""
    def log(msg):
        """Safe log writing"""
        writer = get_log_writer()
        if writer:
            try:
                writer.write(msg + "\n")
                writer.flush()
            except (OSError, IOError, AttributeError):
                pass

    # Fast path: honor runtime-configured path first.
    dynamic_path = _get_dynamic_vdub2_path()
    if dynamic_path:
        _VDUB_CACHE['value'] = dynamic_path
        _VDUB_CACHE['checked_at'] = time.time()
        return dynamic_path

    # Cache to avoid repeated multi-drive scans on each task.
    now = time.time()
    cached_value = _VDUB_CACHE.get('value')
    checked_at = float(_VDUB_CACHE.get('checked_at') or 0.0)
    ttl = _VDUB_POSITIVE_TTL if cached_value else _VDUB_NEGATIVE_TTL
    if checked_at > 0 and (now - checked_at) < ttl:
        return cached_value

    log("\n=== Searching for VirtualDub2 (vdub64.exe or vdub2.exe) ===")
    
    # First try vdub64.exe
    log("  Searching for vdub64.exe...")
    result = find_program_in_path('vdub64.exe')
    if result:
        log(f"  [OK] FOUND: {result}")
        _VDUB_CACHE['value'] = result
        _VDUB_CACHE['checked_at'] = time.time()
        return result
    
    # If not found, try vdub2.exe
    log("  vdub64.exe not found, searching for vdub2.exe...")
    result = find_program_in_path('vdub2.exe')
    if result:
        log(f"  [OK] FOUND: {result}")
        _VDUB_CACHE['value'] = result
        _VDUB_CACHE['checked_at'] = time.time()
        return result
    
    log("  [ERROR] VirtualDub2 not found (neither vdub64.exe nor vdub2.exe)\n")
    _VDUB_CACHE['value'] = None
    _VDUB_CACHE['checked_at'] = time.time()
    return None

def auto_detect_programs():
    """Automatically detect external programs (FFmpeg, VirtualDub2, ab-av1).
    
    Searches for required external tools in the PATH and common locations.
    
    Returns:
        dict: Dictionary containing paths for 'ffmpeg', 'virtualdub', and 'abav1'.
    """
    return {
        'ffmpeg': find_program_in_path('ffmpeg.exe' if sys.platform == 'win32' else 'ffmpeg'),
        'virtualdub': find_virtualdub(),
        'abav1': find_program_in_path('ab-av1.exe' if sys.platform == 'win32' else 'ab-av1'),
    }

def apply_external_tool_paths(ffmpeg_path=None, abav1_path=None, virtualdub_path=None):
    """Set global tool paths based on provided arguments.
    
    Updates the global variables for external tool paths (FFmpeg, FFprobe, ab-av1, VirtualDub).
    If a path is provided, it validates and sets the corresponding global variable.
    FFprobe path is automatically derived from FFmpeg path if possible.
    
    Args:
        ffmpeg_path: Path to FFmpeg executable (optional).
        abav1_path: Path to ab-av1 executable (optional).
        virtualdub_path: Path to VirtualDub2 executable (optional).
        
    Returns:
        None. Modifies global variables FFMPEG_PATH, FFPROBE_PATH, ABAV1_PATH, VDUB2_PATH.
    """
    global FFMPEG_PATH, FFPROBE_PATH, ABAV1_PATH, VDUB2_PATH

    # Detect locally
    if ffmpeg_path:
        ffmpeg_path = os.fspath(ffmpeg_path)
        FFMPEG_PATH = ffmpeg_path
        ffmpeg_path_obj = Path(ffmpeg_path)
        ffprobe_name = 'ffprobe.exe' if ffmpeg_path_obj.suffix.lower() == '.exe' else 'ffprobe'
        ffprobe_candidate = ffmpeg_path_obj.with_name(ffprobe_name)
        if ffprobe_candidate.exists():
            FFPROBE_PATH = os.fspath(ffprobe_candidate)
        else:
            FFPROBE_PATH = DEFAULT_FFPROBE
    else:
        FFMPEG_PATH = FFMPEG_PATH or DEFAULT_FFMPEG
        FFPROBE_PATH = FFPROBE_PATH or DEFAULT_FFPROBE

    if abav1_path:
        ABAV1_PATH = os.fspath(abav1_path)
    else:
        ABAV1_PATH = DEFAULT_ABAV1

    if virtualdub_path:
        vdub_path = Path(virtualdub_path)
        VDUB2_PATH = vdub_path if vdub_path.exists() else None
    else:
        VDUB2_PATH = None
        
    # --- PROPER GLOBALS UPDATE ---
    # Since other modules import these variables using 'from ... import ...',
    # we must update them safely across all modules that use them.
    from . import core_preamble_and_imports
    core_preamble_and_imports.FFMPEG_PATH = FFMPEG_PATH
    core_preamble_and_imports.FFPROBE_PATH = FFPROBE_PATH
    core_preamble_and_imports.ABAV1_PATH = ABAV1_PATH
    core_preamble_and_imports.VDUB2_PATH = VDUB2_PATH
    
    # Update core_audio_video_ops (critical for probing!)
    try:
        from . import core_audio_video_ops
        core_audio_video_ops.FFMPEG_PATH = FFMPEG_PATH
        core_audio_video_ops.FFPROBE_PATH = FFPROBE_PATH
    except ImportError:
        pass
        
    # Update core_metrics_and_validation (critical for VMAF/Encoding)
    try:
        from . import core_metrics_and_validation
        core_metrics_and_validation.FFMPEG_PATH = FFMPEG_PATH
        core_metrics_and_validation.FFPROBE_PATH = FFPROBE_PATH
        core_metrics_and_validation.ABAV1_PATH = ABAV1_PATH
    except ImportError:
        pass

    # Update core_subtitles_and_metadata
    try:
        from . import core_subtitles_and_metadata
        core_subtitles_and_metadata.FFMPEG_PATH = FFMPEG_PATH
        core_subtitles_and_metadata.FFPROBE_PATH = FFPROBE_PATH
    except ImportError:
        pass
        
    # Update gui_track_editor
    try:
        from . import gui_track_editor
        gui_track_editor.FFMPEG_PATH = FFMPEG_PATH
        gui_track_editor.FFPROBE_PATH = FFPROBE_PATH
    except ImportError:
        pass

def _str_to_bool(value):
    """Convert string or environment variable value to boolean.
    
    Args:
        value: String value to convert (typically from environment variable).
        
    Returns:
        bool: True if value is "1", "true", "yes", or "on" (case-insensitive),
              False otherwise (including None).
    """
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")

# Video loading log file
VIDEO_LOADING_LOG_LOCK = threading.Lock()

def init_video_loading_log():
    """Initialize video loading log file."""
    global VIDEO_LOADING_LOG
    if core_preamble_and_imports.VIDEO_LOADING_DEBUG and VIDEO_LOADING_LOG is None:
        try:
            # Log file path: project root directory
            log_path = Path(__file__).resolve().parent.parent / "videoloading.log"
            VIDEO_LOADING_LOG = open(log_path, "w", encoding="utf-8")
            VIDEO_LOADING_LOG.write(f"=== Video Loading Debug Log Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            VIDEO_LOADING_LOG.flush()
        except Exception as e:
            print(f"Warning: Could not create videoloading.log: {e}")

def video_loading_log(message):
    """Ultra detailed video loading log (if --videoloading is enabled)."""
    if not core_preamble_and_imports.VIDEO_LOADING_DEBUG:
        return
    safe_message = sanitize_log_text(message)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # date + milliseconds
    line = f"[{timestamp}] {safe_message}"
    try:
        print(line)
    except Exception:
        pass
    with VIDEO_LOADING_LOG_LOCK:
        if VIDEO_LOADING_LOG:
            try:
                VIDEO_LOADING_LOG.write(line + "\n")
                VIDEO_LOADING_LOG.flush()
            except (OSError, IOError, AttributeError) as e:
                try:
                    sys.stderr.write(f"Video loading log write failed: {e}\n")
                except Exception:
                    pass

def video_loading_log_json(data, title="DB Content"):
    """Write DB content in JSON format to videoloading.log file."""
    if not core_preamble_and_imports.VIDEO_LOADING_DEBUG:
        return
    try:
        # JSON formatting with indentation (more readable)
        json_str = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        prefix = f"[{ts}] "
        with VIDEO_LOADING_LOG_LOCK:
            if VIDEO_LOADING_LOG:
                try:
                    VIDEO_LOADING_LOG.write(f"\n{prefix}=== {title} (JSON) ===\n")
                    for json_line in json_str.splitlines():
                        VIDEO_LOADING_LOG.write(prefix + json_line + "\n")
                    VIDEO_LOADING_LOG.write(f"{prefix}=== End of {title} ===\n\n")
                    VIDEO_LOADING_LOG.flush()
                except (OSError, IOError, AttributeError) as e:
                    try:
                        sys.stderr.write(f"Video loading log JSON write failed: {e}\n")
                    except Exception:
                        pass
    except Exception as e:
        # If JSON serialization fails, just write an error message
        video_loading_log(f"ERROR: Could not serialize {title} to JSON: {e}")

def load_debug_log(message):
    """Debug log during loading (if AV1_LOAD_DEBUG=1)."""
    if not LOAD_DEBUG:
        return
    safe_message = sanitize_log_text(message)
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[LOAD-DEBUG {timestamp}] {safe_message}"
    try:
        print(line)
    except Exception:
        pass
    log_writer = get_log_writer()
    if log_writer:
        try:
            log_writer.write(line + "\n")
            log_writer.flush()
        except (OSError, IOError, AttributeError):
            pass


class EncodingStopped(Exception):
    """Indicates that a user stop occurred."""
    pass


class NoSuitableCRFFound(Exception):
    """Indicates that ab-av1 did not find a suitable CRF value (VMAF >= 85 AND file <= 75%)."""
    pass


class NVENCFallbackRequired(Exception):
    """Indicates that switching from NVENC to SVT-AV1 is required."""
    pass


def resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent):
    """Ensures that VMAF values follow the current state of GUI sliders."""
    # IMPORTANT: Import module to get fresh value, not import-time snapshot
    from . import core_preamble_and_imports
    gui = core_preamble_and_imports.GUI_INSTANCE

    if gui is not None:
        if initial_min_vmaf is None:
            initial_min_vmaf = getattr(gui, "current_min_vmaf", None)
            if initial_min_vmaf is None:
                initial_min_vmaf = float(gui.min_vmaf.get())
        if vmaf_step is None:
            vmaf_step = getattr(gui, "current_vmaf_step", None)
            if vmaf_step is None:
                vmaf_step = float(gui.vmaf_step.get())
        if max_encoded_percent is None:
            max_encoded_percent = getattr(gui, "current_max_encoded_percent", None)
            if max_encoded_percent is None:
                max_encoded_percent = float(gui.max_encoded_percent.get())
    else:
        if initial_min_vmaf is None:
            initial_min_vmaf = 95.0
        if vmaf_step is None:
            vmaf_step = 2.5
        if max_encoded_percent is None:
            max_encoded_percent = 75

    return float(initial_min_vmaf), float(vmaf_step), float(max_encoded_percent)

def get_crf_increment():
    """Get CRF increment value from GUI settings.
    
    Returns:
        int: CRF increment value (default 1 if GUI not available).
    """
    # IMPORTANT: Import module to get fresh value, not import-time snapshot
    from . import core_preamble_and_imports
    gui = core_preamble_and_imports.GUI_INSTANCE
    if gui is not None:
        try:
            return int(gui.crf_increment.get())
        except (AttributeError, ValueError, TypeError):
            pass
    return 1

def get_startup_info():
    """Create Windows subprocess startupinfo to hide console window.
    
    Returns:
        subprocess.STARTUPINFO: Configuration to hide console on Windows,
                                 None on other platforms.
    """
    if os.name != "nt":
        return None
    
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    return startupinfo

@contextmanager
def managed_subprocess(cmd, cwd=None, stop_event=None, timeout=180):
    """Context manager for subprocess.Popen that ensures proper cleanup.
    
    Args:
        cmd: Command to execute
        cwd: Working directory
        stop_event: Optional threading.Event to signal cancellation
        timeout: Timeout in seconds
    
    Yields:
        subprocess.Popen process object
    
    Raises:
        EncodingStopped: If stop_event is set
        subprocess.TimeoutExpired: If process exceeds timeout
    """
    process = None
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            cwd=cwd,
            startupinfo=get_startup_info()
        )
        
        # Process registration
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        yield process
        
    finally:
        # Guarantee cleanup
        if process:
            try:
                if process.poll() is None:  # Still running
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            except (OSError, ProcessLookupError, subprocess.SubprocessError):
                # If terminate/kill fails, try kill
                try:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=2)
                except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
                    pass
            finally:
                # Remove process from list
                with ACTIVE_PROCESSES_LOCK:
                    if process in ACTIVE_PROCESSES:
                        ACTIVE_PROCESSES.remove(process)

def sanitize_path(path: Path) -> str:
    """
    Sanitize path to prevent injection attacks and ensure it exists.
    
    Args:
        path: Path object to sanitize
    
    Returns:
        Sanitized path string
    
    Raises:
        FileNotFoundError: If path does not exist
        ValueError: If path is not a file or is a symlink (security risk)
    """
    if not path:
        raise ValueError("Path cannot be None or empty")
    
    try:
        resolved = path.resolve()
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Cannot resolve path: {e}") from e
    
    # Check if path exists
    if not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")
    
    # Security check: do not allow symbolic links (optional, but safer)
    # Note: This might be too strict, but safer
    try:
        if resolved.is_symlink():
            # If symbolic link, follow it, but warn
            target = resolved.readlink()
            resolved = target.resolve()
    except (OSError, RuntimeError):
        # If fails, continue with resolved path
        pass
    
    return os.fspath(resolved)

def cleanup_ab_av1_temp_dirs(base_dir):
    """Removes directories starting with .ab-av1- left behind by ab-av1."""
    try:
        base_path = Path(base_dir)
        if not base_path.exists():
            return
        for entry in base_path.iterdir():
            if entry.is_dir() and entry.name.startswith(".ab-av1-"):
                try:
                    if not DEBUG_MODE:
                        shutil.rmtree(entry, ignore_errors=True)
                    else:
                        print(f"  [STOP] DEBUG: ab-av1 temp KEPT: {entry}")
                except Exception as e:
                    print(f"[WARN] ab-av1 temp deletion error: {e}")
    except Exception as e:
        print(f"[WARN] ab-av1 temp traversal error: {e}")

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
        self.mirror_to_main_log = False  # If True, also write to LOG_WRITER (for --autotest)

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

    def _get_log_file(self):
        """Returns the appropriate log file based on logger index"""
        # If log_files_list exists, choose based on logger_index (not worker_index!)
        # This ensures that each logger object always writes to the same log file,
        # regardless of which worker uses it
        if self.log_files_list and self.encoder_type == 'nvenc':
            if len(self.log_files_list) > 0:
                # Choose based on logger index to avoid race condition
                log_file_idx = self.logger_index % len(self.log_files_list)
                log_file = self.log_files_list[log_file_idx]
                # Check if file is None or closed, try to reopen
                if log_file is None or (hasattr(log_file, 'closed') and log_file.closed):
                    # Try to get the file path from stored paths
                    if hasattr(self, 'nvenc_log_paths') and log_file_idx < len(self.nvenc_log_paths):
                        file_path = self.nvenc_log_paths[log_file_idx]
                        try:
                            log_file = open(file_path, "a", encoding="utf-8")
                            self.log_files_list[log_file_idx] = log_file
                        except (OSError, IOError, PermissionError):
                            pass
                return log_file
        # Backward compatibility: if no list, use original log_file
        if self.log_file is None or (hasattr(self.log_file, 'closed') and self.log_file.closed):
            # Try to reopen if we have a stored path
            file_path = None
            if self.encoder_type == 'svt' and hasattr(self, 'svt_log_path'):
                file_path = self.svt_log_path
            elif hasattr(self.log_file, 'name'):
                file_path = Path(self.log_file.name)
            if file_path:
                try:
                    self.log_file = open(file_path, "a", encoding="utf-8")
                except (OSError, IOError, PermissionError, AttributeError):
                    pass
        return self.log_file

    def write(self, message):
        """Thread-safe writing to console and log file - with newline handling fix"""
        with self.lock:
            if not message:
                return

            message = sanitize_log_text(message.replace('\r', '\n'))

            # Write to log file (to file chosen based on logger_index)
            log_file = self._get_log_file()
            if not log_file:
                # Debug: log if file is None
                log_writer = get_log_writer()
                if log_writer and hasattr(self, 'encoder_type'):
                    try:
                        log_writer.write(f"[WARN] Log file is None for {self.encoder_type} logger (index {self.logger_index})\n")
                        log_writer.flush()
                    except (OSError, IOError, AttributeError, ValueError):
                        # Log writer closed/unavailable - non-critical
                        pass
            if log_file:
                try:
                    # Check if file is still open and writable
                    if hasattr(log_file, 'closed') and log_file.closed:
                        # File was closed, try to reopen it
                        file_path = None
                        if self.log_files_list and self.encoder_type == 'nvenc':
                            # For NVENC, we need to get the path from the list
                            log_file_idx = self.logger_index % len(self.log_files_list)
                            # Try to get path from original file if available
                            if hasattr(self, 'nvenc_log_paths') and log_file_idx < len(self.nvenc_log_paths):
                                file_path = self.nvenc_log_paths[log_file_idx]
                            elif self.log_file and hasattr(self.log_file, 'name'):
                                file_path = Path(self.log_file.name)
                        elif self.encoder_type == 'svt':
                            if hasattr(self, 'svt_log_path'):
                                file_path = self.svt_log_path
                            elif self.log_file and hasattr(self.log_file, 'name'):
                                file_path = Path(self.log_file.name)
                        else:
                            if self.log_file and hasattr(self.log_file, 'name'):
                                file_path = Path(self.log_file.name)
                        
                        if file_path:
                            try:
                                log_file = open(file_path, "a", encoding="utf-8")
                                # Update the reference in the list if it's in a list
                                if self.log_files_list and self.encoder_type == 'nvenc':
                                    log_file_idx = self.logger_index % len(self.log_files_list)
                                    self.log_files_list[log_file_idx] = log_file
                                elif self.encoder_type == 'svt':
                                    self.log_file = log_file
                            except (OSError, IOError, PermissionError) as e:
                                log_writer = get_log_writer()
                                if log_writer:
                                    try:
                                        log_writer.write(f"[WARN] Failed to reopen log file {file_path}: {e}\n")
                                        log_writer.flush()
                                    except (OSError, IOError, AttributeError, ValueError):
                                        pass
                                return
                    log_file.write(self._timestamp_for_file(message))
                    log_file.flush()
                except (OSError, IOError, AttributeError) as e:
                    # Log error to main log if available
                    log_writer = get_log_writer()
                    if log_writer:
                        try:
                            log_writer.write(f"[WARN] Log file write error ({self.encoder_type}): {e}\n")
                            log_writer.flush()
                        except (OSError, IOError, AttributeError, ValueError):
                            # Log writer closed/unavailable - non-critical
                            pass

            # Add to buffer
            self.buffer += message

            # Process full lines
            lines = self.buffer.split('\n')
            self.buffer = lines.pop()  # Keep last partial line

            # Send full lines - ONLY to console corresponding to own encoder type
            for line in lines:
                payload = line + '\n'
                if self.encoder_type == 'nvenc':
                    # Send worker_index, logger_index, and video_path for log storage
                    self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))
                elif self.encoder_type == 'svt':
                    self.gui_queue.put(("svt_log", payload, self.current_video_path))
                else:
                    # Default fallback
                    self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))

            # Mirror to main log (LOG_WRITER) when enabled (--autotest mode)
            if self.mirror_to_main_log:
                log_writer = get_log_writer()
                if log_writer:
                    try:
                        encoder_tag = (self.encoder_type or 'worker').upper()
                        prefix = f"[{encoder_tag}#{self.worker_index + 1}] "
                        for line in message.split('\n'):
                            if line.strip():
                                log_writer.write(prefix + line + '\n')
                        log_writer.flush()
                    except (OSError, IOError, AttributeError, ValueError):
                        pass

    def flush(self):
        """Flush buffer - ONLY to console corresponding to own encoder type"""
        with self.lock:
            if self.buffer:
                self.buffer = sanitize_log_text(self.buffer)
                payload = self.buffer + '\n'
                # Write to log file (to file chosen based on logger_index)
                log_file = self._get_log_file()
                if log_file:
                    try:
                        # Check if file is still open and writable
                        if hasattr(log_file, 'closed') and log_file.closed:
                            # File was closed, try to reopen it
                            file_path = None
                            if self.log_files_list and self.encoder_type == 'nvenc':
                                # For NVENC, get path from stored paths
                                log_file_idx = self.logger_index % len(self.log_files_list)
                                if hasattr(self, 'nvenc_log_paths') and log_file_idx < len(self.nvenc_log_paths):
                                    file_path = self.nvenc_log_paths[log_file_idx]
                                elif self.log_file and hasattr(self.log_file, 'name'):
                                    file_path = Path(self.log_file.name)
                            elif self.encoder_type == 'svt':
                                if hasattr(self, 'svt_log_path'):
                                    file_path = self.svt_log_path
                                elif self.log_file and hasattr(self.log_file, 'name'):
                                    file_path = Path(self.log_file.name)
                            else:
                                if self.log_file and hasattr(self.log_file, 'name'):
                                    file_path = Path(self.log_file.name)
                            
                            if file_path:
                                try:
                                    log_file = open(file_path, "a", encoding="utf-8")
                                    # Update the reference in the list if it's in a list
                                    if self.log_files_list and self.encoder_type == 'nvenc':
                                        log_file_idx = self.logger_index % len(self.log_files_list)
                                        self.log_files_list[log_file_idx] = log_file
                                    elif self.encoder_type == 'svt':
                                        self.log_file = log_file
                                except (OSError, IOError, PermissionError) as e:
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[WARN] Failed to reopen log file {file_path}: {e}\n")
                                            LOG_WRITER.flush()
                                        except (OSError, IOError, AttributeError, ValueError):
                                            pass
                                return
                        log_file.write(self._timestamp_for_file(self.buffer))
                        log_file.flush()
                    except (OSError, IOError, AttributeError) as e:
                        # Log error to main log if available
                        # Log error to main log if available
                        log_writer = get_log_writer()
                        if log_writer:
                            try:
                                log_writer.write(f"[WARN] Log file flush error ({self.encoder_type}): {e}\n")
                                log_writer.flush()
                            except (OSError, IOError, AttributeError, ValueError):
                                # LOG_WRITER error - non-critical, silently ignore
                                pass
                if self.encoder_type == 'nvenc':
                    # Send worker_index, logger_index, and video_path for log storage
                    self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))
                elif self.encoder_type == 'svt':
                    self.gui_queue.put(("svt_log", payload, self.current_video_path))
                else:
                    # Default fallback
                    self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload, self.current_video_path))

                # Mirror to main log (LOG_WRITER) when enabled (--autotest mode)
                if self.mirror_to_main_log:
                    log_writer = get_log_writer()
                    if log_writer:
                        try:
                            encoder_tag = (self.encoder_type or 'worker').upper()
                            prefix = f"[{encoder_tag}#{self.worker_index + 1}] "
                            for line in payload.split('\n'):
                                if line.strip():
                                    log_writer.write(prefix + line + '\n')
                            log_writer.flush()
                        except (OSError, IOError, AttributeError, ValueError):
                            pass

                self.buffer = ""

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
    
    def clear_all_loggers(self):
        """Clear all loggers from the stack (emergency cleanup)."""
        stack = self._get_stack()
        stack.clear()

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
            # Worker output goes ONLY to the logger (GUI tab), not mirrored to console/log
        else:
            if self._fallback is not None:
                try:
                    self._fallback.write(message)
                except (AttributeError, OSError, IOError):
                    # If fallback is not writable, swallow silently
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
                    # If fallback is not flushable, swallow silently
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

# Safe stdout initialization - if sys.stdout is None, use a dummy stream
_safe_stdout = sys.stdout
if _safe_stdout is None:
    # If sys.stdout is None, create a dummy stream
    class DummyStream:
        """Dummy stdout stream for environments where sys.stdout is None (e.g., pythonw.exe)."""
        def write(self, s): pass
        def flush(self): pass
        def isatty(self): return False
        def fileno(self): raise OSError("fileno() not available")
    _safe_stdout = DummyStream()

STDOUT_ROUTER = ThreadSafeStdoutRouter(_safe_stdout)
sys.stdout = STDOUT_ROUTER

def debug_print(*args, **kwargs):
    """Print directly to forceconsole, bypassing console_redirect.
    
    This function writes to the original stdout (sys.__stdout__), which
    is the forceconsole window when --forceconsole is active. It bypasses
    the STDOUT_ROUTER and console_redirect mechanism.
    
    Args:
        *args: Positional arguments (same as print())
        **kwargs: Keyword arguments (same as print())
    
    Usage:
        debug_print("This goes to forceconsole")
        debug_print(f"Value: {x}, Another: {y}")
    """
    # Use the original stdout that was saved before we replaced it
    # This is the Windows console window when --forceconsole is active
    original_stdout = sys.__stdout__
    if original_stdout is None:
        # Fallback to the safe stdout if __stdout__ is None
        original_stdout = _safe_stdout
    
    # Format the message like print() does
    sep = kwargs.get('sep', ' ')
    end = kwargs.get('end', '\n')
    file = kwargs.get('file', original_stdout)
    flush = kwargs.get('flush', False)
    
    try:
        message = sep.join(str(arg) for arg in args) + end
        file.write(message)
        if flush:
            file.flush()
    except (AttributeError, OSError, IOError):
        # If writing fails, silently ignore (console might not be available)
        pass


@contextmanager
def console_redirect(logger):
    """Context manager to redirect print() output to a specific logger.
    
    Args:
        logger: Logger instance to redirect output to.
        
    Yields:
        None. Restores original stdout routing on exit.
    """
    STDOUT_ROUTER.set_logger(logger)
    try:
        yield
    finally:
        STDOUT_ROUTER.clear_logger()

def debug_pause(current_step, next_step, file_info=""):
    """
    Debug mode - thread-safe version.
    Sends message to main thread via Queue.
    """
    if not DEBUG_MODE:
        return
    
    print(f"\n{'!'*80}")
    print(f"[STOP] DEBUG PAUSE")
    print(f"{'!'*80}")
    print(f"Current: {current_step}")
    print(f"Next: {next_step}")
    if file_info:
        print(f"Info: {file_info}")
    print(f"{'!'*80}\n")
    
    continue_event = threading.Event()
    
    try:
        # If there is a GUI queue (provided by encoding_worker)
        if hasattr(debug_pause, 'gui_queue'):
            debug_pause.gui_queue.put((
                "debug_pause",
                current_step,
                next_step,
                file_info,
                continue_event
            ))
            # Wait for main thread - timeout 30 seconds
            if not continue_event.wait(timeout=60.0):
                print("    [WARN] DEBUG: Timeout - GUI did not respond within 60 seconds, continuing...")
        else:
            # Console fallback
            input("▶ DEBUG - ENTER to continue...")
    except Exception as e:
        print(f"Debug pause error: {e}")
        try:
            input("▶ DEBUG - ENTER to continue...")
        except (EOFError, KeyboardInterrupt, OSError):
            pass
