import importlib
import os
import pkgutil
import shutil
import subprocess
import sys
import threading
import time
import types
from datetime import datetime
from pathlib import Path

# ============================================================================
# AUTOMATIC DEPENDENCY INSTALLER
# ============================================================================
# Required packages: package_name -> import_name mapping
# Some packages have different names for pip install vs import (e.g., Pillow -> PIL)
REQUIRED_PACKAGES = {
    'Pillow': 'PIL',      # Image processing
    'flask': 'flask',     # Web interface (optional but recommended)
}

def ensure_dependencies():
    """Check and install missing dependencies automatically.
    
    Iterates through REQUIRED_PACKAGES and installs any missing packages
    using pip. Shows progress in console.
    """
    missing = []
    
    for package_name, import_name in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(package_name)
    
    if not missing:
        return True  # All dependencies satisfied
    
    print(f"\n{'='*60}")
    print("MISSING DEPENDENCIES DETECTED")
    print(f"{'='*60}")
    print(f"The following packages are missing: {', '.join(missing)}")
    print("Installing automatically...")
    print()
    
    for package in missing:
        print(f"  Installing {package}...", end=" ", flush=True)
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", package, "--quiet"],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per package
            )
            if result.returncode == 0:
                print("[OK] OK")
            else:
                print(f"[ERROR] FAILED")
                print(f"    Error: {result.stderr.strip()}")
                return False
        except subprocess.TimeoutExpired:
            print("[ERROR] TIMEOUT")
            return False
        except Exception as e:
            print(f"[ERROR] ERROR: {e}")
            return False
    
    print()
    print(f"{'='*60}")
    print("All dependencies installed successfully!")
    print(f"{'='*60}")
    print()
    
    # Reload modules after installation
    for package_name, import_name in REQUIRED_PACKAGES.items():
        if package_name in missing:
            try:
                importlib.import_module(import_name)
            except ImportError as e:
                print(f"Warning: Could not import {import_name} after installation: {e}")
    
    return True

# Run dependency check before any other imports
if not ensure_dependencies():
    print("\nFailed to install required dependencies.")
    print("Please install manually: pip install Pillow flask")
    sys.exit(1)

# ============================================================================

import tkinter as tk
import zipfile

# Dynamic loading: core and gui modules from fragments (runs in zipapp too)
def _load_core_and_gui():
    # Get package name - may be None in frozen exe, so fallback to explicit name
    pkg = __package__ or 'av1_recompress'

    def _load_fragments(module_name: str, parts: list[str], header: str = ""):
        full_module_name = f"{pkg}.{module_name}"
        mod = types.ModuleType(full_module_name)
        
        # Determine the base path for loading fragments
        # PyInstaller sets sys._MEIPASS when running as frozen exe
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            # Running as PyInstaller bundle
            base_path = Path(sys._MEIPASS) / 'av1_recompress'
        else:
            # Normal execution
            base_path = Path(__file__).resolve().parent
        
        # Explicitly set module attributes for proper relative imports
        mod.__dict__["__file__"] = str(base_path / f"{module_name}.py")
        mod.__dict__["__name__"] = full_module_name
        mod.__dict__["__package__"] = pkg
        mod.__dict__["__loader__"] = None
        mod.__dict__["__spec__"] = None
        fragments = [header] if header else []
        
        for name in parts:
            file_path = base_path / name
            
            # Try direct file read first (works for PyInstaller and normal file system)
            if file_path.exists():
                data = file_path.read_bytes()
            else:
                # Fallback to pkgutil for zipapp
                data = pkgutil.get_data(pkg, name)
                if data is None:
                    raise FileNotFoundError(f"Cannot find fragment file: {name}")
            
            text = data.decode("utf-8")
            if text.startswith("\ufeff"):
                text = text.lstrip("\ufeff")
            fragments.append(text)
        
        exec("\n".join(fragments), mod.__dict__)
        sys.modules[f"{pkg}.{module_name}"] = mod
        return mod

    # Assembling Core module
    core_parts = [
        "core_preamble_and_imports.py",
        "core_paths_tools_logging.py",
        "core_translations_bridge.py",
        "core_probe_and_scan.py",
        "core_subtitles_and_metadata.py",
        "core_metrics_and_validation.py",
        "core_audio_video_ops.py",
        "core_workers_and_flows.py",
        "core_cli_and_entrypoints.py",
    ]
    core_mod = _load_fragments("core", core_parts)
    
    # Register core sub-modules in sys.modules for relative imports in GUI fragments
    # The GUI fragments use "from .core_preamble_and_imports import ...", etc.
    # These need to resolve properly in the exec() environment
    core_submodules = [
        "core_preamble_and_imports",
        "core_paths_tools_logging",
        "core_translations_bridge",
        "core_probe_and_scan",
        "core_subtitles_and_metadata",
        "core_metrics_and_validation",
        "core_audio_video_ops",
        "core_workers_and_flows",
        "core_cli_and_entrypoints",
    ]
    for submod_name in core_submodules:
        full_name = f"{pkg}.{submod_name}"
        if full_name not in sys.modules:
            # Create a module alias that references the core module namespace
            alias_mod = types.ModuleType(full_name)
            alias_mod.__dict__.update(core_mod.__dict__)
            alias_mod.__dict__["__name__"] = full_name
            alias_mod.__dict__["__package__"] = pkg
            sys.modules[full_name] = alias_mod

    # i18n module: normal import
    i18n_mod = importlib.import_module(f"{pkg}.i18n")
    
    # Import gui_imports module and register it for relative imports
    # GUI fragments use "from .gui_imports import *"
    gui_imports_mod = importlib.import_module(f"{pkg}.gui_imports")
    # Ensure it's in sys.modules with the correct name
    sys.modules[f"{pkg}.gui_imports"] = gui_imports_mod

    # Assembling GUI module (header needed for proxy and imports)
    gui_parts = [
        "gui_preamble_and_imports.py",
        "gui_shared.py",
        "gui_app_state_and_paths.py",
        "gui_widgets_layout.py",
        "gui_language_and_labels.py",
        "gui_tree_setup.py",
        "gui_buttons_actions.py",
        "gui_db_and_state_load.py",
        "gui_context_menus.py",
        "gui_video_loading.py",
        "gui_encoding_control.py",
        "gui_nvenc_worker.py",
        "gui_svt_worker.py",
        "gui_vmaf_worker.py",
        "gui_audio_worker.py",
        "gui_rebuild_worker.py",
        "gui_task_list.py",  # NEW: Task list based queue management
        "gui_queue_management.py",
        "gui_settings_control.py",  # NEW: Centralized settings control
        "gui_events_and_progress.py",
        "gui_misc_helpers.py",
        "gui_http.py",
        "gui_main.py",
    ]
    gui_header = (
        "from . import core, i18n\n"
        "from .core import *\n"
        "from .i18n import *\n"
    )
    gui_mod = _load_fragments("gui", gui_parts, header=gui_header)
    gui_mod.__dict__["core"] = core_mod
    gui_mod.__dict__["i18n"] = i18n_mod

    return core_mod, gui_mod


core, gui = _load_core_and_gui()

# Set APP_ROOT explicitly for frozen builds to find resources correctly
# This ensures save.db is looked for in the exe folder, not CWD
if getattr(sys, 'frozen', False):
    core.APP_ROOT = Path(sys.executable).resolve().parent
else:
    core.APP_ROOT = Path(__file__).resolve().parent



# Export necessary elements from the loaded modules
# (can't use relative imports here in frozen exe where __package__ may be None)
VideoEncoderGUI = gui.VideoEncoderGUI


AUTOTEST_FORCE_EXIT_SECONDS = 900


def _is_autotest_active(short_test: bool = False, autotest_profile: str = "") -> bool:
    profile = (autotest_profile or os.environ.get("AV1_AUTO_TEST_PROFILE") or "").strip().lower()
    if short_test or profile in ("mini", "medium", "maxi", "legacy"):
        return True
    return os.environ.get("AV1_AUTO_TEST") == "1"


def _start_autotest_force_exit_watchdog(root, log_sink=None, close_callback=None, timeout_seconds: int = AUTOTEST_FORCE_EXIT_SECONDS):
    """Force-exit watchdog for autotests. Runs in separate thread and kills process on deadloop."""
    if root is None or timeout_seconds <= 0:
        return
    if getattr(root, "_autotest_force_watchdog_started", False):
        return
    setattr(root, "_autotest_force_watchdog_started", True)

    def _log(line: str):
        msg = f"[AUTOTEST] {line}\n"
        try:
            if log_sink is not None and hasattr(log_sink, "write"):
                log_sink.write(msg)
                if hasattr(log_sink, "flush"):
                    log_sink.flush()
                return
        except Exception:
            pass
        try:
            print(msg, end="")
        except Exception:
            pass

    def _request_close():
        try:
            if callable(close_callback):
                close_callback()
                return
        except Exception:
            pass
        try:
            root.quit()
        except Exception:
            pass
        try:
            root.destroy()
        except Exception:
            pass

    def _watchdog():
        _log(f"Brute-force exit watchdog armed: {timeout_seconds} seconds")
        time.sleep(timeout_seconds)
        _log("Brute-force timeout reached, forcing shutdown")
        try:
            root.after(0, _request_close)
        except Exception:
            pass
        time.sleep(3.0)
        os._exit(124)

    threading.Thread(target=_watchdog, name="autotest-force-exit-watchdog", daemon=True).start()


def main(short_test: bool = False, http_enabled: bool = False, http_port: int = 5000, autotest_profile: str = ""):
    print(f"\n{'='*80}\nAV1 BATCH ENCODER\n{'='*80}\n")
    root = tk.Tk()
    app = VideoEncoderGUI(root, http_enabled=http_enabled, http_port=http_port)

    if _is_autotest_active(short_test=short_test, autotest_profile=autotest_profile):
        log_target = getattr(core, "LOG_WRITER", None) or sys.stdout
        _start_autotest_force_exit_watchdog(
            root,
            log_sink=log_target,
            close_callback=getattr(app, "_autotest_exit", None),
            timeout_seconds=AUTOTEST_FORCE_EXIT_SECONDS,
        )

    if short_test:
        def _short_test_exit():
            print("\n=== AUTOTEST MINI mode: 10 seconds elapsed, automatic exit ===\n")
            try:
                exit_cb = getattr(app, "_autotest_exit", None)
                if callable(exit_cb):
                    exit_cb()
                    return
            except Exception:
                pass
            try:
                root.quit()
            except Exception:
                pass
            try:
                root.destroy()
            except Exception:
                pass
        root.after(10_000, _short_test_exit)

    root.mainloop()


def run():
    # Check for help flag FIRST (before any other processing)
    if any(arg.lower() in ('-h', '--help', '-help', '/*') for arg in sys.argv[1:]):
        # Determine language based on system locale
        import locale
        try:
            system_lang = locale.getdefaultlocale()[0] or ''
        except Exception:
            system_lang = ''
        
        is_hungarian = system_lang.lower().startswith('hu')
        
        if is_hungarian:
            help_text = """
AV1 Batch Encoder - Parancssori Kapcsolók
==========================================

Használat: AV1_Batch_Encoder.exe [KAPCSOLÓK]

Kapcsolók:
  -h, --help           Súgó megjelenítése és kilépés
  --forceconsole       Konzol ablak megjelenítése (hibakereséshez)
  --autotest-mini      Mini önteszt: automatikus kilépés 10 mp után
  --shorttest          Régi alias a --autotest-mini kapcsolóhoz
  --autotest           Közepes önteszt (mentett állapot betöltés + dump + kilépés)
  --autotest-medium    Közepes önteszt (mint --autotest)
  --autotest-maxi      Maxi önteszt (betöltés + átkódolás indítás + 5 perc + azonnali leállítás + dump)
  --autotest-legacy    Régi önteszt (1 perc futás után stop + kilépés)
  --load-debug         Részletes betöltési hibakeresési üzenetek
  --videoloading       Videó betöltési hibakeresési üzenetek
  --http               HTTP webes felület engedélyezése
  --port:PORT          HTTP szerver port beállítása (alapért.: 5000)

Példák:
  AV1_Batch_Encoder.exe                     Normál indítás (csak GUI)
  AV1_Batch_Encoder.exe --http              Webes felület az 5000-es porton
  AV1_Batch_Encoder.exe --http --port:8080  Webes felület a 8080-as porton
  AV1_Batch_Encoder.exe --forceconsole      Indítás látható konzol ablakkal
"""
            exit_prompt = "\nNyomj Enter-t a kilépéshez..."
        else:
            help_text = """
AV1 Batch Encoder - Command Line Options
=========================================

Usage: AV1_Batch_Encoder.exe [OPTIONS]

Options:
  -h, --help           Show this help message and exit
  --forceconsole       Force console window to appear (for debugging)
  --autotest-mini      Exit automatically after 10 seconds (mini self-test)
  --shorttest          Legacy alias of --autotest-mini
  --autotest           Medium self-test (load saved state + dump + exit)
  --autotest-medium    Medium self-test (same as --autotest)
  --autotest-maxi      Maxi self-test (load + start encode + 5 min + immediate stop + dump)
  --autotest-legacy    Legacy self-test (start, 1 min, stop, exit)
  --load-debug         Enable detailed loading debug messages
  --videoloading       Enable video loading debug messages
  --http               Enable HTTP web interface
  --port:PORT          Set HTTP server port (default: 5000)

Examples:
  AV1_Batch_Encoder.exe                     Start normally (GUI only)
  AV1_Batch_Encoder.exe --http              Start with web interface on port 5000
  AV1_Batch_Encoder.exe --http --port:8080  Start with web interface on port 8080
  AV1_Batch_Encoder.exe --forceconsole      Start with visible console window
"""
            exit_prompt = "\nPress Enter to exit..."
        
        if sys.platform == "win32":
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                
                # Allocate a new console for help display
                kernel32.AllocConsole()
                sys.stdout = open("CONOUT$", "w", encoding="utf-8")
                sys.stderr = open("CONOUT$", "w", encoding="utf-8")
                sys.stdin = open("CONIN$", "r", encoding="utf-8")
                
                print(help_text)
                sys.stdout.flush()
                
                try:
                    input(exit_prompt)
                except Exception:
                    import time
                    time.sleep(5)  # Fallback: wait 5 seconds
                    
            except Exception:
                # Last resort fallback
                print(help_text)
        else:
            # Non-Windows: just print and exit
            print(help_text)
        
        sys.exit(0)
    
    # Simple switches for console execution and quick exit
    force_console = False
    short_test = os.environ.get("AV1_SHORT_TEST") == "1"
    autotest_profile = (os.environ.get("AV1_AUTO_TEST_PROFILE") or "").strip().lower()
    if autotest_profile == "mini":
        short_test = True
        os.environ["AV1_SHORT_TEST"] = "1"
    load_debug_flag = core.LOAD_DEBUG
    http_enabled = False
    http_port = 5000
    cleaned_args = [sys.argv[0]]
    
    # Pre-scan for console flag to enable it ASAP
    for arg in sys.argv[1:]:
        arg_lower = arg.lower()
        if arg_lower in ("-forceconsole", "--forceconsole", "--force-console"):
            force_console = True
            
    # Conditional Console Allocation (Windows Only)
    if force_console and sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            # Check if we already have a console
            if kernel32.GetConsoleWindow() == 0:
                kernel32.AllocConsole()
                # Re-connect standard streams
                sys.stdout = open("CONOUT$", "w", encoding="utf-8")
                sys.stderr = open("CONOUT$", "w", encoding="utf-8")
                sys.stdin = open("CONIN$", "r", encoding="utf-8")
                print("Console allocated via --forceconsole")
        except Exception as e:
            # Fallback if something goes wrong, though unlikely on Windows
            pass

    for arg in sys.argv[1:]:
        arg_lower = arg.lower()
        if arg_lower in ("-forceconsole", "--forceconsole", "--force-console"):
            # Already handled, just mark flag and skip arg
            force_console = True
            continue
        if arg_lower in ("--autotest-mini", "--autotest-mini-test", "--autotest-mini-short"):
            short_test = True
            autotest_profile = "mini"
            os.environ["AV1_SHORT_TEST"] = "1"
            os.environ["AV1_AUTO_TEST"] = "1"
            os.environ["AV1_AUTO_TEST_PROFILE"] = "mini"
            continue
        if arg_lower in ("-shorttest", "--shorttest", "--short-test"):
            short_test = True
            autotest_profile = "mini"
            os.environ["AV1_SHORT_TEST"] = "1"
            os.environ["AV1_AUTO_TEST"] = "1"
            os.environ["AV1_AUTO_TEST_PROFILE"] = "mini"
            continue
        if arg_lower in ("--load-debug", "--loaddebug", "-loaddebug"):
            load_debug_flag = True
            continue
        if arg_lower in ("--videoloading", "--video-loading", "-videoloading"):
            os.environ["AV1_VIDEO_LOADING_DEBUG"] = "1"
            core.VIDEO_LOADING_DEBUG = True
            continue
        if arg_lower in ("--autotest-short", "--autotest-shorttest"):
            short_test = True
            autotest_profile = "mini"
            os.environ["AV1_SHORT_TEST"] = "1"
            os.environ["AV1_AUTO_TEST"] = "1"
            os.environ["AV1_AUTO_TEST_PROFILE"] = "mini"
            continue
        if arg_lower in ("--autotest", "--autotest-medium", "-autotest"):
            os.environ["AV1_AUTO_TEST"] = "1"
            autotest_profile = "medium"
            os.environ["AV1_AUTO_TEST_PROFILE"] = autotest_profile
            continue
        if arg_lower in ("--autotest-maxi", "--autotest-max"):
            os.environ["AV1_AUTO_TEST"] = "1"
            autotest_profile = "maxi"
            os.environ["AV1_AUTO_TEST_PROFILE"] = autotest_profile
            continue
        if arg_lower in ("--autotest-legacy", "--autotest-old"):
            os.environ["AV1_AUTO_TEST"] = "1"
            autotest_profile = "legacy"
            os.environ["AV1_AUTO_TEST_PROFILE"] = autotest_profile
            continue
        if arg_lower.startswith("--autotest:") or arg_lower.startswith("--autotest="):
            profile = arg_lower.split(":", 1)[1] if ":" in arg_lower else arg_lower.split("=", 1)[1]
            if profile == "mini":
                short_test = True
                autotest_profile = "mini"
                os.environ["AV1_SHORT_TEST"] = "1"
                os.environ["AV1_AUTO_TEST"] = "1"
                os.environ["AV1_AUTO_TEST_PROFILE"] = "mini"
                continue
            if profile in ("medium", "maxi", "legacy"):
                os.environ["AV1_AUTO_TEST"] = "1"
                autotest_profile = profile
                os.environ["AV1_AUTO_TEST_PROFILE"] = autotest_profile
                continue
        if arg_lower == "--no-autotest":
            os.environ.pop("AV1_AUTO_TEST", None)
            os.environ.pop("AV1_AUTO_TEST_PROFILE", None)
            autotest_profile = ""
            continue
        if arg_lower == "--http":
            http_enabled = True
            continue
        if arg_lower.startswith("--port:"):
            try:
                http_port = int(arg_lower.split(":")[1])
            except ValueError:
                pass
            continue
        cleaned_args.append(arg)

    if force_console:
        os.environ["AV1_FORCE_CONSOLE"] = "1"
    if load_debug_flag:
        os.environ["AV1_LOAD_DEBUG"] = "1"
        core.LOAD_DEBUG = True
    if os.environ.get("AV1_AUTO_TEST") == "1" and not os.environ.get("AV1_AUTO_TEST_PROFILE"):
        os.environ["AV1_AUTO_TEST_PROFILE"] = "medium"
    if not autotest_profile:
        autotest_profile = (os.environ.get("AV1_AUTO_TEST_PROFILE") or "").strip().lower()
    sys.argv = cleaned_args

    # Logging to file - saving original stdout/stderr FIRST
    # Use sys.stdout because earlier forceconsole logic might have redirected it (AllocConsole)
    # sys.__stdout__ remains None in pythonw execution even after AllocConsole
    
    # CRITICAL FIX: Unwrap STDOUT_ROUTER if present to avoid infinite recursion
    # If sys.stdout is already our router, grabbing it as 'original' and plugging it back 
    # into the router's fallback creates a loop.
    current_stdout = sys.stdout
    if hasattr(core, 'ThreadSafeStdoutRouter') and isinstance(current_stdout, core.ThreadSafeStdoutRouter):
         original_stdout = current_stdout._fallback
    else:
         original_stdout = current_stdout

    current_stderr = sys.stderr
    if hasattr(core, 'ThreadSafeStdoutRouter') and isinstance(current_stderr, core.ThreadSafeStdoutRouter):
         original_stderr = current_stderr._fallback
    else:
         original_stderr = current_stderr

    # Determine APP_ROOT
    # If running as pyz (zipapp), sys.argv[0] is the .pyz file path
    # If running as script, __file__ is inside the package
    # Note: sys.argv[0] might not exist or be empty in some environments, check os.path.exists
    is_pyz = False
    if sys.argv[0] and os.path.isfile(sys.argv[0]):
        try:
            if zipfile.is_zipfile(sys.argv[0]):
                is_pyz = True
        except Exception:
            pass

    if is_pyz:
        core.APP_ROOT = Path(sys.argv[0]).resolve().parent
    elif getattr(sys, 'frozen', False):
        # Frozen exe: APP_ROOT is already set correctly in module init (line 234-235)
        # Don't override it with __file__ which points to temp extraction folder
        pass
    else:
        # Standard script execution: project root is parent of av1_recompress package
        core.APP_ROOT = Path(__file__).resolve().parent.parent

    # Log file path: project root directory
    log_path = core.APP_ROOT / "av1_recompress.log"
    log_bak_path = core.APP_ROOT / "av1_recompress.log.bak"
    log_file = None

    def _rotate_main_log_to_backup(src_path: Path, bak_path: Path, retries: int = 3) -> bool:
        """Create/overwrite .bak from previous main log before opening a new one."""
        if not src_path.exists():
            return False

        for attempt in range(retries):
            try:
                # Overwrite backup with the previous run's full log.
                shutil.copy2(src_path, bak_path)
                return True
            except (OSError, PermissionError):
                if attempt < retries - 1:
                    import time
                    time.sleep(0.3)
                else:
                    return False
        return False

    class TimestampedFileWriter:
        """Thread-safe line-based timestamp wrapper for file logs."""
        def __init__(self, file_obj):
            self._file = file_obj
            self._lock = threading.Lock()
            self._line_start = True

        def _timestamp_prefix(self):
            return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] "

        def write(self, data):
            if data is None:
                return 0
            text = str(data)
            if not text:
                return 0
            try:
                if hasattr(core, "sanitize_log_text"):
                    text = core.sanitize_log_text(text)
            except Exception:
                pass
            with self._lock:
                chunks = text.splitlines(keepends=True)
                if not chunks:
                    chunks = [text]
                out = []
                for chunk in chunks:
                    # Do not prefix blank separator-only lines.
                    if self._line_start and chunk.strip("\r\n"):
                        out.append(self._timestamp_prefix())
                    out.append(chunk)
                    self._line_start = chunk.endswith("\n")
                self._file.write("".join(out))
            return len(text)

        def flush(self):
            with self._lock:
                self._file.flush()

        def close(self):
            with self._lock:
                self._file.close()

        @property
        def closed(self):
            return self._file.closed

        @property
        def name(self):
            return self._file.name

        @property
        def encoding(self):
            return getattr(self._file, "encoding", "utf-8")

        def __getattr__(self, attr):
            return getattr(self._file, attr)

    # Backup previous run log before creating a fresh av1_recompress.log.
    # Requested behavior: always overwrite av1_recompress.log.bak.
    _backup_ok = _rotate_main_log_to_backup(log_path, log_bak_path, retries=3)
    if log_path.exists() and not _backup_ok and sys.stderr:
        try:
            sys.stderr.write(f"WARNING: Could not create backup log: {log_bak_path}\n")
        except Exception:
            pass
    
    # Try to open log file with retry mechanism (crucial for restart race conditions)
    for i in range(3):
        try:
            raw_log_file = open(log_path, "w", encoding="utf-8")
            log_file = TimestampedFileWriter(raw_log_file)
            break
        except (IOError, PermissionError) as e:
            if i < 2:
                import time
                time.sleep(0.5)
            else:
                 # Fallback to temp directory if write permission denied or locked
                try:
                    import tempfile
                    temp_log_path = Path(tempfile.gettempdir()) / "av1_recompress.log"
                    raw_log_file = open(temp_log_path, "w", encoding="utf-8")
                    log_file = TimestampedFileWriter(raw_log_file)
                    # Try to warn about fallback
                    if sys.stderr:
                        try:
                            sys.stderr.write(f"WARNING: Could not write to {log_path}, falling back to {temp_log_path}\n")
                        except (OSError, IOError, AttributeError, ValueError):
                            # stderr write failed - not critical
                            pass
                except (OSError, IOError, PermissionError):
                    # Temp log file creation also failed - give up
                    log_file = None
    
    core.LOG_WRITER = log_file

    # Set MKVMERGE_PATH properly using the resolved APP_ROOT
    mkvmerge_exe = core.APP_ROOT / "mkvmerge.exe"
    if mkvmerge_exe.exists():
        core.MKVMERGE_PATH = str(mkvmerge_exe)
    else:
        # If not in APP_ROOT, check if it's in the same dir as the executable (for dist)
        if getattr(sys, 'frozen', False):
             exe_dir_mkvmerge = Path(sys.executable).parent / "mkvmerge.exe"
             if exe_dir_mkvmerge.exists():
                 core.MKVMERGE_PATH = str(exe_dir_mkvmerge)
             else:
                 core.MKVMERGE_PATH = "mkvmerge"
        else:
             core.MKVMERGE_PATH = "mkvmerge"  # Fallback to PATH

    if core.VIDEO_LOADING_DEBUG:
        core.init_video_loading_log()

    class TeeOutput:
        def __init__(self, file_obj, original):
            self.file = file_obj
            self.original = original

        def write(self, data):
            safe_data = data
            try:
                if hasattr(core, "sanitize_log_text"):
                    safe_data = core.sanitize_log_text(data)
            except Exception:
                safe_data = data

            # Write to log file
            try:
                if self.file and not self.file.closed:
                    self.file.write(safe_data)
                    self.file.flush()
            except (OSError, IOError, AttributeError):
                pass
            
            # Write to original output (Console)
            if self.original:
                try:
                    self.original.write(safe_data)
                    self.original.flush()
                except UnicodeEncodeError:
                    try:
                        fallback_data = safe_data.encode(self.original.encoding or 'utf-8', errors='replace').decode(self.original.encoding or 'utf-8')
                        self.original.write(fallback_data)
                        self.original.flush()
                    except Exception:
                        pass
                except Exception:
                    # e.g. console window closed
                    pass

        def flush(self):
            try:
                if self.file and not self.file.closed:
                    self.file.flush()
            except (OSError, IOError, AttributeError):
                pass
            if self.original:
                try:
                    self.original.flush()
                except (OSError, IOError, AttributeError):
                    pass

    # Setup Console Output explicitly if forced
    console_out_file = None
    if force_console and sys.platform == "win32":
        try:
            # We assume AllocConsole was called earlier or console exists
            console_out_file = open("CONOUT$", "w", encoding="utf-8")
        except Exception:
            pass
    
    # Determine the real console stream (or None)
    target_console = console_out_file if console_out_file else original_stdout

    tee_stdout = TeeOutput(log_file, target_console)
    tee_stderr = TeeOutput(log_file, original_stderr if original_stderr else console_out_file)

    # CRITICAL: Do NOT overwrite sys.stdout, because core_paths_tools_logging already 
    # replaced it with STDOUT_ROUTER. We must inject our TeeOutput as the fallback 
    # for the router.
    if hasattr(core, 'STDOUT_ROUTER'):
        core.STDOUT_ROUTER._fallback = tee_stdout
    else:
        # Should not happen given imports, but failsafe
        sys.stdout = tee_stdout

    # stderr is not routed by default, so we can replace it
    sys.stderr = tee_stderr

    tee_stdout.write("=== PROGRAM START ===\n")
    tee_stdout.write(f"Python version: {sys.version}\n")
    tee_stdout.write(f"Executable: {sys.executable}\n")
    tee_stdout.write(f"Platform: {sys.platform}\n")

    if sys.platform == "win32" and not os.environ.get("AV1_FORCE_CONSOLE"):
        tee_stdout.write("Checking pythonw.exe...\n")
        if not sys.executable.endswith("pythonw.exe"):
            pythonw_exe = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
            tee_stdout.write(f"pythonw.exe path: {pythonw_exe}\n")
            if os.path.exists(pythonw_exe):
                tee_stdout.write("pythonw.exe exists, restarting...\n")
                # Due to modular structure, starting with pythonw from module (-m) so relative imports work.
                # Propagate arguments
                args = [pythonw_exe, "-m", "av1_recompress.app"]
                if http_enabled:
                    args.append("--http")
                    args.append(f"--port:{http_port}")
                
                 # Important: Restore flags
                if short_test:
                     args.append("--autotest-mini")
                if load_debug_flag:
                     args.append("--load-debug")
                if core.VIDEO_LOADING_DEBUG: # Using core variable because env var might be set
                     args.append("--videoloading")
                
                # Do NOT append --forceconsole here, as this branch is explicitly for NOT forced console
                
                # IMPORTANT: Set CWD to APP_ROOT so pythonw -m can find the package
                # core.APP_ROOT is resolved, so it should be safe
                cwd = str(core.APP_ROOT)
                
                # Also force flush log to ensure we see "restarting" message
                tee_stdout.flush()
                if log_file and not log_file.closed:
                    log_file.flush()
                    log_file.close()
                
                subprocess.Popen(args, cwd=cwd)
                sys.exit(0)
            else:
                tee_stdout.write("pythonw.exe does NOT exist\n")
        else:
            tee_stdout.write("Already running in pythonw.exe\n")

        try:
            tee_stdout.write("Initializing Tk()...\n")
            root = tk.Tk()
            tee_stdout.write("Tk() created\n")

            tee_stdout.write("Initializing VideoEncoderGUI...\n")
            app = VideoEncoderGUI(root, http_enabled=http_enabled, http_port=http_port)
            tee_stdout.write("VideoEncoderGUI created\n")
            tee_stdout.write(f"[OK] SQLite database file: {app.db_path}\n")

            if _is_autotest_active(short_test=short_test, autotest_profile=autotest_profile):
                log_target = getattr(core, "LOG_WRITER", None) or tee_stdout
                _start_autotest_force_exit_watchdog(
                    root,
                    log_sink=log_target,
                    close_callback=getattr(app, "_autotest_exit", None),
                    timeout_seconds=AUTOTEST_FORCE_EXIT_SECONDS,
                )

            if short_test:

                def _short_test_exit():

                    msg = "\n=== AUTOTEST MINI mode: 10 seconds elapsed, automatic exit ===\n"

                    try:

                        tee_stdout.write(msg)

                    except Exception:

                        print(msg)

                    try:

                        exit_cb = getattr(app, "_autotest_exit", None)
                        if callable(exit_cb):
                            exit_cb()
                            return

                    except Exception:

                        pass

                    try:

                        root.quit()

                    except Exception:

                        pass

                    try:

                        root.destroy()

                    except Exception:

                        pass

                root.after(10_000, _short_test_exit)

            tee_stdout.write("Starting mainloop()...\n")
            root.mainloop()
            tee_stdout.write("mainloop() finished\n")
        except Exception as e:
            tee_stderr.write(f"ERROR: {e}\n")
            import traceback
            traceback.print_exc(file=tee_stderr)
        finally:
            log_file.close()
    else:
        try:
            main(short_test=short_test, http_enabled=http_enabled, http_port=http_port, autotest_profile=autotest_profile)
        finally:
            try:
                log_file.close()
            except Exception:
                pass


if __name__ == "__main__":
    run()
