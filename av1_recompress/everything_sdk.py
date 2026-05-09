# Everything SDK wrapper for ultra-fast file scanning
# Uses voidtools Everything's MFT-based search when available
# Falls back to os.scandir() if Everything is not installed

import os
import sys
import ctypes
from pathlib import Path

# Everything SDK constants
EVERYTHING_OK = 0
EVERYTHING_ERROR_MEMORY = 1
EVERYTHING_ERROR_IPC = 2
EVERYTHING_ERROR_REGISTERCLASSEX = 3
EVERYTHING_ERROR_CREATEWINDOW = 4
EVERYTHING_ERROR_CREATETHREAD = 5
EVERYTHING_ERROR_INVALIDINDEX = 6
EVERYTHING_ERROR_INVALIDCALL = 7

# Request flags
EVERYTHING_REQUEST_FILE_NAME = 0x00000001
EVERYTHING_REQUEST_PATH = 0x00000002
EVERYTHING_REQUEST_FULL_PATH_AND_FILE_NAME = 0x00000004
EVERYTHING_REQUEST_SIZE = 0x00000010
EVERYTHING_REQUEST_DATE_MODIFIED = 0x00000040

# Global state
_everything_dll = None
_everything_available = None


def _load_everything_dll():
    """Load Everything64.dll or Everything32.dll based on Python architecture."""
    global _everything_dll, _everything_available
    
    if _everything_available is not None:
        return _everything_available
    
    if sys.platform != 'win32':
        _everything_available = False
        return False
    
    try:
        # Determine DLL name based on Python architecture
        dll_name = "Everything64.dll" if sys.maxsize > 2**32 else "Everything32.dll"
        
        # Search paths for DLL
        search_paths = [
            # Current directory
            Path.cwd() / dll_name,
            # Same directory as script
            Path(__file__).parent / dll_name,
            # Common Everything install locations (dinamikus környezeti változókkal)
            Path(os.environ.get('ProgramFiles', 'C:\\Program Files')) / 'Everything' / dll_name if os.environ.get('ProgramFiles') else None,
            Path(os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)')) / 'Everything' / dll_name if os.environ.get('ProgramFiles(x86)') else None,
            Path(os.environ.get('LOCALAPPDATA', '')) / 'Everything' / dll_name if os.environ.get('LOCALAPPDATA') else None,
            # System path
            Path(os.environ.get('SystemRoot', 'C:\\Windows')) / 'System32' / dll_name if os.environ.get('SystemRoot') else None,
        ]
        # Filter out None values
        search_paths = [p for p in search_paths if p is not None]
        
        dll_path = None
        for path in search_paths:
            if path.exists():
                dll_path = str(path)
                break
        
        if dll_path is None:
            # Try loading from PATH
            dll_path = dll_name
        
        _everything_dll = ctypes.WinDLL(dll_path)
        
        # Set up function signatures
        _everything_dll.Everything_SetSearchW.argtypes = [ctypes.c_wchar_p]
        _everything_dll.Everything_SetRequestFlags.argtypes = [ctypes.c_uint32]
        _everything_dll.Everything_QueryW.argtypes = [ctypes.c_int]
        _everything_dll.Everything_QueryW.restype = ctypes.c_int
        _everything_dll.Everything_GetNumResults.restype = ctypes.c_uint32
        _everything_dll.Everything_GetResultFullPathNameW.argtypes = [ctypes.c_uint32, ctypes.c_wchar_p, ctypes.c_uint32]
        _everything_dll.Everything_GetResultFullPathNameW.restype = ctypes.c_uint32
        _everything_dll.Everything_GetResultSize.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint64)]
        _everything_dll.Everything_GetResultSize.restype = ctypes.c_int
        _everything_dll.Everything_GetResultDateModified.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint64)]
        _everything_dll.Everything_GetResultDateModified.restype = ctypes.c_int
        _everything_dll.Everything_GetLastError.restype = ctypes.c_uint32
        _everything_dll.Everything_CleanUp.restype = None
        
        # Test if Everything is running by doing a simple query
        _everything_dll.Everything_SetSearchW("")
        _everything_dll.Everything_QueryW(True)
        error = _everything_dll.Everything_GetLastError()
        
        if error == EVERYTHING_ERROR_IPC:
            # Everything is not running
            print("[Everything SDK] Everything is not running. Install and run Everything for faster scanning.")
            _everything_available = False
            _everything_dll = None
            return False
        
        print(f"[Everything SDK] Loaded successfully: {dll_path}")
        _everything_available = True
        return True
        
    except (OSError, AttributeError, Exception) as e:
        print(f"[Everything SDK] Not available: {e}")
        _everything_available = False
        _everything_dll = None
        return False


def everything_scan_directory(directory, extensions=None, progress_callback=None):
    """Scan directory using Everything SDK (ultra-fast MFT-based search).
    
    Args:
        directory: Path to directory to scan.
        extensions: Optional list of file extensions to filter (e.g., ['.mp4', '.mkv']).
        progress_callback: Optional callback(scanned_count) called periodically.
        
    Returns:
        dict: {Path: {'size': int, 'mtime': float}} for all matching files.
              Returns None if Everything is not available (use fallback).
    """
    if not _load_everything_dll():
        return None
    
    try:
        # IMPORTANT: Do NOT use resolve() here!
        # resolve() converts drive letters (Y:\) to UNC paths (\\192.168.50.124\...)
        # but Everything indexes by drive letter, not UNC path
        directory_path = Path(directory)
        if not directory_path.is_absolute():
            directory_path = directory_path.absolute()
        
        # Use the string representation directly (preserves drive letter)
        search_dir = str(directory_path)
        
        # Build search query
        # Format: "Y:\Videos\" (with trailing backslash for exact folder match)
        search_parts = [f'"{search_dir}\\"']
        
        if extensions:
            # Clean extensions (remove dots, make lowercase)
            ext_list = [e.lstrip('.').lower() for e in extensions]
            search_parts.append(f"ext:{';'.join(ext_list)}")
        
        search_query = ' '.join(search_parts)
        
        # Set search parameters
        _everything_dll.Everything_SetSearchW(search_query)
        _everything_dll.Everything_SetRequestFlags(
            EVERYTHING_REQUEST_FULL_PATH_AND_FILE_NAME | 
            EVERYTHING_REQUEST_SIZE | 
            EVERYTHING_REQUEST_DATE_MODIFIED
        )
        
        # Execute query (wait for results)
        if not _everything_dll.Everything_QueryW(True):
            error = _everything_dll.Everything_GetLastError()
            if error == EVERYTHING_ERROR_IPC:
                print("[Everything SDK] IPC error - Everything may not be running")
                return None
            return None
        
        num_results = _everything_dll.Everything_GetNumResults()
        print(f"[Everything SDK] Found {num_results:,} files in {search_dir}")
        
        # Collect results
        scan_results = {}
        path_buffer = ctypes.create_unicode_buffer(32768)  # MAX_PATH * 4
        size_value = ctypes.c_uint64()
        mtime_value = ctypes.c_uint64()
        
        for i in range(num_results):
            # Get full path
            path_len = _everything_dll.Everything_GetResultFullPathNameW(i, path_buffer, 32768)
            if path_len > 0:
                file_path = Path(path_buffer.value)
                
                # Get size
                size = 0
                if _everything_dll.Everything_GetResultSize(i, ctypes.byref(size_value)):
                    size = size_value.value
                
                # Get mtime (FILETIME format - 100-nanosecond intervals since 1601)
                mtime = 0.0
                if _everything_dll.Everything_GetResultDateModified(i, ctypes.byref(mtime_value)):
                    # Convert FILETIME to Unix timestamp
                    # FILETIME is 100-nanosecond intervals since January 1, 1601
                    # Unix epoch is January 1, 1970
                    # Difference: 11644473600 seconds
                    filetime = mtime_value.value
                    mtime = (filetime / 10000000.0) - 11644473600.0
                
                scan_results[file_path] = {
                    'size': size,
                    'mtime': mtime
                }
                
                # Progress callback every 1000 files
                if progress_callback and (i + 1) % 1000 == 0:
                    progress_callback(i + 1)
        
        # Final progress callback
        if progress_callback:
            progress_callback(num_results)
        
        # Cleanup
        _everything_dll.Everything_CleanUp()
        
        return scan_results
        
    except Exception as e:
        print(f"[Everything SDK] Error during scan: {e}")
        return None


def is_everything_available():
    """Check if Everything SDK is available and running."""
    return _load_everything_dll()


def everything_find_file(filename, prefer_path_contains=None):
    """Find a file by name using Everything SDK.
    
    Args:
        filename: Name of the file to find (e.g., 'vspipe.exe')
        prefer_path_contains: Optional string to prefer paths containing this substring
                             (e.g., 'Hybrid' to prefer paths in Hybrid folder)
        
    Returns:
        Path: Full path to the file if found, None otherwise
    """
    if not _load_everything_dll():
        return None
    
    try:
        # Search for exact filename
        search_query = f'"{filename}"'
        
        _everything_dll.Everything_SetSearchW(search_query)
        _everything_dll.Everything_SetRequestFlags(EVERYTHING_REQUEST_FULL_PATH_AND_FILE_NAME)
        
        if not _everything_dll.Everything_QueryW(True):
            error = _everything_dll.Everything_GetLastError()
            if error == EVERYTHING_ERROR_IPC:
                return None
            return None
        
        num_results = _everything_dll.Everything_GetNumResults()
        if num_results == 0:
            _everything_dll.Everything_CleanUp()
            return None
        
        # Collect all results
        results = []
        path_buffer = ctypes.create_unicode_buffer(32768)
        
        for i in range(min(num_results, 50)):  # Limit to 50 results for speed
            path_len = _everything_dll.Everything_GetResultFullPathNameW(i, path_buffer, 32768)
            if path_len > 0:
                file_path = Path(path_buffer.value)
                if file_path.exists():
                    results.append(file_path)
        
        _everything_dll.Everything_CleanUp()
        
        if not results:
            return None
        
        # If preference is set, try to find matching path first
        if prefer_path_contains:
            for path in results:
                if prefer_path_contains.lower() in str(path).lower():
                    return path
        
        # Return first valid result
        return results[0]
        
    except Exception as e:
        print(f"[Everything SDK] Error finding file '{filename}': {e}")
        return None


# Video extensions for filtering
VIDEO_EXTENSIONS_FOR_EVERYTHING = [
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
    '.mpg', '.mpeg', '.m2ts', '.mts', '.ts', '.vob', '.3gp', '.3g2',
    '.ogv', '.ogm', '.divx', '.xvid', '.rm', '.rmvb', '.asf'
]
