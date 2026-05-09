def batch_scan_directory(directory, progress_callback=None):
    """Batch scan directory for all files with size and mtime.
    
    Uses os.scandir() for efficient directory traversal - 10-100× faster
    than individual stat() calls. Recursively scans subdirectories.
    
    Args:
        directory: Path object or string to directory to scan.
        progress_callback: Optional callback(scanned_count) called periodically.
        
    Returns:
        dict: {Path: {'size': int, 'mtime': float}} for all files in directory tree.
              Returns empty dict on errors.
    
    Example:
        scan = batch_scan_directory(Path('/videos'))
        # {Path('/videos/video1.mp4'): {'size': 123456, 'mtime': 1234567890.0}, ...}
    """
    scan_results = {}
    # Counter for progress callback
    scan_counter = [0]
    
    def scan_dir_recursive(dir_path, _visited=None):
        """Recursive helper for scanning."""
        if _visited is None:
            _visited = set()
        try:
            real_path = Path(dir_path).resolve()
            if real_path in _visited:
                return
            _visited.add(real_path)
        except (OSError, ValueError):
            return
        try:
            with os.scandir(dir_path) as entries:
                for entry in entries:
                    try:
                        # is_file() and is_dir() are also cached - no extra I/O!
                        if entry.is_file(follow_symlinks=False):
                            # entry.stat() reuses already cached stat info - very fast!
                            stat_info = entry.stat(follow_symlinks=False)
                            scan_results[Path(entry.path)] = {
                                'size': stat_info.st_size,
                                'mtime': stat_info.st_mtime
                            }
                            scan_counter[0] += 1
                            # Call progress callback every 500 files
                            if progress_callback and scan_counter[0] % 500 == 0:
                                try:
                                    progress_callback(scan_counter[0])
                                except Exception:
                                    pass
                        elif entry.is_dir(follow_symlinks=False):
                            # Recursive scan for subdirectories
                            try:
                                scan_dir_recursive(Path(entry.path), _visited)
                            except (OSError, PermissionError):
                                # Skip inaccessible subdirectories
                                continue
                    except (OSError, PermissionError):
                        # Skip inaccessible files
                        continue
        except (OSError, PermissionError):
            pass
    
    try:
        directory_path = Path(directory) if not isinstance(directory, Path) else directory
        
        if not directory_path.exists() or not directory_path.is_dir():
            return scan_results
        
        scan_dir_recursive(directory_path)
        
        # Final callback with total count
        if progress_callback:
            try:
                progress_callback(scan_counter[0])
            except Exception:
                pass
            
    except (OSError, PermissionError, TypeError):
        # Return whatever we managed to scan
        pass
    
    return scan_results


def is_directory_completely_empty(directory):
    """Check if a directory is missing or completely empty (recursively contains no files).
    
    Args:
        directory: Path to the directory.
        
    Returns:
        bool: True if directory is empty or missing, False otherwise.
    """
    try:
        path = Path(directory)
    except (TypeError, ValueError, OSError):
        return True
    try:
        if not path.exists():
            return True
        for _, _, files in os.walk(path):
            if files:
                return False
        return True
    except (OSError, PermissionError):
        return False
