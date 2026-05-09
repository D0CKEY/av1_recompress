from .gui_imports import *
from .gui_shared import *

def _infer_output_encoder_type(status_code, status_text=""):
    """Infer encoder type without probing when status already encodes it."""
    code = str(status_code or "").strip().lower()
    text = str(status_text or "").strip().lower()

    if code == "completed_nvenc":
        return "nvenc"
    if code == "completed_svt":
        return "svt-av1"

    if "nvenc" in text:
        return "nvenc"
    if "svt-av1" in text or "svt" in text:
        return "svt-av1"

    return None

class DBAndStateLoadMixin:
    TREE_ITEM_SCHEMA_VERSION = 2
    TREE_ITEM_KEY_ALIASES = {
        'new_size_bytes': 'output_size_bytes',
        'output_file_size_bytes': 'output_size_bytes',
        'output_encoder_type': 'encoder_type',
    }

    def _rebuild_manual_status_text(self, base_status_text, manual_cq_value=None, manual_quality_check=None):
        """Rebuild status text with manual CQ suffix from raw cached/db metadata."""
        base_status_text = str(base_status_text or "")
        if not base_status_text or " (M " in base_status_text:
            return base_status_text
        if manual_cq_value is None:
            return base_status_text
        try:
            cq_value = int(manual_cq_value)
        except (TypeError, ValueError):
            return base_status_text

        if hasattr(self, '_format_manual_status_suffix'):
            try:
                return base_status_text + self._format_manual_status_suffix(cq_value, manual_quality_check)
            except Exception:
                pass

        quality_text = str(manual_quality_check or '').strip().upper()
        return f"{base_status_text} (M CQ:{cq_value}{(' ' + quality_text) if quality_text else ''})"

    def _canonical_tree_item_key(self, key):
        if key is None:
            return None
        return self.TREE_ITEM_KEY_ALIASES.get(key, key)

    def _migrate_tree_item_cache(self, item_cache):
        if not isinstance(item_cache, dict):
            return {}

        # Legacy -> canonical aliases.
        for legacy_key, canonical_key in self.TREE_ITEM_KEY_ALIASES.items():
            legacy_value = item_cache.get(legacy_key)
            canonical_value = item_cache.get(canonical_key)
            if canonical_value is None and legacy_value is not None:
                item_cache[canonical_key] = legacy_value

        # Canonical -> legacy mirrors for backward compatibility.
        if item_cache.get('output_size_bytes') is not None:
            item_cache['new_size_bytes'] = item_cache.get('output_size_bytes')
        if item_cache.get('encoder_type'):
            item_cache['output_encoder_type'] = item_cache.get('encoder_type')
        elif item_cache.get('output_encoder_type'):
            item_cache['encoder_type'] = item_cache.get('output_encoder_type')

        item_cache['__schema_version'] = self.TREE_ITEM_SCHEMA_VERSION
        return item_cache

    def _ensure_tree_item_cache(self, item_id):
        if not hasattr(self, 'tree_item_data') or self.tree_item_data is None:
            self.tree_item_data = {}
        item_cache = self.tree_item_data.get(item_id)
        if not isinstance(item_cache, dict):
            item_cache = {}
            self.tree_item_data[item_id] = item_cache
        return self._migrate_tree_item_cache(item_cache)

    def get_tree_item_meta(self, item_id, key=None, default=None):
        if not hasattr(self, 'tree_item_data') or self.tree_item_data is None:
            return {} if key is None else default

        item_cache = self.tree_item_data.get(item_id)
        if not isinstance(item_cache, dict):
            return {} if key is None else default

        item_cache = self._migrate_tree_item_cache(item_cache)
        if key is None:
            return dict(item_cache)

        canonical_key = self._canonical_tree_item_key(key)
        if canonical_key in item_cache:
            value = item_cache.get(canonical_key)
            return default if value is None else value

        value = item_cache.get(key, default)
        return default if value is None else value

    def set_tree_item_meta(self, item_id, **kwargs):
        if not kwargs:
            return self._ensure_tree_item_cache(item_id)

        item_cache = self._ensure_tree_item_cache(item_id)
        for key, value in kwargs.items():
            if value is None:
                continue
            canonical_key = self._canonical_tree_item_key(key)
            item_cache[canonical_key] = value

            # Keep legacy mirrors in sync.
            if canonical_key == 'output_size_bytes':
                item_cache['new_size_bytes'] = value
            elif canonical_key == 'encoder_type':
                item_cache['output_encoder_type'] = value

        return self._migrate_tree_item_cache(item_cache)

    def merge_tree_item_meta(self, item_id, meta):
        if not meta:
            return self._ensure_tree_item_cache(item_id)
        if not isinstance(meta, dict):
            return self._ensure_tree_item_cache(item_id)
        return self.set_tree_item_meta(item_id, **meta)

    def remove_tree_item_meta_keys(self, item_id, *keys):
        if not keys or not hasattr(self, 'tree_item_data'):
            return
        item_cache = self.tree_item_data.get(item_id)
        if not isinstance(item_cache, dict):
            return

        for key in keys:
            canonical_key = self._canonical_tree_item_key(key)
            item_cache.pop(key, None)
            item_cache.pop(canonical_key, None)
            if canonical_key == 'output_size_bytes':
                item_cache.pop('new_size_bytes', None)
            elif canonical_key == 'encoder_type':
                item_cache.pop('output_encoder_type', None)
        self._migrate_tree_item_cache(item_cache)

    def _normalize_extra_metadata_dict(self, metadata):
        if not isinstance(metadata, dict):
            return None

        color_metadata = metadata.get('color_metadata')
        if not isinstance(color_metadata, dict):
            color_metadata = {}

        raw_streams = metadata.get('streams')
        if not isinstance(raw_streams, dict):
            raw_streams = {}

        cleaned_streams = {'video': [], 'audio': [], 'subtitle': []}
        for stream_type in ('video', 'audio', 'subtitle'):
            entries = raw_streams.get(stream_type)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                cleaned_entry = {
                    'index': entry.get('index'),
                    'codec': entry.get('codec'),
                    'lang': entry.get('lang'),
                    'title': entry.get('title'),
                    'default': (str(entry.get('default')).lower() in ('1', 'true', 'yes')) if entry.get('default') is not None else False,
                    'forced': (str(entry.get('forced')).lower() in ('1', 'true', 'yes')) if entry.get('forced') is not None else False,
                    'start_time_ms': entry.get('start_time_ms'),
                    'bit_rate': entry.get('bit_rate'),
                    'size_bytes': entry.get('size_bytes'),
                }
                if stream_type == 'video':
                    cleaned_entry['width'] = entry.get('width')
                    cleaned_entry['height'] = entry.get('height')
                    cleaned_entry['rotation'] = entry.get('rotation')
                elif stream_type == 'audio':
                    cleaned_entry['channels'] = entry.get('channels')

                has_stream_value = any(
                    cleaned_entry.get(key) not in (None, "", [], {})
                    for key in cleaned_entry.keys()
                    if key not in ('default', 'forced')
                ) or cleaned_entry.get('default') or cleaned_entry.get('forced')
                if has_stream_value:
                    cleaned_streams[stream_type].append(cleaned_entry)

        cleaned = {
            'width': metadata.get('width'),
            'height': metadata.get('height'),
            'rotation': metadata.get('rotation'),
            'file_size': metadata.get('file_size'),
            'bit_rate': metadata.get('bit_rate'),
            'color_metadata': {
                'pix_fmt': color_metadata.get('pix_fmt'),
                'color_space': color_metadata.get('color_space'),
                'color_primaries': color_metadata.get('color_primaries'),
                'color_transfer': color_metadata.get('color_transfer'),
                'color_range': color_metadata.get('color_range'),
                'sample_aspect_ratio': color_metadata.get('sample_aspect_ratio'),
                'display_aspect_ratio': color_metadata.get('display_aspect_ratio'),
            },
            'streams': cleaned_streams
        }

        has_top_level_value = any(cleaned.get(key) not in (None, "", [], {}) for key in ('width', 'height', 'rotation', 'file_size', 'bit_rate'))
        has_color_value = any(value not in (None, "", [], {}) for value in cleaned['color_metadata'].values())
        has_stream_value = any(cleaned['streams'].get(stream_type) for stream_type in ('video', 'audio', 'subtitle'))

        if not has_top_level_value and not has_color_value and not has_stream_value:
            return None
        return cleaned

    def _serialize_extra_metadata(self, metadata):
        normalized = self._normalize_extra_metadata_dict(metadata)
        if not normalized:
            return None
        try:
            return json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        except (TypeError, ValueError):
            return None

    def _extra_metadata_has_any_stream_rows(self, metadata):
        """Return True when cached extra metadata contains at least one parsed stream row."""
        if not isinstance(metadata, dict):
            return False

        streams = metadata.get('streams')
        if not isinstance(streams, dict):
            return False

        for stream_type in ('video', 'audio', 'subtitle'):
            entries = streams.get(stream_type)
            if isinstance(entries, list) and any(isinstance(entry, dict) for entry in entries):
                return True
        return False

    def _extra_metadata_has_size_and_bitrate(self, metadata):
        """Return True when metadata has file_size and streams have size/bitrate data."""
        if not isinstance(metadata, dict):
            return False

        if metadata.get('file_size') is None:
            return False

        streams = metadata.get('streams')
        if not isinstance(streams, dict):
            return False

        for stype in ('audio', 'subtitle'):
            for entry in streams.get(stype) or []:
                if not isinstance(entry, dict):
                    continue
                if entry.get('size_bytes') is not None or entry.get('bit_rate') is not None:
                    return True

        for entry in streams.get('video') or []:
            if not isinstance(entry, dict):
                continue
            if entry.get('size_bytes') is not None or entry.get('bit_rate') is not None:
                return True

        return False

    def _diagnose_metadata_readiness(self, metadata, label=""):
        """Return a string describing why metadata is not ready (for logging)."""
        if not isinstance(metadata, dict):
            return f"{label}: not a dict ({type(metadata).__name__})"
        reasons = []
        if metadata.get('file_size') is None:
            reasons.append("file_size=None")
        streams = metadata.get('streams')
        if not isinstance(streams, dict):
            reasons.append("streams missing")
        else:
            has_any = False
            for stype in ('audio', 'subtitle', 'video'):
                for entry in streams.get(stype) or []:
                    if not isinstance(entry, dict):
                        continue
                    if entry.get('size_bytes') is not None or entry.get('bit_rate') is not None:
                        has_any = True
                        break
                if has_any:
                    break
            if not has_any:
                reasons.append("no stream has size_bytes/bit_rate")
        if not reasons:
            return f"{label}: READY"
        return f"{label}: " + ", ".join(reasons)

    def _deserialize_extra_metadata(self, raw_value):
        if raw_value in (None, "", b""):
            return None
        if isinstance(raw_value, dict):
            return self._normalize_extra_metadata_dict(raw_value)
        try:
            return self._normalize_extra_metadata_dict(json.loads(raw_value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def _init_database(self):
        """Initialize SQLite database and create tables."""
        # Use lock - ensures only one database operation runs at a time
        with self.db_lock:
            conn = None
            try:
                # Retry logic for SQLITE_BUSY errors
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                        break  # Successful connection
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[_init_database] Database locked, retrying {attempt + 1}/{max_retries}...")
                            time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            raise  # Other error or last attempt
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                conn.commit()
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if LOAD_DEBUG:
                    load_debug_log(f"[_init_database] Error: {e}")
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[ERROR] SQLite database initialization error: {e}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _reconstruct_tree_values_from_cache(self, cached_data, video_path):
        """Reconstruct tree values array from tree_item_data cache.

        This is used when app is closing to avoid deadlock from accessing
        the tree widget from a background thread after root.destroy().

        Args:
            cached_data: dict from tree_item_data cache
            video_path: Path object for the video

        Returns:
            list: Values array matching tree column structure, or empty list
        """
        if not cached_data:
            return []

        try:
            denoise_display = cached_data.get('denoise_display', '')
            if not denoise_display:
                denoise_display = denoise_level_to_display(cached_data.get('denoise_enabled', 0))
            status_display = cached_data.get('status_display', cached_data.get('status', '-'))
            status_display = self._rebuild_manual_status_text(
                status_display,
                cached_data.get('manual_cq_value'),
                cached_data.get('manual_quality_check')
            )

            # Column order: denoise, hard_rotate, video_name, status, cq, vmaf, psnr, progress,
            #               orig_size, new_size, size_change, duration, frames, completed_date
            values = [
                denoise_display,
                hard_rotate_to_display(cached_data.get('hard_rotate_degrees', 0)),
                video_path.name if hasattr(video_path, 'name') else str(video_path),
                status_display,
                str(cached_data.get('cq', '-')) if cached_data.get('cq') is not None else '-',
                str(cached_data.get('vmaf', '-')) if cached_data.get('vmaf') is not None else '-',
                str(cached_data.get('psnr', '-')) if cached_data.get('psnr') is not None else '-',
                cached_data.get('progress_display', cached_data.get('progress', '-')),
                cached_data.get('orig_size_display', cached_data.get('orig_size_str', '-')),
                cached_data.get('new_size_display', cached_data.get('new_size_str', '-')),
                cached_data.get('size_change_display', cached_data.get('size_change', '-')),
                cached_data.get('duration_display', cached_data.get('duration', '-')),
                cached_data.get('frames_display', cached_data.get('frames', '-')),
                cached_data.get('completed_date', '')
            ]
            return values
        except Exception:
            return []

    def _ensure_db_tables(self, cursor):
        """Ensures that necessary tables exist in the database."""
        try:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_path TEXT PRIMARY KEY,
                output_path TEXT,
                order_number INTEGER,
                video_name TEXT,
                status TEXT,
                status_code TEXT,
                cq TEXT,
                vmaf TEXT,
                psnr TEXT,
                progress TEXT,
                orig_size TEXT,
                new_size TEXT,
                size_change TEXT,
                completed_date TEXT,
                orig_size_bytes INTEGER,
                new_size_bytes INTEGER,
                source_frame_count INTEGER,
                source_duration_seconds REAL,
                source_fps REAL,
                output_frame_count INTEGER,
                output_duration_seconds REAL,
                output_fps REAL,
                source_modified_timestamp REAL,
                output_modified_timestamp REAL,
                output_file_size_bytes INTEGER,
                output_encoder_type TEXT,
                source_extra_metadata TEXT,
                output_extra_metadata TEXT,
                denoise_enabled INTEGER DEFAULT 0,
                manual_cq_range TEXT,
                manual_cq_value INTEGER,
                manual_quality_check TEXT,
                hard_rotate_degrees INTEGER DEFAULT 0
            )
            ''')
        except sqlite3.Error as e:
            if LOAD_DEBUG:
                load_debug_log(f"[_ensure_db_tables] Error creating tables: {e}")
            raise
        # Migration: add source_modified_timestamp if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN source_modified_timestamp REAL')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: add denoise_enabled if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN denoise_enabled INTEGER DEFAULT 0')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: add manual_cq_range if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN manual_cq_range TEXT')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: add manual_cq_value if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN manual_cq_value INTEGER')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: add manual_quality_check if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN manual_quality_check TEXT')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: add hard_rotate_degrees if missing (old DBs)
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN hard_rotate_degrees INTEGER DEFAULT 0')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN output_track_editor_cache TEXT')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        # Migration: output raw timing/frame metadata
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN output_frame_count INTEGER')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN output_duration_seconds REAL')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN output_fps REAL')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN source_extra_metadata TEXT')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN output_extra_metadata TEXT')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise

        # Backfill legacy rows for probe-cache fields.
        # This avoids unnecessary ffprobe calls after restart when old rows miss
        # output metadata that can be inferred safely.
        try:
            cursor.execute(
                "UPDATE videos SET output_file_size_bytes = new_size_bytes "
                "WHERE output_file_size_bytes IS NULL AND new_size_bytes IS NOT NULL"
            )
        except sqlite3.Error:
            pass
        try:
            cursor.execute(
                "UPDATE videos SET output_encoder_type = 'svt-av1' "
                "WHERE (output_encoder_type IS NULL OR TRIM(output_encoder_type) = '') "
                "AND status_code = 'completed_svt'"
            )
        except sqlite3.Error:
            pass
        try:
            cursor.execute(
                "UPDATE videos SET output_encoder_type = 'nvenc' "
                "WHERE (output_encoder_type IS NULL OR TRIM(output_encoder_type) = '') "
                "AND status_code = 'completed_nvenc'"
            )
        except sqlite3.Error:
            pass
        
        # Create program_paths table for external tool paths
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS program_paths (
                    program_name TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            try:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rebuild_state (
                        video_path TEXT PRIMARY KEY,
                        item_id TEXT,
                        rebuild_state TEXT,
                        previous_status TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            except sqlite3.Error:
                pass
        except sqlite3.Error:
            pass  # Table might already exist

    def _register_db_thread(self, thread):
        """Track active DB save threads."""
        with self.db_thread_lock:
            self.active_db_threads.append(thread)

    def _unregister_db_thread(self, thread=None):
        """Remove finished DB save thread from tracking."""
        current = thread or threading.current_thread()
        with self.db_thread_lock:
            self.active_db_threads = [t for t in self.active_db_threads if t is not current]

    def _start_db_thread(self, target, name=None, daemon=False):
        """Starts a DB thread that automatically unregisters.
        
        Args:
            target: The function to run in the thread
            name: Optional thread name for debugging
            daemon: If True, thread will not block program exit.
                   Use True for non-critical background saves.
                   Use False for critical saves that must complete.
        """
        def wrapper():
            try:
                target()
            finally:
                self._unregister_db_thread()
        thread = threading.Thread(target=wrapper, name=name, daemon=daemon)
        self._register_db_thread(thread)
        thread.start()
        return thread

    def _wait_for_db_threads(self, timeout=30.0):
        """Waits for all active DB threads to finish."""
        from .core_paths_tools_logging import get_log_writer
        
        def log_debug(msg):
            """Safe logging wrapper that handles None LOG_WRITER."""
            try:
                writer = get_log_writer()
                if writer:
                    writer.write(msg)
                    writer.flush()
            except (AttributeError, OSError, IOError):
                pass  # Silently ignore if log writer is not available
        
        log_debug(f"[DEBUG] _wait_for_db_threads() called with timeout={timeout}\n")
        
        end_time = (time.time() + timeout) if timeout is not None else None
        iteration = 0
        
        while True:
            iteration += 1
            with self.db_thread_lock:
                alive_threads = [t for t in self.active_db_threads if t.is_alive()]
                self.active_db_threads = alive_threads
            
            if not alive_threads:
                log_debug(f"[DEBUG] _wait_for_db_threads() - no more alive threads after {iteration} iterations\n")
                break
            
            log_debug(f"[DEBUG] _wait_for_db_threads() iteration {iteration} - {len(alive_threads)} alive threads\n")
            for i, thread in enumerate(alive_threads):
                log_debug(f"[DEBUG]   Thread {i}: {thread.name} (alive: {thread.is_alive()})\n")
            
            for thread in alive_threads:
                remaining = None
                if end_time is not None:
                    remaining = max(0, end_time - time.time())
                
                log_debug(f"[DEBUG] Joining thread {thread.name} (remaining: {remaining})\n")
                try:
                    thread.join(remaining)
                except (RuntimeError, AssertionError) as e:
                    log_debug(f"[DEBUG] Exception joining thread {thread.name}: {e}\n")
                    # Thread might have already terminated, continue
                    continue
                
                # Check if timeout reached
                if end_time is not None and time.time() >= end_time:
                    log_debug(f"[DEBUG] _wait_for_db_threads() - timeout reached after {iteration} iterations\n")
                    return
        
        log_debug(f"[DEBUG] _wait_for_db_threads() - all threads finished after {iteration} iterations\n")
    
    def _cleanup_database(self):
        """Cleanup database on exit: WAL checkpoint and close all connections.
        
        This ensures that .db-shm and .db-wal files are properly deleted on exit.
        CRITICAL: Uses db_lock to prevent race conditions with running DB operations.
        """
        if not hasattr(self, 'db_path') or not self.db_path:
            return
        
        # Use db_lock to prevent concurrent access during cleanup
        # This prevents race conditions with save_state_to_db running in background
        lock_acquired = False
        conn = None
        try:
            # Try to acquire lock with short timeout (don't block exit indefinitely)
            lock_acquired = self.db_lock.acquire(timeout=2.0)
            if not lock_acquired:
                # Another DB operation may still be finishing.
                # Wait briefly for tracked DB threads, then try once more.
                try:
                    self._wait_for_db_threads(timeout=3.0)
                except Exception:
                    pass
                lock_acquired = self.db_lock.acquire(timeout=3.0)
            if not lock_acquired:
                # Another DB operation is still running - skip cleanup.
                try:
                    from .core_paths_tools_logging import get_log_writer
                    writer = get_log_writer()
                    if writer:
                        writer.write("[_cleanup_database] Warning: Could not acquire db_lock, skipping cleanup\n")
                        writer.flush()
                except Exception:
                    pass
                return
            
            # Open connection for cleanup with short timeout
            conn = sqlite3.connect(str(self.db_path), timeout=3.0)
            cursor = conn.cursor()
            
            # CRITICAL: WAL checkpoint to merge WAL file into main DB
            # TRUNCATE mode ensures WAL file is emptied and can be deleted
            # Use retry logic similar to save_state_to_db
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    cursor.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                    break  # Success
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                        continue
                    raise  # Re-raise if not a lock error or last attempt
            
            # Optional: Switch back to DELETE mode (default) to remove WAL files
            cursor.execute('PRAGMA journal_mode = DELETE')
            
            conn.commit()
            
        except (sqlite3.Error, OSError) as e:
            # Silent error - don't block exit
            try:
                from .core_paths_tools_logging import get_log_writer
                writer = get_log_writer()
                if writer:
                    writer.write(f"[_cleanup_database] Error: {e}\n")
                    writer.flush()
            except Exception:
                pass
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            if lock_acquired:
                try:
                    self.db_lock.release()
                except RuntimeError:
                    pass

    def _build_settings_snapshot(self):
        """Build a thread-safe snapshot of current settings from tkinter variables.

        MUST be called from the GUI thread. The returned dict can be safely
        passed to save_state_to_db(settings_snapshot=...) from background threads.

        Returns:
            dict: Settings key-value pairs ready for DB insertion.
        """
        source_path_str = str(self.source_path) if (hasattr(self, 'source_path') and self.source_path) else None
        dest_path_str = str(self.dest_path) if (hasattr(self, 'dest_path') and self.dest_path) else None
        return {
            'source_path': source_path_str,
            'dest_path': dest_path_str,
            'min_vmaf': float(self.min_vmaf.get()),
            'vmaf_step': float(self.vmaf_step.get()),
            'max_encoded_percent': float(self.max_encoded_percent.get()),
            'max_encoded_mode': str(self.max_encoded_mode.get()),
            'resize_enabled': bool(self.resize_enabled.get()),
            'resize_height': int(self.resize_height.get()),
            'deband_enabled': bool(self.deband_enabled.get()),
            'force_8bit_denoised_master': bool(self.force_8bit_denoised_master.get()),
            'audio_compression_enabled': bool(self.audio_compression_enabled.get()),
            'audio_compression_method': str(self.audio_compression_method.get()),
            'auto_vmaf_psnr': bool(self.auto_vmaf_psnr.get()),
            'svt_preset': int(self.svt_preset.get()),
            'nvenc_worker_count': int(self.nvenc_worker_count.get()),
            'nvenc_enabled': bool(self.nvenc_enabled.get()),
            'svt_worker_count': int(self.svt_worker_count.get()),
            'crf_increment': int(self.crf_increment.get()),
            'max_cq_limit': int(self.max_cq_limit.get()),
            'vdub_validation_disabled': bool(self.vdub_validation_disabled.get()),
            'hybrid_path': str(self.hybrid_path.get()) if hasattr(self, 'hybrid_path') and self.hybrid_path.get() else ''
        }


    def save_state_to_db(self, progress_callback=None, settings_snapshot=None):
        """Save table state to SQLite database.
        
        Args:
            progress_callback: Optional callback for progress messages.
            settings_snapshot: Pre-cached settings dict from GUI thread.
                When provided, tkinter .get() calls are skipped, making this
                method safe to call from background threads.
                When None, reads settings directly from tkinter variables
                (only safe from the GUI thread).
        """
        # Use lock - ensures only one database operation runs at a time
        with self.db_lock:
            conn = None
            try:
                import time as time_module
                start_time = time_module.time()
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Start | db_path={self.db_path}")
                
                if progress_callback:
                    progress_callback("Saving database...")


                # Prevent overwriting valid status with empty one on immediate close
                # If source path is not set and video list is empty, and we are not in a explicit 'clear' operation...
                # Actually, explicit clear sets self.video_items = {} but keeps source path usually
                
                has_source = hasattr(self, 'source_path') and self.source_path
                has_videos = hasattr(self, 'video_items') and self.video_items
                
                # If we have absolutely nothing (fresh start), do NOT overwrite potential existing DB
                # unless the user explicitly cleared it (which we can't easily track here, but usually source remains)
                if not has_source and not has_videos:
                    print(
                        "DEBUG: save_state_to_db SKIPPING due to empty state. "
                        f"has_source={bool(has_source)}, video_count=0"
                    )
                    if LOAD_DEBUG:
                        load_debug_log(f"[save_state_to_db] Skipping save: Empty state (start/close cycle prevention)")
                    return
                
                video_count = len(self.video_items) if has_videos else 0
                print(
                    "DEBUG: save_state_to_db PROCEEDING. "
                    f"has_source={bool(has_source)}, video_count={video_count}"
                )

                # Retry logic for SQLITE_BUSY errors
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                        break  # Successful connection
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_state_to_db] Database locked, retrying {attempt + 1}/{max_retries}...")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[save_state_to_db] Database locked, retrying {attempt + 1}/{max_retries}...\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            # Other error or last attempt - log it
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[ERROR] [save_state_to_db] SQLite connection error: {e}\n")
                                    import traceback
                                    LOG_WRITER.write(traceback.format_exc())
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            raise
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # PRAGMA settings - IMPORTANT: must be set before transaction!
                # Set WAL mode (if not already set)
                try:
                    cursor.execute('PRAGMA journal_mode')
                    current_mode = cursor.fetchone()[0].upper()
                    if current_mode != 'WAL':
                        cursor.execute('PRAGMA journal_mode = WAL')
                except Exception:
                    pass  # If error, continue
                
                # Set synchronous (before transaction!)
                try:
                    cursor.execute('PRAGMA synchronous = NORMAL')
                except Exception:
                    pass  # If error, continue
                
                # Load previous values from database (if any) - for preservation
                cursor.execute(
                    'SELECT video_path, status, status_code, output_modified_timestamp, source_frame_count, '
                    'source_duration_seconds, source_fps, orig_size_bytes, source_modified_timestamp, '
                    'output_file_size_bytes, output_encoder_type, new_size_bytes, output_frame_count, '
                    'output_duration_seconds, output_fps, source_extra_metadata, output_extra_metadata, '
                    'denoise_enabled, manual_cq_range, manual_cq_value, manual_quality_check, hard_rotate_degrees FROM videos'
                )
                existing_data = {}
                for row in cursor.fetchall():
                    existing_data[row[0]] = {
                        'status': row[1],
                        'status_code': row[2],
                        'output_modified_timestamp': row[3],
                        'source_frame_count': row[4],
                        'source_duration_seconds': row[5],
                        'source_fps': row[6],
                        'orig_size_bytes': row[7],
                        'source_modified_timestamp': row[8],
                        'output_file_size_bytes': row[9],
                        'output_encoder_type': row[10],
                        'new_size_bytes': row[11],  # Output file size
                        'output_frame_count': row[12] if len(row) > 12 else None,
                        'output_duration_seconds': row[13] if len(row) > 13 else None,
                        'output_fps': row[14] if len(row) > 14 else None,
                        'source_extra_metadata': self._deserialize_extra_metadata(row[15] if len(row) > 15 else None),
                        'output_extra_metadata': self._deserialize_extra_metadata(row[16] if len(row) > 16 else None),
                        'denoise_enabled': normalize_denoise_level(row[17] if len(row) > 17 else 0),
                        'manual_cq_range': row[18] if len(row) > 18 else None,
                        'manual_cq_value': row[19] if len(row) > 19 else None,
                        'manual_quality_check': row[20] if len(row) > 20 else None,
                        'hard_rotate_degrees': row[21] if len(row) > 21 else 0
                    }

                try:
                    cursor.execute('SELECT video_path, output_track_editor_cache FROM videos')
                    existing_track_editor_cache = {
                        row[0]: row[1]
                        for row in cursor.fetchall()
                        if row and row[0] and row[1]
                    }
                except sqlite3.Error:
                    existing_track_editor_cache = {}
                
                # Save settings
                # IMPORTANT: source_path and dest_path should always be saved if set
                source_path_str = str(self.source_path) if (hasattr(self, 'source_path') and self.source_path) else None
                dest_path_str = str(self.dest_path) if (hasattr(self, 'dest_path') and self.dest_path) else None
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Saving settings: source_path={source_path_str}, dest_path={dest_path_str}")
                
                # THREAD-SAFETY FIX: Use pre-cached settings_snapshot if provided
                # (from background thread callers), otherwise read from tkinter variables
                # directly (only safe from GUI thread).
                if settings_snapshot is not None:
                    settings_data = settings_snapshot
                else:
                    settings_data = {
                        'source_path': source_path_str,
                        'dest_path': dest_path_str,
                        'min_vmaf': float(self.min_vmaf.get()),
                        'vmaf_step': float(self.vmaf_step.get()),
                        'max_encoded_percent': float(self.max_encoded_percent.get()),
                        'max_encoded_mode': str(self.max_encoded_mode.get()),
                        'resize_enabled': bool(self.resize_enabled.get()),
                        'resize_height': int(self.resize_height.get()),
                        'deband_enabled': bool(self.deband_enabled.get()),
                        'force_8bit_denoised_master': bool(self.force_8bit_denoised_master.get()),
                        'audio_compression_enabled': bool(self.audio_compression_enabled.get()),
                        'audio_compression_method': str(self.audio_compression_method.get()),
                        'auto_vmaf_psnr': bool(self.auto_vmaf_psnr.get()),
                        'svt_preset': int(self.svt_preset.get()),
                        'nvenc_worker_count': int(self.nvenc_worker_count.get()),
                        'nvenc_enabled': bool(self.nvenc_enabled.get()),
                        'svt_worker_count': int(self.svt_worker_count.get()),
                        'crf_increment': int(self.crf_increment.get()),
                        'max_cq_limit': int(self.max_cq_limit.get()),
                        'vdub_validation_disabled': bool(self.vdub_validation_disabled.get()),
                        'hybrid_path': str(self.hybrid_path.get()) if hasattr(self, 'hybrid_path') and self.hybrid_path.get() else ''
                    }
                
                # Update settings table (INSERT OR REPLACE) - batch optimization
                settings_values = [(key, str(value) if value is not None else None) for key, value in settings_data.items()]
                cursor.executemany('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', settings_values)
                    
                if LOAD_DEBUG:
                    load_debug_log(f"[save_settings_to_db] Settings saved: {len(settings_values)} settings")

                videos_data = self._build_video_rows_for_state_save(existing_data, existing_track_editor_cache)
                if videos_data:
                    current_paths = {row[0] for row in videos_data}
                    try:
                        cursor.execute('SELECT video_path FROM videos')
                        stale_paths = [row[0] for row in cursor.fetchall() if row and row[0] not in current_paths]
                        if stale_paths:
                            cursor.executemany('DELETE FROM videos WHERE video_path = ?', [(path,) for path in stale_paths])
                    except sqlite3.Error:
                        pass

                    cursor.executemany('''
                        INSERT OR REPLACE INTO videos (
                            video_path, output_path, order_number, video_name, status, status_code,
                            cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                            orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                            output_frame_count, output_duration_seconds, output_fps,
                            source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type,
                            source_extra_metadata, output_extra_metadata,
                            denoise_enabled, manual_cq_range, manual_cq_value, manual_quality_check, hard_rotate_degrees,
                            output_track_editor_cache
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', videos_data)

                    if LOAD_DEBUG:
                        load_debug_log(f"[save_state_to_db] Video rows saved: {len(videos_data)}")
                    if progress_callback:
                        try:
                            progress_callback(f"Writing database... ({len(videos_data)} videos)")
                        except Exception:
                            pass
                elif has_videos and LOAD_DEBUG:
                    load_debug_log("[save_state_to_db] WARNING: video_items exists but no DB video rows were built")
                
                conn.commit()
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if LOAD_DEBUG:
                    load_debug_log(f"[save_settings_to_db] Error: {e}")
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _build_video_rows_for_state_save(self, existing_data, existing_track_editor_cache=None):
        """Build videos table rows without doing slow probes or unsafe Tk access."""
        if not hasattr(self, 'video_items') or not self.video_items:
            return []

        existing_data = existing_data or {}
        existing_track_editor_cache = existing_track_editor_cache or {}
        on_main_thread = threading.current_thread() is threading.main_thread()
        app_closing = is_app_closing()
        rows = []

        def value_at(values, column_name, default="-"):
            idx = self.COLUMN_INDEX.get(column_name) if hasattr(self, 'COLUMN_INDEX') else None
            if idx is None or idx >= len(values):
                return default
            value = values[idx]
            return default if value in (None, "") else value

        with self.video_items_lock:
            video_item_pairs = list(self.video_items.items())

        for video_path, item_id in video_item_pairs:
            try:
                video_path = Path(video_path)
                meta = self.get_tree_item_meta(item_id) if item_id is not None else {}
                if app_closing or not on_main_thread:
                    values = self._reconstruct_tree_values_from_cache(meta, video_path)
                else:
                    try:
                        values = list(self.tree.item(item_id, 'values'))
                    except (tk.TclError, KeyError, AttributeError):
                        values = self._reconstruct_tree_values_from_cache(meta, video_path)

                existing = existing_data.get(str(video_path), {})
                output_file = self.video_to_output.get(video_path)
                if not output_file:
                    try:
                        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                    except Exception:
                        output_file = None

                status_text = str(value_at(values, 'status', meta.get('status_display', existing.get('status', ""))))
                if meta.get('metadata_refresh_in_progress'):
                    status_text = meta.get('metadata_refresh_original_status') or meta.get('status_display') or existing.get('status') or status_text
                status_code = normalize_status_to_code(status_text) or existing.get('status_code') or meta.get('status_code')

                cq_val = str(value_at(values, 'cq', meta.get('cq', existing.get('cq', "-"))))
                vmaf_val = str(value_at(values, 'vmaf', meta.get('vmaf', existing.get('vmaf', "-"))))
                psnr_val = str(value_at(values, 'psnr', meta.get('psnr', existing.get('psnr', "-"))))
                progress_val = str(value_at(values, 'progress', meta.get('progress_display', existing.get('progress', "-"))))
                orig_size_str = str(value_at(values, 'orig_size', meta.get('orig_size_display', existing.get('orig_size', "-"))))
                new_size_str = str(value_at(values, 'new_size', meta.get('new_size_display', existing.get('new_size', "-"))))
                size_change_str = str(value_at(values, 'size_change', meta.get('size_change_display', existing.get('size_change', "-"))))
                completed_date = str(value_at(values, 'completed_date', meta.get('completed_date', existing.get('completed_date', ""))))
                if completed_date == "-":
                    completed_date = ""

                orig_size_bytes = meta.get('source_size_bytes')
                if orig_size_bytes is None:
                    orig_size_bytes = existing.get('orig_size_bytes')
                if orig_size_bytes is None:
                    cached_stat = self.video_stat_cache.get(video_path, {})
                    orig_size_bytes = cached_stat.get('source_size_bytes')
                if orig_size_bytes is None:
                    orig_size_bytes = parse_size_to_bytes(orig_size_str) if orig_size_str != "-" else None

                new_size_bytes = meta.get('output_size_bytes')
                if new_size_bytes is None:
                    new_size_bytes = existing.get('new_size_bytes') or existing.get('output_file_size_bytes')
                if new_size_bytes is None:
                    new_size_bytes = parse_size_to_bytes(new_size_str) if new_size_str != "-" else None

                source_modified_timestamp = meta.get('source_modified_timestamp')
                if source_modified_timestamp is None:
                    source_modified_timestamp = existing.get('source_modified_timestamp')
                if source_modified_timestamp is None:
                    cached_stat = self.video_stat_cache.get(video_path, {})
                    source_modified_timestamp = cached_stat.get('source_modified_timestamp')

                output_modified_timestamp = meta.get('output_modified_timestamp')
                if output_modified_timestamp is None:
                    output_modified_timestamp = existing.get('output_modified_timestamp')

                output_file_size_bytes = meta.get('output_size_bytes')
                if output_file_size_bytes is None:
                    output_file_size_bytes = existing.get('output_file_size_bytes') or new_size_bytes

                output_encoder_type = meta.get('encoder_type') or meta.get('output_encoder_type') or existing.get('output_encoder_type')
                if status_code == 'completed_copy':
                    output_encoder_type = None
                elif not output_encoder_type:
                    output_encoder_type = _infer_output_encoder_type(status_code, status_text)

                denoise_enabled = normalize_denoise_level(meta.get('denoise_enabled', existing.get('denoise_enabled', 0)))
                hard_rotate_degrees = normalize_hard_rotate_degrees(meta.get('hard_rotate_degrees', existing.get('hard_rotate_degrees', 0)))
                manual_cq_range = meta.get('manual_cq_range', existing.get('manual_cq_range'))
                manual_cq_value = meta.get('manual_cq_value', existing.get('manual_cq_value'))
                manual_quality_check = meta.get('manual_quality_check', existing.get('manual_quality_check'))

                if manual_cq_value is None and " (M " in status_text:
                    try:
                        manual_part = status_text.split(" (M ", 1)[1].rstrip(")")
                        if "CQ:" in manual_part:
                            manual_cq_value = int(manual_part.split("CQ:", 1)[1].split()[0])
                            manual_cq_range = manual_cq_range or "single"
                        quality = manual_part.split()[-1].lower()
                        if quality in ('vmaf', 'psnr', 'both'):
                            manual_quality_check = quality
                    except (IndexError, ValueError, AttributeError):
                        pass

                source_extra_metadata = self._normalize_extra_metadata_dict(meta.get('source_extra_metadata'))
                if source_extra_metadata is None:
                    source_extra_metadata = self._normalize_extra_metadata_dict(existing.get('source_extra_metadata'))
                output_extra_metadata = self._normalize_extra_metadata_dict(meta.get('output_extra_metadata'))
                if output_extra_metadata is None:
                    output_extra_metadata = self._normalize_extra_metadata_dict(existing.get('output_extra_metadata'))

                video_name = str(value_at(values, 'video_name', ""))
                if not video_name:
                    try:
                        video_name = self.format_relative_name(video_path)
                    except Exception:
                        video_name = video_path.name

                rows.append((
                    str(video_path),
                    str(output_file) if output_file else None,
                    int(self.video_order.get(video_path, 0)),
                    video_name,
                    status_text,
                    status_code,
                    cq_val,
                    vmaf_val,
                    psnr_val,
                    progress_val,
                    orig_size_str,
                    new_size_str,
                    size_change_str,
                    completed_date,
                    orig_size_bytes,
                    new_size_bytes,
                    meta.get('source_frame_count', existing.get('source_frame_count')),
                    meta.get('source_duration_seconds', existing.get('source_duration_seconds')),
                    meta.get('source_fps', existing.get('source_fps')),
                    meta.get('output_frame_count', existing.get('output_frame_count')),
                    meta.get('output_duration_seconds', existing.get('output_duration_seconds')),
                    meta.get('output_fps', existing.get('output_fps')),
                    source_modified_timestamp,
                    output_modified_timestamp,
                    output_file_size_bytes,
                    output_encoder_type,
                    self._serialize_extra_metadata(source_extra_metadata),
                    self._serialize_extra_metadata(output_extra_metadata),
                    denoise_enabled,
                    manual_cq_range,
                    manual_cq_value,
                    manual_quality_check,
                    hard_rotate_degrees,
                    existing_track_editor_cache.get(str(video_path))
                ))
            except Exception as e:
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] [save_state_to_db] Video row build skipped: {video_path} -> {e}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                continue

        return rows

    def update_single_video_extra_metadata_in_db(self, video_path, source_extra_metadata=None, output_extra_metadata=None):
        """Persist cached extra tooltip metadata without touching the rest of the row."""
        if not hasattr(self, 'db_path') or not self.db_path:
            return

        source_json = self._serialize_extra_metadata(source_extra_metadata)
        output_json = self._serialize_extra_metadata(output_extra_metadata)

        if source_json is None and output_json is None:
            if LOG_WRITER and not LOG_WRITER.closed:
                try:
                    LOG_WRITER.write(f"  [EXTRA-META-DB] {Path(video_path).name}: SKIP WRITE (both serializations returned None)\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, ValueError):
                    pass
            return

        with self.db_lock:
            conn = None
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)

                updates = []
                params = []
                if source_json is not None:
                    updates.append('source_extra_metadata = ?')
                    params.append(source_json)
                if output_json is not None:
                    updates.append('output_extra_metadata = ?')
                    params.append(output_json)

                if not updates:
                    return

                params.append(str(video_path))
                cursor.execute(
                    f"UPDATE videos SET {', '.join(updates)} WHERE video_path = ?",
                    tuple(params)
                )
                rows_affected = cursor.rowcount
                conn.commit()

                if LOG_WRITER and not LOG_WRITER.closed:
                    try:
                        _src_size = source_extra_metadata.get('file_size') if isinstance(source_extra_metadata, dict) else None
                        _src_br = source_extra_metadata.get('bit_rate') if isinstance(source_extra_metadata, dict) else None
                        _src_streams = len(source_extra_metadata.get('streams', {}).get('video', [])) if isinstance(source_extra_metadata, dict) else 0
                        _src_audio = len(source_extra_metadata.get('streams', {}).get('audio', [])) if isinstance(source_extra_metadata, dict) else 0
                        _src_sub = len(source_extra_metadata.get('streams', {}).get('subtitle', [])) if isinstance(source_extra_metadata, dict) else 0
                        LOG_WRITER.write(
                            f"  [EXTRA-META-DB] {Path(video_path).name}: WRITTEN "
                            f"(rows={rows_affected}, src_json={len(source_json) if source_json else 0}b, "
                            f"out_json={len(output_json) if output_json else 0}b, "
                            f"src: file_size={_src_size} bitrate={_src_br} "
                            f"streams={_src_streams}v/{_src_audio}a/{_src_sub}s)\n"
                        )
                        LOG_WRITER.flush()
                    except (OSError, IOError, ValueError):
                        pass
            except (sqlite3.Error, OSError, PermissionError, ValueError, TypeError) as e:
                if LOG_WRITER and not LOG_WRITER.closed:
                    try:
                        LOG_WRITER.write(f"  [EXTRA-META-DB] {Path(video_path).name}: ERROR writing to DB: {e}\n")
                        LOG_WRITER.flush()
                    except (OSError, IOError, ValueError):
                        pass
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def update_single_video_in_db(self, video_path, item_id, status_text, cq_str, vmaf_str, psnr_str, orig_size_str, new_size_mb, change_percent, completed_date, manual_cq_range=None, manual_cq_value=None, manual_quality_check=None, denoise_enabled=None, hard_rotate_degrees=None, source_extra_metadata=None, output_extra_metadata=None, clear_output_state=False):
        """Update database entry for a single video (after encoding finished).
        
        Args:
            manual_cq_range: Manual CQ range (e.g., "10-15")
            manual_cq_value: Manual CQ value used (e.g., 11)
            manual_quality_check: Quality check type ('vmaf', 'psnr', 'both', or None)
            denoise_enabled: Denoise level (0=disabled, 1=strong, 2=light, 3=very-strong, 4=ultra-strong)
            hard_rotate_degrees: Hard rotate value (0/90/180/270)
        """
        if not hasattr(self, 'db_path') or not self.db_path:
            return  # No database
        
        # Use lock - ensures only one database operation runs at a time
        with self.db_lock:
            conn = None
            try:
                # Retry logic for SQLITE_BUSY errors
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                        break  # Successful connection
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            raise  # Other error or last attempt
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # PRAGMA settings
                try:
                    cursor.execute('PRAGMA journal_mode = WAL')
                    cursor.execute('PRAGMA synchronous = NORMAL')
                except Exception:
                    pass
                
                # Query existing data (if any)
                video_path_str = str(video_path)
                cursor.execute(
                    'SELECT output_path, order_number, video_name, orig_size_bytes, new_size_bytes, '
                    'source_frame_count, source_duration_seconds, source_fps, '
                    'output_frame_count, output_duration_seconds, output_fps, '
                    'source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, '
                    'output_encoder_type, source_extra_metadata, output_extra_metadata, '
                    'denoise_enabled, manual_cq_range, manual_cq_value, '
                    'manual_quality_check, hard_rotate_degrees FROM videos WHERE video_path = ?',
                    (video_path_str,)
                )
                existing_row = cursor.fetchone()
                
                # Query tree data
                # Tkinter widget access is only safe from the main thread.
                # From worker threads (or during shutdown), use cached row metadata.
                on_main_thread = threading.current_thread() is threading.main_thread()
                if is_app_closing() or not on_main_thread:
                    cached_data = self.get_tree_item_meta(item_id)
                    values = self._reconstruct_tree_values_from_cache(cached_data, video_path)
                else:
                    try:
                        values = self.tree.item(item_id, 'values')
                    except (tk.TclError, KeyError, AttributeError):
                        values = []

                if clear_output_state:
                    completed_date = ""

                # Determine output file
                output_file = self.video_to_output.get(video_path)
                if not output_file:
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                
                output_path_str = str(output_file) if output_file else None
                order_num = self.video_order.get(video_path, 0)
                video_name = video_path.name
                
                # Status code
                status_code = normalize_status_to_code(status_text)
                completed_codes = ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists')
                
                # Sizes
                if new_size_mb is not None:
                    new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                else:
                    new_size_str = "-"
                if change_percent is not None:
                    change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                else:
                    change_percent_str = "-"
                
                # Calculate byte values
                orig_size_bytes = parse_size_to_bytes(orig_size_str) if orig_size_str and orig_size_str != "-" else None
                new_size_bytes = round(new_size_mb * 1024 * 1024) if new_size_mb is not None else None
                
                # Runtime cache behind tree row (raw numeric fields, not localized UI strings)
                original_data = self.get_tree_item_meta(item_id) if item_id is not None else {}
                cached_stat = self.video_stat_cache.get(video_path, {})

                # Keep existing data (if any)
                if existing_row:
                    # Use existing values if no new value
                    if not output_path_str:
                        output_path_str = existing_row[0]
                    if not order_num:
                        order_num = existing_row[1] or 0
                    if not video_name:
                        video_name = existing_row[2] or video_path.name
                    if orig_size_bytes is None:
                        orig_size_bytes = existing_row[3]
                    if new_size_bytes is None:
                        new_size_bytes = existing_row[4]
                    
                    source_frame_count = existing_row[5]
                    source_duration_seconds = existing_row[6]
                    source_fps = existing_row[7]
                    output_frame_count = existing_row[8]
                    output_duration_seconds = existing_row[9]
                    output_fps = existing_row[10]
                    source_modified_timestamp = existing_row[11]
                    output_modified_timestamp = existing_row[12]
                    output_file_size_bytes = existing_row[13]
                    output_encoder_type = existing_row[14]

                    # Fill missing raw metadata from in-memory caches (fast, no probing)
                    if source_frame_count is None:
                        source_frame_count = original_data.get('source_frame_count')
                    if source_duration_seconds is None:
                        source_duration_seconds = original_data.get('source_duration_seconds')
                    if source_fps is None:
                        source_fps = original_data.get('source_fps')
                    if orig_size_bytes is None:
                        orig_size_bytes = original_data.get('source_size_bytes')
                    if source_modified_timestamp is None:
                        source_modified_timestamp = original_data.get('source_modified_timestamp')
                    if orig_size_bytes is None:
                        orig_size_bytes = cached_stat.get('source_size_bytes')
                    if source_modified_timestamp is None:
                        source_modified_timestamp = cached_stat.get('source_modified_timestamp')
                    if output_modified_timestamp is None:
                        output_modified_timestamp = original_data.get('output_modified_timestamp')
                    if output_file_size_bytes is None:
                        output_file_size_bytes = original_data.get('output_size_bytes')
                    if output_encoder_type is None:
                        output_encoder_type = original_data.get('encoder_type')
                    if output_frame_count is None:
                        output_frame_count = original_data.get('output_frame_count')
                    if output_duration_seconds is None:
                        output_duration_seconds = original_data.get('output_duration_seconds')
                    if output_fps is None:
                        output_fps = original_data.get('output_fps')

                    source_extra_metadata_db = self._deserialize_extra_metadata(existing_row[15] if len(existing_row) > 15 else None)
                    output_extra_metadata_db = self._deserialize_extra_metadata(existing_row[16] if len(existing_row) > 16 else None)
                    
                    # Manual encoding parameters - NEW
                    denoise_enabled_db = existing_row[17] if len(existing_row) > 17 else None
                    manual_cq_range_db = existing_row[18] if len(existing_row) > 18 else None
                    manual_cq_value_db = existing_row[19] if len(existing_row) > 19 else None
                    manual_quality_check_db = existing_row[20] if len(existing_row) > 20 else None
                    hard_rotate_degrees_db = existing_row[21] if len(existing_row) > 21 else None
                else:
                    # No existing entry
                    source_extra_metadata_db = None
                    output_extra_metadata_db = None
                    denoise_enabled_db = None
                    manual_cq_range_db = None
                    manual_cq_value_db = None
                    manual_quality_check_db = None
                    hard_rotate_degrees_db = None
                    
                    source_frame_count = original_data.get('source_frame_count')
                    source_duration_seconds = original_data.get('source_duration_seconds')
                    source_fps = original_data.get('source_fps')
                    output_frame_count = original_data.get('output_frame_count')
                    output_duration_seconds = original_data.get('output_duration_seconds')
                    output_fps = original_data.get('output_fps')
                    
                    # Source stat() - from cache or fresh
                    if cached_stat:
                        if orig_size_bytes is None:
                            orig_size_bytes = cached_stat.get('source_size_bytes')
                        source_modified_timestamp = cached_stat.get('source_modified_timestamp')
                    else:
                        if video_path.exists():
                            try:
                                stat_info = video_path.stat()
                                if orig_size_bytes is None:
                                    orig_size_bytes = stat_info.st_size
                                source_modified_timestamp = stat_info.st_mtime
                            except (OSError, PermissionError):
                                source_modified_timestamp = None
                        else:
                            source_modified_timestamp = None
                    
                    # Output file data (if completed status)
                    if status_code in completed_codes:
                        if output_file and output_file.exists():
                            try:
                                output_stat_info = output_file.stat()
                                output_file_size_bytes = output_stat_info.st_size
                                output_modified_timestamp = output_stat_info.st_mtime
                            except (OSError, PermissionError):
                                output_file_size_bytes = None
                                output_modified_timestamp = None
                            
                            # Output encoder_type from cache; operation-end block performs/refreshes probe.
                            output_encoder_type = original_data.get('encoder_type')
                        else:
                            output_file_size_bytes = None
                            output_modified_timestamp = None
                            output_encoder_type = None
                    else:
                        output_file_size_bytes = None
                        output_modified_timestamp = None
                        output_encoder_type = None

                # Ensure output file size bytes follows the latest completed operation data.
                if status_code in completed_codes and new_size_bytes is not None and output_file_size_bytes is None:
                    output_file_size_bytes = new_size_bytes

                if status_code == 'completed_copy':
                    output_encoder_type = None  # Copied file has no encoder type - clear stale DB value
                elif not output_encoder_type:
                    output_encoder_type = _infer_output_encoder_type(status_code, status_text)

                resolved_source_extra_metadata = self._normalize_extra_metadata_dict(source_extra_metadata)
                if resolved_source_extra_metadata is None:
                    resolved_source_extra_metadata = self._normalize_extra_metadata_dict(original_data.get('source_extra_metadata'))
                if resolved_source_extra_metadata is None:
                    resolved_source_extra_metadata = source_extra_metadata_db

                resolved_output_extra_metadata = self._normalize_extra_metadata_dict(output_extra_metadata)
                if resolved_output_extra_metadata is None:
                    resolved_output_extra_metadata = self._normalize_extra_metadata_dict(original_data.get('output_extra_metadata'))
                if resolved_output_extra_metadata is None:
                    resolved_output_extra_metadata = output_extra_metadata_db

                if clear_output_state:
                    new_size_str = "-"
                    change_percent_str = "-"
                    new_size_bytes = None
                    output_frame_count = None
                    output_duration_seconds = None
                    output_fps = None
                    output_modified_timestamp = None
                    output_file_size_bytes = None
                    output_encoder_type = None
                    resolved_output_extra_metadata = None

                # Operation-end probe: spread ffprobe cost during runtime (encoding/VMAF finish),
                # so exit-time save does not need to probe hundreds of files.
                if status_code in completed_codes and (not is_app_closing()):
                    # Source side: probe only when core metadata is missing.
                    source_meta_missing = (
                        source_frame_count is None or
                        source_duration_seconds is None or
                        source_fps is None
                    )
                    if source_meta_missing and video_path and video_path.exists():
                        try:
                            from .core_audio_video_ops import get_video_info
                            probed_duration, probed_fps = get_video_info(video_path)
                            if source_duration_seconds is None:
                                source_duration_seconds = probed_duration
                            if source_fps is None:
                                source_fps = probed_fps
                            if source_frame_count is None and probed_duration and probed_fps:
                                try:
                                    source_frame_count = int(probed_duration * probed_fps)
                                except (ValueError, TypeError):
                                    pass
                        except Exception:
                            pass

                    # Always refresh source stat cache on completion if file exists.
                    if video_path and video_path.exists():
                        try:
                            src_stat = video_path.stat()
                            if orig_size_bytes is None:
                                orig_size_bytes = src_stat.st_size
                            if source_modified_timestamp is None:
                                source_modified_timestamp = src_stat.st_mtime
                        except (OSError, PermissionError):
                            pass

                    # Output side: refresh stat and probe encoder type only if missing.
                    if output_file and output_file.exists():
                        try:
                            out_stat = output_file.stat()
                            output_file_size_bytes = out_stat.st_size
                            if new_size_bytes is None:
                                new_size_bytes = out_stat.st_size
                            output_modified_timestamp = out_stat.st_mtime
                        except (OSError, PermissionError):
                            pass

                        # Probe encoder tag only when still unknown after status-based inference.
                        # Skip completed_copy: output IS the source file, probe would detect source codec
                        needs_encoder_probe = (
                            not output_encoder_type and
                            status_code in ('completed', 'completed_exists')
                        )
                        if needs_encoder_probe:
                            try:
                                probe_cmd = [
                                    FFPROBE_PATH, '-v', 'error',
                                    '-show_entries', 'format_tags=Settings',
                                    '-of', 'default=noprint_wrappers=1:nokey=1',
                                    str(output_file)
                                ]
                                result = subprocess.run(
                                    probe_cmd,
                                    capture_output=True,
                                    text=True,
                                    encoding='utf-8',
                                    errors='replace',
                                    timeout=600,
                                    startupinfo=get_startup_info()
                                )
                                if result.returncode == 0 and result.stdout:
                                    settings_tag = result.stdout.strip().upper()
                                    if 'NVENC' in settings_tag or 'CQ:' in settings_tag:
                                        output_encoder_type = 'nvenc'
                                    elif 'SVT-AV1' in settings_tag or 'SVT' in settings_tag or 'CRF:' in settings_tag:
                                        output_encoder_type = 'svt-av1'
                            except Exception:
                                pass

                        # Fill output timing/frame metadata if still missing.
                        if output_duration_seconds is None or output_fps is None:
                            try:
                                from .core_audio_video_ops import get_video_info
                                probed_out_duration, probed_out_fps = get_video_info(output_file)
                                if output_duration_seconds is None:
                                    output_duration_seconds = probed_out_duration
                                if output_fps is None:
                                    output_fps = probed_out_fps
                            except Exception:
                                pass
                        if output_frame_count is None and output_duration_seconds and output_fps:
                            try:
                                output_frame_count = int(output_duration_seconds * output_fps)
                            except (ValueError, TypeError):
                                pass

                    probe_registration = None
                    try:
                        from .core_audio_video_ops import get_video_extra_metadata

                        probe_targets = []
                        source_extra_metadata_needs_probe = (
                            resolved_source_extra_metadata is None or
                            not self._extra_metadata_has_any_stream_rows(resolved_source_extra_metadata) or
                            not self._extra_metadata_has_size_and_bitrate(resolved_source_extra_metadata)
                        )
                        output_extra_metadata_needs_probe = (
                            resolved_output_extra_metadata is None or
                            not self._extra_metadata_has_any_stream_rows(resolved_output_extra_metadata) or
                            not self._extra_metadata_has_size_and_bitrate(resolved_output_extra_metadata)
                        )

                        _db_probe_reasons = []
                        if source_extra_metadata_needs_probe and video_path and video_path.exists():
                            probe_targets.append(video_path)
                            if resolved_source_extra_metadata is None:
                                _db_probe_reasons.append("source: no cached data")
                            elif not self._extra_metadata_has_any_stream_rows(resolved_source_extra_metadata):
                                _db_probe_reasons.append("source: missing stream rows")
                            else:
                                _db_probe_reasons.append("source: missing file_size/bit_rate")
                        if output_extra_metadata_needs_probe and output_file and output_file.exists():
                            probe_targets.append(output_file)
                            if resolved_output_extra_metadata is None:
                                _db_probe_reasons.append("output: no cached data")
                            elif not self._extra_metadata_has_any_stream_rows(resolved_output_extra_metadata):
                                _db_probe_reasons.append("output: missing stream rows")
                            else:
                                _db_probe_reasons.append("output: missing file_size/bit_rate")

                        if _db_probe_reasons:
                            if LOG_WRITER and not LOG_WRITER.closed:
                                try:
                                    LOG_WRITER.write(f"  [DB-PROBE] {Path(video_path).name}: RE-PROBING ({'; '.join(_db_probe_reasons)})\n")
                                    LOG_WRITER.flush()
                                except (OSError, IOError, ValueError):
                                    pass

                        probe_allowed = True
                        if probe_targets and hasattr(self, '_try_begin_metadata_probe_activity'):
                            probe_allowed, probe_registration = self._try_begin_metadata_probe_activity(*probe_targets)

                        if probe_allowed:
                            if source_extra_metadata_needs_probe and video_path and video_path.exists():
                                resolved_source_extra_metadata = get_video_extra_metadata(video_path)
                            if output_extra_metadata_needs_probe and output_file and output_file.exists():
                                resolved_output_extra_metadata = get_video_extra_metadata(output_file)
                    except Exception:
                        pass
                    finally:
                        if probe_registration and hasattr(self, '_finish_metadata_probe_activity'):
                            self._finish_metadata_probe_activity(*probe_registration)

                # Resolve denoise/hard-rotate before cache update so None never leaks into metadata.
                resolved_denoise = denoise_enabled
                if resolved_denoise is None:
                    resolved_denoise = original_data.get('denoise_enabled')
                if resolved_denoise is None:
                    resolved_denoise = denoise_enabled_db
                resolved_denoise = normalize_denoise_level(resolved_denoise)

                resolved_hard_rotate = hard_rotate_degrees
                if resolved_hard_rotate is None:
                    resolved_hard_rotate = original_data.get('hard_rotate_degrees')
                if resolved_hard_rotate is None:
                    resolved_hard_rotate = hard_rotate_degrees_db
                resolved_hard_rotate = normalize_hard_rotate_degrees(resolved_hard_rotate)

                # Persist fresh raw metadata to in-memory caches for fast future saves.
                if item_id is not None and clear_output_state:
                    self.remove_tree_item_meta_keys(
                        item_id,
                        'output_size_bytes',
                        'output_modified_timestamp',
                        'output_extra_metadata',
                        'output_frame_count',
                        'output_duration_seconds',
                        'output_fps',
                        'encoder_type'
                    )

                if item_id is not None:
                    self.set_tree_item_meta(
                        item_id,
                        source_frame_count=source_frame_count,
                        source_duration_seconds=source_duration_seconds,
                        source_fps=source_fps,
                        output_frame_count=output_frame_count,
                        output_duration_seconds=output_duration_seconds,
                        output_fps=output_fps,
                        source_size_bytes=orig_size_bytes,
                        source_modified_timestamp=source_modified_timestamp,
                        output_size_bytes=new_size_bytes,
                        output_modified_timestamp=output_modified_timestamp,
                        encoder_type=output_encoder_type,
                        source_extra_metadata=resolved_source_extra_metadata,
                        output_extra_metadata=resolved_output_extra_metadata,
                        denoise_enabled=resolved_denoise,
                        denoise_display=denoise_level_to_display(resolved_denoise),
                        hard_rotate_degrees=resolved_hard_rotate,
                        hard_rotate_display=hard_rotate_to_display(resolved_hard_rotate),
                        status_display=status_text,
                        progress_display="-" if clear_output_state else None,
                        new_size_display="-" if clear_output_state else None,
                        size_change_display="-" if clear_output_state else None,
                        completed_date="" if clear_output_state else None
                    )

                if video_path:
                    if video_path not in self.video_stat_cache:
                        self.video_stat_cache[video_path] = {}
                    if orig_size_bytes is not None:
                        self.video_stat_cache[video_path]['source_size_bytes'] = orig_size_bytes
                    if source_modified_timestamp is not None:
                        self.video_stat_cache[video_path]['source_modified_timestamp'] = source_modified_timestamp
                
                # Fallback logic for manual parameters (use DB value if parameter is None)
                if manual_cq_range is None:
                    manual_cq_range = manual_cq_range_db
                if manual_cq_value is None:
                    manual_cq_value = manual_cq_value_db
                if manual_quality_check is None:
                    manual_quality_check = manual_quality_check_db
                denoise_enabled = resolved_denoise
                if hard_rotate_degrees is None:
                    hard_rotate_degrees = resolved_hard_rotate
                else:
                    hard_rotate_degrees = normalize_hard_rotate_degrees(hard_rotate_degrees)
                
                cursor.execute(
                    'SELECT output_track_editor_cache FROM videos WHERE video_path = ?',
                    (video_path_str,)
                )
                existing_cache_row = cursor.fetchone()
                preserved_cache = existing_cache_row[0] if existing_cache_row and existing_cache_row[0] else None

                if clear_output_state:
                    preserved_cache = None

                # INSERT OR REPLACE
                progress_db_value = "-" if clear_output_state else "100%"

                cursor.execute('''
                    INSERT OR REPLACE INTO videos (
                        video_path, output_path, order_number, video_name, status, status_code,
                        cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                        orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                        output_frame_count, output_duration_seconds, output_fps,
                        source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type,
                        source_extra_metadata, output_extra_metadata,
                        denoise_enabled, manual_cq_range, manual_cq_value, manual_quality_check, hard_rotate_degrees,
                        output_track_editor_cache
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video_path_str, output_path_str, order_num, video_name, status_text, status_code,
                    cq_str, vmaf_str, psnr_str, progress_db_value, orig_size_str, new_size_str, change_percent_str, completed_date,
                    orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                    output_frame_count, output_duration_seconds, output_fps,
                    source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type,
                    self._serialize_extra_metadata(resolved_source_extra_metadata),
                    self._serialize_extra_metadata(resolved_output_extra_metadata),
                    denoise_enabled, manual_cq_range, manual_cq_value, manual_quality_check, hard_rotate_degrees,
                    preserved_cache
                ))
                
                conn.commit()

                # Schedule DB update notification from the main thread only.
                self._db_update_notification_pending = True
                try:
                    if hasattr(self, 'encoding_queue') and self.encoding_queue:
                        self.encoding_queue.put_nowait(("db_update_saved",))
                except Exception:
                    pass
                
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                # Silent error - do not disturb encoding process
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] [update_single_video_in_db] Error: {e} | video: {video_path}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _save_settings_debounced(self):
        """Debounced beállítások mentése (2 másodperc késleltetéssel)"""
        # Töröljük az előző timert, ha van
        if self.settings_save_timer:
            self.root.after_cancel(self.settings_save_timer)
        
        # Új timer beállítása 2 másodpercre
        self.settings_save_timer = self.root.after(2000, self._do_save_settings)

    def _do_save_settings(self):
        """Tényleges beállítások mentése háttérszálban"""
        def save_in_thread():
            try:
                self.save_settings_to_db()
            except Exception as e:
                if LOAD_DEBUG:
                    load_debug_log(f"[_do_save_settings] Hiba: {e}")
        
        self._start_db_thread(save_in_thread, name="SaveSettings", daemon=True)
        self.settings_save_timer = None

    def load_state_from_db(self):
        """Táblázat állapot betöltése SQLite adatbázisból"""
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                if not self.db_path.exists():
                    print(f"DEBUG: load_state_from_db failed - file does not exist: {self.db_path}")
                    return None
                
                # Retry logika SQLITE_BUSY hibákra
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[load_state_from_db] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...")
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            print(f"DEBUG: load_state_from_db connection failed: {e}")
                            raise  # Egyéb hiba vagy utolsó próbálkozás
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # Settings betöltése
                cursor.execute('SELECT key, value FROM settings')
                settings_rows = cursor.fetchall()
                settings_dict = {row[0]: row[1] for row in settings_rows}
                
                # Videos betöltése
                cursor.execute('''
                    SELECT video_path, output_path, order_number, video_name, status, status_code,
                           cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                           orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                           output_frame_count, output_duration_seconds, output_fps,
                           source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type,
                           source_extra_metadata, output_extra_metadata,
                           denoise_enabled, manual_cq_range, manual_cq_value, manual_quality_check, hard_rotate_degrees
                    FROM videos
                ''')
                videos_rows = cursor.fetchall()
                
                # Videos lista létrehozása
                videos_list = []
                for row in videos_rows:
                    video_dict = {
                        'video_path': row[0],
                        'output_path': row[1],
                        'order_number': row[2],
                        'video_name': row[3],
                        'status': row[4],
                        'status_code': row[5],
                        'cq': row[6],
                        'vmaf': row[7],
                        'psnr': row[8],
                        'progress': row[9],
                        'orig_size': row[10],
                        'new_size': row[11],
                        'size_change': row[12],
                        'completed_date': row[13],
                        'orig_size_bytes': row[14],
                        'new_size_bytes': row[15],
                        'source_frame_count': row[16],
                        'source_duration_seconds': row[17],
                        'source_fps': row[18],
                        'output_frame_count': row[19] if len(row) > 19 else None,
                        'output_duration_seconds': row[20] if len(row) > 20 else None,
                        'output_fps': row[21] if len(row) > 21 else None,
                        'source_modified_timestamp': row[22] if len(row) > 22 else None,
                        'output_modified_timestamp': row[23] if len(row) > 23 else None,
                        'output_file_size_bytes': row[24] if len(row) > 24 else None,
                        'output_encoder_type': row[25] if len(row) > 25 else None,
                        'source_extra_metadata': self._deserialize_extra_metadata(row[26] if len(row) > 26 else None),
                        'output_extra_metadata': self._deserialize_extra_metadata(row[27] if len(row) > 27 else None),
                        'denoise_enabled': normalize_denoise_level(row[28] if len(row) > 28 else 0),
                        'manual_cq_range': row[29] if len(row) > 29 else None,
                        'manual_cq_value': row[30] if len(row) > 30 else None,
                        'manual_quality_check': row[31] if len(row) > 31 else None,
                        'hard_rotate_degrees': row[32] if len(row) > 32 else 0
                    }
                    videos_list.append(video_dict)
                
                # Helper to safe parse int from potential float string
                def safe_int(val, default=0):
                    if not val: return default
                    try:
                        return int(float(val))
                    except (ValueError, TypeError):
                        return default

                # State data összeállítása (kompatibilitás a régi kóddal)
                state_data = {
                    'source_path': settings_dict.get('source_path'),
                    'dest_path': settings_dict.get('dest_path'),
                    'min_vmaf': float(settings_dict.get('min_vmaf', 0)) if settings_dict.get('min_vmaf') else 0,
                    'vmaf_step': float(settings_dict.get('vmaf_step', 0)) if settings_dict.get('vmaf_step') else 0,
                    'max_encoded_percent': safe_int(settings_dict.get('max_encoded_percent')),
                    'max_encoded_mode': settings_dict.get('max_encoded_mode', 'full'),  # Default to 'full' for old DBs
                    'resize_enabled': settings_dict.get('resize_enabled') == 'True' if settings_dict.get('resize_enabled') else False,
                    'resize_height': safe_int(settings_dict.get('resize_height')),
                    'deband_enabled': settings_dict.get('deband_enabled') == 'True' if settings_dict.get('deband_enabled') else True,
                    'force_8bit_denoised_master': settings_dict.get('force_8bit_denoised_master') == 'True' if settings_dict.get('force_8bit_denoised_master') else False,
                    'audio_compression_enabled': settings_dict.get('audio_compression_enabled') == 'True' if settings_dict.get('audio_compression_enabled') else False,
                    'audio_compression_method': settings_dict.get('audio_compression_method', ''),
                    'auto_vmaf_psnr': settings_dict.get('auto_vmaf_psnr') == 'True' if settings_dict.get('auto_vmaf_psnr') else False,
                    'svt_preset': safe_int(settings_dict.get('svt_preset')),
                    'nvenc_worker_count': safe_int(settings_dict.get('nvenc_worker_count')),
                    'svt_worker_count': safe_int(settings_dict.get('svt_worker_count'), 1),
                    'crf_increment': safe_int(settings_dict.get('crf_increment'), 1),
                    'max_cq_limit': safe_int(settings_dict.get('max_cq_limit'), 0),
                    'nvenc_enabled': settings_dict.get('nvenc_enabled') == 'True' if settings_dict.get('nvenc_enabled') else True,
                    'vdub_validation_disabled': settings_dict.get('vdub_validation_disabled') == 'True' if settings_dict.get('vdub_validation_disabled') else False,
                    'hybrid_path': settings_dict.get('hybrid_path', ''),
                    'videos': videos_list
                }
                
                print(f"DEBUG: load_state_from_db validation - source_path: {state_data.get('source_path')}, videos: {len(state_data.get('videos', []))}")
                
                return state_data
            except (sqlite3.Error, OSError, PermissionError, ValueError, TypeError) as e:
                print(f"DEBUG: load_state_from_db Error: {e}")
                if LOAD_DEBUG:
                    load_debug_log(f"[load_state_from_db] Error: {e}")
                return None
            except Exception as e:
                print(f"DEBUG: load_state_from_db Unexpected error: {e}")
                if LOAD_DEBUG:
                    load_debug_log(f"[load_state_from_db] Unexpected error: {e}")
                return None
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def show_db_notification(self):
        """Show database save notification for 3 seconds."""
        if hasattr(self, 'db_notification_label'):
            self.db_notification_label.config(text="[OK] Database saved", foreground="green")
            # Hide after 3 seconds
            self.root.after(3000, self.hide_db_notification)

    def show_db_update_notification_debounced_pending(self):
        """Flush a queued DB update notification request on the main thread."""
        if not getattr(self, '_db_update_notification_pending', False):
            return
        self._db_update_notification_pending = False
        self.show_db_update_notification_debounced()

    def show_db_update_notification_debounced(self):
        """Show database update notification with debouncing (1 second delay)."""
        self._db_update_notification_pending = False
        # Cancel previous timer if exists
        if hasattr(self, 'db_update_notification_timer') and self.db_update_notification_timer:
            self.root.after_cancel(self.db_update_notification_timer)
        
        # Set new timer
        def show_notification():
            if hasattr(self, 'db_notification_label'):
                self.db_notification_label.config(text="[OK] Database updated", foreground="green")
                # Hide after 3 seconds
                self.root.after(3000, self.hide_db_notification)
            self.db_update_notification_timer = None
        
        self.db_update_notification_timer = self.root.after(1000, show_notification)  # 1 second debounce

    def hide_db_notification(self):
        """Hide database notification."""
        if hasattr(self, 'db_notification_label'):
            self.db_notification_label.config(text="")

    def build_track_editor_cache(self, output_path):
        """Build track editor cache for an output file.

        Runs get_video_streams_for_editor(quick=False) and stores the result
        along with file identity (size + mtime) as JSON in save.db.

        Args:
            output_path: Path to the output (encoded) video file.

        Returns:
            dict or None: The cache dict if built successfully.
        """
        from pathlib import Path as _Path
        output_path = _Path(output_path) if not isinstance(output_path, _Path) else output_path
        if not output_path.exists():
            return None

        try:
            from .core_audio_video_ops import get_video_streams_for_editor
            streams = get_video_streams_for_editor(output_path, quick=False)
            if not streams:
                return None

            stat_info = output_path.stat()
            cache_data = {
                'cached_output_file_size_bytes': stat_info.st_size,
                'cached_output_mtime': stat_info.st_mtime,
            }
            for track_type in ('audio', 'subtitle'):
                tracks = streams.get(track_type, [])
                cache_tracks = []
                for t in tracks:
                    entry = {
                        'original_index': t.get('original_index', t.get('index')),
                        'codec': t.get('codec'),
                        'lang': t.get('lang'),
                        'title': t.get('title'),
                        'default': t.get('default', False),
                        'start_time_ms': t.get('start_time_ms', 0),
                        'size_mb': t.get('size_mb'),
                        'bit_rate': t.get('bit_rate'),
                    }
                    if track_type == 'audio':
                        entry['channels'] = t.get('channels')
                    elif track_type == 'subtitle':
                        entry['forced'] = t.get('forced', False)
                    cache_tracks.append(entry)
                cache_data[track_type] = cache_tracks

            video_info = streams.get('video')
            if video_info:
                cache_data['video'] = {
                    'index': video_info.get('index'),
                    'codec': video_info.get('codec'),
                    'width': video_info.get('width'),
                    'height': video_info.get('height'),
                    'bit_rate': video_info.get('bit_rate'),
                    'rotation': video_info.get('rotation', 0),
                }

            return cache_data
        except Exception:
            return None

    def save_track_editor_cache_to_db(self, video_path, cache_data):
        """Persist track editor cache JSON to the videos table.

        Args:
            video_path: Source video path (PRIMARY KEY in videos table).
            cache_data: Dict from build_track_editor_cache().
        """
        if not hasattr(self, 'db_path') or not self.db_path or not cache_data:
            return
        try:
            cache_json = json.dumps(cache_data, ensure_ascii=True)
        except (TypeError, ValueError):
            return

        video_path_str = str(video_path)
        with self.db_lock:
            conn = None
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                cursor.execute(
                    'UPDATE videos SET output_track_editor_cache = ? WHERE video_path = ?',
                    (cache_json, video_path_str)
                )
                conn.commit()
            except Exception:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def get_track_editor_cache_from_db(self, video_path, output_path=None):
        """Read and validate track editor cache from DB.

        Validates cache by comparing stored file identity (size + mtime)
        against the current output file on disk.

        Args:
            video_path: Source video path (PRIMARY KEY in videos table).
            output_path: Output file path for validation. If None, tries to resolve.

        Returns:
            dict or None: Validated cache dict, or None if missing/invalid.
        """
        if not hasattr(self, 'db_path') or not self.db_path:
            return None

        from pathlib import Path as _Path
        video_path_str = str(video_path)
        with self.db_lock:
            conn = None
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                cursor.execute(
                    'SELECT output_track_editor_cache, output_path FROM videos WHERE video_path = ?',
                    (video_path_str,)
                )
                row = cursor.fetchone()
            except Exception:
                return None
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        if not row or not row[0]:
            return None

        try:
            cache_data = json.loads(row[0])
        except (json.JSONDecodeError, TypeError, ValueError):
            return None

        if not isinstance(cache_data, dict):
            return None

        resolved_output = output_path
        if resolved_output is None and row[1]:
            resolved_output = _Path(row[1])
        if resolved_output is None:
            resolved_output = self.video_to_output.get(_Path(video_path))
        if resolved_output is None:
            try:
                from .core_audio_video_ops import get_output_filename
                resolved_output = get_output_filename(_Path(video_path), self.source_path, self.dest_path)
            except Exception:
                return None
        if resolved_output is None:
            return None

        resolved_output = _Path(resolved_output) if not isinstance(resolved_output, _Path) else resolved_output
        if not resolved_output.exists():
            return None

        try:
            stat_info = resolved_output.stat()
        except (OSError, PermissionError):
            return None

        cached_size = cache_data.get('cached_output_file_size_bytes')
        cached_mtime = cache_data.get('cached_output_mtime')

        if cached_size is None or cached_mtime is None:
            return None

        if stat_info.st_size != cached_size or stat_info.st_mtime != cached_mtime:
            return None

        return cache_data

    def schedule_track_editor_cache_build(self, video_path, output_path):
        """Build and save track editor cache in a background thread.

        Args:
            video_path: Source video path (PRIMARY KEY).
            output_path: Output file to probe.
        """
        from pathlib import Path as _Path
        output_path = _Path(output_path) if not isinstance(output_path, _Path) else output_path
        video_path = _Path(video_path) if not isinstance(video_path, _Path) else video_path

        if not output_path.exists():
            return

        def _build_and_save():
            cache_data = self.build_track_editor_cache(output_path)
            if cache_data:
                self.save_track_editor_cache_to_db(video_path, cache_data)

        t = threading.Thread(target=_build_and_save, daemon=True, name="TrackEditorCacheBuild")
        t.start()


# === PROGRAM PATH-OK KEZELÉSE (Module-level functions) ===

def save_program_path(program, path):
    """Program path mentése az adatbázisba.
    
    Args:
        program: Program neve (pl. 'ffmpeg', 'virtualdub', 'hybrid')
        path: Program elérési útja
    """
    # Import here to avoid circular imports
    from .core_paths_tools_logging import get_log_writer
    
    db_path = Path('save.db')
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS program_paths (
                program_name TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            INSERT OR REPLACE INTO program_paths (program_name, path, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (program, str(path)))
        
        conn.commit()
        
        # Log success
        writer = get_log_writer()
        if writer:
            writer.write(f"[save_program_path] Saved {program} path: {path}\n")
            writer.flush()
    except Exception as e:
        writer = get_log_writer()
        if writer:
            writer.write(f"[save_program_path] Error saving {program}: {e}\n")
            writer.flush()
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def get_saved_program_path(program):
    """Program path betöltése az adatbázisból.
    
    Args:
        program: Program neve
        
    Returns:
        str: A mentett path, vagy None
    """
    db_path = Path('save.db')
    
    if not db_path.exists():
        return None
    
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        cursor.execute(
            'SELECT path FROM program_paths WHERE program_name = ?',
            (program,)
        )
        
        row = cursor.fetchone()
        
        if row:
            path = row[0]
            if Path(path).exists():
                return path
    except Exception:
        pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    
    return None


def get_all_saved_program_paths():
    """Összes mentett program path lekérdezése.
    
    Returns:
        dict: Program nevek és path-ok szótára
    """
    db_path = Path('save.db')
    
    if not db_path.exists():
        return {}
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        cursor.execute('SELECT program_name, path FROM program_paths')
        rows = cursor.fetchall()
        conn.close()
        
        return {name: path for name, path in rows if Path(path).exists()}
    except Exception:
        return {}
