from .gui_imports import *
from .gui_shared import *

class ContextMenusMixin:
    def _write_main_log_line(self, message):
        """Best-effort write to the main application log file."""
        try:
            writer = get_log_writer()
            if not writer:
                return
            if hasattr(writer, 'closed') and writer.closed:
                return
            line = str(message)
            if not line.endswith("\n"):
                line += "\n"
            writer.write(line)
            writer.flush()
        except Exception:
            pass

    def _log_context_menu_event(self, message):
        """Write context-menu selection events to main UI audit log."""
        try:
            log_fn = getattr(self, "_write_user_interaction_log", None)
            if callable(log_fn):
                log_fn(message)
                return
        except Exception:
            pass

    def _ensure_metadata_probe_coordination_support(self):
        """Initialize shared coordination for metadata probe/delete races."""
        if getattr(self, "_metadata_probe_coordination_initialized", False):
            return
        self._metadata_probe_coordination_initialized = True
        self._metadata_probe_state_lock = threading.Lock()
        self._metadata_probe_active_counts = {}
        self._metadata_probe_blocked_paths = set()

    def _normalize_metadata_probe_path_key(self, path_value):
        if not path_value:
            return None
        try:
            return os.path.normcase(os.path.abspath(os.fspath(Path(path_value))))
        except Exception:
            try:
                return os.path.normcase(os.path.abspath(str(path_value)))
            except Exception:
                return str(path_value).lower()

    def _collect_metadata_probe_path_keys(self, *paths):
        keys = []
        for path_value in paths:
            key = self._normalize_metadata_probe_path_key(path_value)
            if key and key not in keys:
                keys.append(key)
        return tuple(keys)

    def _is_metadata_probe_blocked_for_path(self, path_value):
        self._ensure_metadata_probe_coordination_support()
        key = self._normalize_metadata_probe_path_key(path_value)
        if not key:
            return False
        with self._metadata_probe_state_lock:
            return key in self._metadata_probe_blocked_paths

    def _try_begin_metadata_probe_activity(self, *paths):
        """Register an active metadata probe unless deletion already blocked it."""
        self._ensure_metadata_probe_coordination_support()
        keys = self._collect_metadata_probe_path_keys(*paths)
        if not keys:
            return True, tuple()
        with self._metadata_probe_state_lock:
            if any(key in self._metadata_probe_blocked_paths for key in keys):
                return False, keys
            for key in keys:
                self._metadata_probe_active_counts[key] = self._metadata_probe_active_counts.get(key, 0) + 1
        return True, keys

    def _finish_metadata_probe_activity(self, *path_keys):
        self._ensure_metadata_probe_coordination_support()
        keys = self._collect_metadata_probe_path_keys(*path_keys)
        if not keys:
            return
        with self._metadata_probe_state_lock:
            for key in keys:
                current_count = self._metadata_probe_active_counts.get(key, 0)
                if current_count <= 1:
                    self._metadata_probe_active_counts.pop(key, None)
                else:
                    self._metadata_probe_active_counts[key] = current_count - 1

    def _block_metadata_probe_paths(self, *paths):
        self._ensure_metadata_probe_coordination_support()
        keys = self._collect_metadata_probe_path_keys(*paths)
        if not keys:
            return tuple()
        with self._metadata_probe_state_lock:
            self._metadata_probe_blocked_paths.update(keys)
        return keys

    def _unblock_metadata_probe_paths(self, *paths):
        self._ensure_metadata_probe_coordination_support()
        keys = self._collect_metadata_probe_path_keys(*paths)
        if not keys:
            return
        with self._metadata_probe_state_lock:
            for key in keys:
                self._metadata_probe_blocked_paths.discard(key)

    def _wait_for_metadata_probe_paths_idle(self, *paths, timeout_sec=2.5, poll_interval=0.05):
        """Wait briefly until in-flight metadata probes release the target path."""
        self._ensure_metadata_probe_coordination_support()
        keys = self._collect_metadata_probe_path_keys(*paths)
        if not keys:
            return 0
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while True:
            with self._metadata_probe_state_lock:
                active_count = sum(self._metadata_probe_active_counts.get(key, 0) for key in keys)
            if active_count <= 0:
                return 0
            if time.monotonic() >= deadline:
                return active_count
            time.sleep(max(0.01, float(poll_interval)))

    def _delete_output_file_with_probe_guard(self, output_file, video_path=None, item_id=None, log_context="delete_output"):
        """Delete an output file after blocking new app-owned probes for it."""
        if not output_file:
            return
        output_file = Path(output_file)
        if not output_file.exists():
            return

        blocked_keys = self._block_metadata_probe_paths(output_file)
        remaining_probe_count = 0
        try:
            remaining_probe_count = self._wait_for_metadata_probe_paths_idle(output_file)
            last_error = None
            for attempt_idx in range(1, 9):
                try:
                    output_file.unlink()
                    if attempt_idx > 1 or remaining_probe_count:
                        video_name = Path(video_path).name if video_path else "-"
                        self._write_main_log_line(
                            f"[file_delete] Completed | context={log_context} | video={video_name} | item_id={item_id or '-'} | file={output_file.name} | attempts={attempt_idx} | waited_for_probes={remaining_probe_count}"
                        )
                    return
                except FileNotFoundError:
                    return
                except (OSError, PermissionError) as delete_error:
                    last_error = delete_error
                    is_share_violation = getattr(delete_error, "winerror", None) == 32
                    if not is_share_violation and getattr(delete_error, "errno", None) not in (13,):
                        video_name = Path(video_path).name if video_path else "-"
                        self._write_main_log_line(
                            f"[file_delete] Failed | context={log_context} | video={video_name} | item_id={item_id or '-'} | file={output_file.name} | error={delete_error}"
                        )
                        raise
                    if attempt_idx < 8:
                        time.sleep(0.2)
                        continue
                    video_name = Path(video_path).name if video_path else "-"
                    self._write_main_log_line(
                        f"[file_delete] Failed | context={log_context} | video={video_name} | item_id={item_id or '-'} | file={output_file.name} | error={last_error}"
                    )
                    raise last_error
        finally:
            if blocked_keys:
                self._unblock_metadata_probe_paths(*blocked_keys)

        # Fallback if global UI logger is unavailable.
        try:
            writer = get_log_writer()
            if not writer:
                return
            if hasattr(writer, 'closed') and writer.closed:
                return
            writer.write(f"[UI] {message}\n")
            writer.flush()
        except Exception:
            pass

    def _wrap_context_menu_command(self, menu_label, callback, video_path=None, item_id=None, extra=None):
        """Wrap context-menu callbacks so selected menu actions are always logged."""
        def _wrapped():
            parts = [f"type=context_menu_select", f"menu='{menu_label}'"]
            if video_path is not None:
                try:
                    parts.append(f"video='{Path(video_path).name}'")
                except Exception:
                    parts.append(f"video='{video_path}'")
            if item_id is not None:
                parts.append(f"item_id={item_id}")
                try:
                    values = self.tree.item(item_id, 'values')
                    if values and hasattr(self, "COLUMN_INDEX"):
                        status_idx = self.COLUMN_INDEX.get('status')
                        video_idx = self.COLUMN_INDEX.get('video_name')
                        if status_idx is not None and status_idx < len(values):
                            parts.append(f"status='{values[status_idx]}'")
                        if video_idx is not None and video_idx < len(values):
                            parts.append(f"row_video='{values[video_idx]}'")
                except Exception:
                    pass
            if isinstance(extra, dict):
                for key, val in extra.items():
                    parts.append(f"{key}={val}")

            self._log_context_menu_event(" | ".join(parts))
            return callback()

        return _wrapped

    def on_right_click(self, event):
        """Handle right-click event on Treeview items.

        Shows a context menu with options for re-encoding, audio manipulation,
        and VMAF/PSNR testing.
        """

        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        tags = self.tree.item(item_id, 'tags')
        if 'subtitle' in tags:
            return

        # Support for multiple video selection
        selected_items = self.tree.selection()
        if not selected_items:
            selected_items = [item_id]

        # Filter out subtitle items
        selected_video_items = []
        for sel_item in selected_items:
            sel_tags = self.tree.item(sel_item, 'tags')
            if 'subtitle' not in sel_tags:
                selected_video_items.append(sel_item)

        if not selected_video_items:
            return

        # Destroy previous context menu to prevent GDI handle leak
        # (Windows "No more menus can be allocated" error)
        prev_menu = getattr(self, '_context_menu', None)
        if prev_menu is not None:
            try:
                prev_menu.destroy()
            except tk.TclError:
                pass

        menu = tk.Menu(self.root, tearoff=0)
        self._context_menu = menu
    
        # If multiple videos are selected, show bulk actions
        if len(selected_video_items) > 1:
            def multi_menu_cmd(menu_label, callback, **extra):
                meta = {'selected_count': len(selected_video_items)}
                if extra:
                    meta.update(extra)
                return self._wrap_context_menu_command(
                    menu_label,
                    callback,
                    extra=meta
                )

            multi_completed = True
            multi_needs_check_items = []
            for sel_item in selected_video_items:
                values = self.tree.item(sel_item, 'values')
                status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
                sel_tags = self.tree.item(sel_item, 'tags') or ()
                row_needs_check = is_status_needs_check(status) or ('needs_check' in sel_tags)
                row_completed_or_check = is_status_completed(status) or row_needs_check
                video_path = self._get_video_path_by_item(sel_item)
                output_file = self.video_to_output.get(video_path) if video_path else None
                if row_needs_check and video_path and output_file and output_file.exists():
                    multi_needs_check_items.append(sel_item)
                if not (video_path and row_completed_or_check and output_file and output_file.exists()):
                    multi_completed = False

            # "Ellenőrizve" bulk option for needs_check videos
            if multi_needs_check_items:
                verified_multi_label = t('menu_mark_verified_multi').format(count=len(multi_needs_check_items))
                menu.add_command(
                    label=verified_multi_label,
                    command=multi_menu_cmd(
                        verified_multi_label,
                        lambda items=multi_needs_check_items: self._bulk_mark_as_verified(items),
                        action='bulk_mark_as_verified'
                    )
                )
                menu.add_separator()

            reencode_label_key = 'context_multi_reencode_menu' if multi_completed else 'context_multi_encode_menu'
            reencode_menu = tk.Menu(menu, tearoff=0)
            auto_label = self._get_context_label('auto', multi_completed)
            reencode_menu.add_command(
                label=auto_label,
                command=multi_menu_cmd(
                    auto_label,
                    lambda: self.bulk_schedule_auto(selected_video_items, 'auto'),
                    action='bulk_schedule_auto',
                    encoder='auto'
                )
            )
            svt_label = self._get_context_label('svt', multi_completed)
            reencode_menu.add_command(
                label=svt_label,
                command=multi_menu_cmd(
                    svt_label,
                    lambda: self.bulk_schedule_auto(selected_video_items, 'svt'),
                    action='bulk_schedule_auto',
                    encoder='svt'
                )
            )
            nvenc_label = self._get_context_label('nvenc', multi_completed)
            reencode_menu.add_command(
                label=nvenc_label,
                state=tk.NORMAL if self.nvenc_enabled.get() else tk.DISABLED,
                command=multi_menu_cmd(
                    nvenc_label,
                    lambda: self.bulk_schedule_auto(selected_video_items, 'nvenc'),
                    action='bulk_schedule_auto',
                    encoder='nvenc'
                )
            )
            menu.add_cascade(label=t(reencode_label_key), menu=reencode_menu)

            # Manual re-encode submenu (bulk) - available for all files
            # Determine encoder type based on settings
            encoder_type = "NVENC" if self.nvenc_enabled.get() else "SVT-AV1"

            manual_reeencode_menu = tk.Menu(menu, tearoff=0)

            # CQ ranges and quality check options (same as single item menu)
            cq_ranges = [
                ("10-24", 10, 24),
                ("25-39", 25, 39),
                ("40-54", 40, 54),
                ("55-65", 55, 65)
            ]

            quality_checks = [
                (t('manual_quality_vmaf'), 'vmaf'),
                (t('manual_quality_psnr'), 'psnr'),
                (t('manual_quality_both'), 'both'),
                (t('manual_quality_none'), 'none')
            ]

            for range_label, start_cq, end_cq in cq_ranges:
                range_submenu = tk.Menu(manual_reeencode_menu, tearoff=0)

                def _populate_multi_cq_range(
                    _sub=range_submenu, _rl=range_label, _scq=start_cq, _ecq=end_cq,
                    _et=encoder_type, _items=selected_video_items
                ):
                    if _sub.index('end') is not None:
                        return  # Már fel van töltve
                    for cq_val in range(_scq, _ecq + 1):
                        cq_sub = tk.Menu(_sub, tearoff=0)
                        for chk_label, chk_type in quality_checks:
                            cq_sub.add_command(
                                label=chk_label,
                                command=multi_menu_cmd(
                                    f"{t('context_multi_manual_reencode_menu')} / {_rl} / CQ {cq_val} / {chk_label}",
                                    lambda items=_items, c=cq_val, e=_et, ch=chk_type, r=_rl:
                                        self.bulk_manual_reencode(items, c, e, ch, r),
                                    action='bulk_manual_reencode',
                                    cq=cq_val,
                                    encoder=_et,
                                    quality_check=chk_type,
                                    cq_range=_rl,
                                    count=len(_items)
                                )
                            )
                        _sub.add_cascade(label=f"CQ {cq_val}", menu=cq_sub)

                range_submenu.configure(postcommand=_populate_multi_cq_range)
                manual_reeencode_menu.add_cascade(label=range_label, menu=range_submenu)

            menu.add_cascade(label=t('context_multi_manual_reencode_menu'), menu=manual_reeencode_menu)

            if multi_completed:
                menu.add_separator()
                
                rebuild_candidates = []
                for sel_item in selected_video_items:
                    sel_vp = self._get_video_path_by_item(sel_item)
                    if not sel_vp:
                        continue
                    sel_of = self.video_to_output.get(sel_vp)
                    if not sel_of or not sel_of.exists():
                        continue
                    sel_vals = self.tree.item(sel_item, 'values')
                    sel_status = sel_vals[self.COLUMN_INDEX['status']] if len(sel_vals) > self.COLUMN_INDEX['status'] else ""
                    if is_status_rebuild(sel_status):
                        continue
                    from .i18n import is_status_queue as _is_q
                    if _is_q(sel_status):
                        continue
                    rebuild_candidates.append((sel_vp, sel_item))
                
                if rebuild_candidates:
                    rebuild_multi_label = t('menu_rebuild_mkv_multi').format(count=len(rebuild_candidates))
                    menu.add_command(
                        label=rebuild_multi_label,
                        command=multi_menu_cmd(
                            rebuild_multi_label,
                            lambda items=rebuild_candidates: self.schedule_rebuild(items),
                            action='schedule_rebuild',
                            count=len(rebuild_candidates)
                        )
                    )
                
                vmaf_multi_label = t('menu_vmaf_test_multiple').format(count=len(selected_video_items))
                menu.add_command(
                    label=vmaf_multi_label,
                    command=multi_menu_cmd(
                        vmaf_multi_label,
                        lambda: self.request_vmaf_test_multiple(selected_video_items),
                        action='request_vmaf_test_multiple'
                    )
                )

            # Re-read metadata from file (bulk) - available for mixed selections too
            menu.add_separator()
            refresh_multi_label = t('menu_refresh_metadata_multiple').format(count=len(selected_video_items))
            menu.add_command(
                label=refresh_multi_label,
                command=multi_menu_cmd(
                    refresh_multi_label,
                    lambda: self._refresh_metadata_from_file_multiple(selected_video_items),
                    action='refresh_metadata_multiple'
                )
            )

            # Bulk denoise - applies the chosen level to every selected video.
            # bulk_toggle_denoise() handles pending/completed/active rows smartly with one
            # aggregated confirmation (completed outputs are deleted + re-encoded).
            menu.add_separator()
            denoise_multi_menu = tk.Menu(menu, tearoff=0)
            denoise_multi_options = [
                (4, t('menu_denoise_ultra_strong')),
                (3, t('menu_denoise_very_strong')),
                (1, t('menu_denoise_strong')),
                (2, t('menu_denoise_light')),
            ]
            for level, label_text in denoise_multi_options:
                denoise_multi_menu.add_command(
                    label=label_text,
                    command=multi_menu_cmd(
                        f"{t('menu_denoise_multi').format(count=len(selected_video_items))} / {label_text}",
                        lambda items=selected_video_items, lvl=level: self.bulk_toggle_denoise(items, lvl),
                        action='bulk_toggle_denoise',
                        denoise_level=level
                    )
                )
            denoise_multi_menu.add_separator()
            disable_multi_label = t('menu_denoise_disable')
            denoise_multi_menu.add_command(
                label=disable_multi_label,
                command=multi_menu_cmd(
                    f"{t('menu_denoise_multi').format(count=len(selected_video_items))} / {disable_multi_label}",
                    lambda items=selected_video_items: self.bulk_toggle_denoise(items, 0),
                    action='bulk_toggle_denoise',
                    denoise_level=0
                )
            )
            menu.add_cascade(
                label=t('menu_denoise_multi').format(count=len(selected_video_items)),
                menu=denoise_multi_menu
            )

            if menu.index('end') is not None:
                menu.post(event.x_root, event.y_root)
            return
    
        # Single video selected - normal menu
        selected_video_path = None
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                selected_video_path = video_path
                break
    
        if not selected_video_path:
            return

        def single_menu_cmd(menu_label, callback, **extra):
            return self._wrap_context_menu_command(
                menu_label,
                callback,
                video_path=selected_video_path,
                item_id=item_id,
                extra=extra if extra else None
            )
    
        values = self.tree.item(item_id, 'values')
        status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
        cq_str = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else ""
    
        is_completed = is_status_completed(status)
        output_file = self.video_to_output.get(selected_video_path)
        is_needs_check = is_status_needs_check(status) or ('needs_check' in (tags or ()))

        # For needs_check rows, keep "Verified" as the first menu item.
        if is_needs_check and output_file and output_file.exists():
            verified_label = t('menu_mark_verified')
            menu.add_command(
                label=verified_label,
                command=single_menu_cmd(
                    verified_label,
                    lambda: self._mark_as_verified(item_id, selected_video_path),
                    action='mark_as_verified'
                )
            )
            menu.add_separator()
    
        # Open submenu
        open_submenu = tk.Menu(menu, tearoff=0)
        menu.add_cascade(label=t('menu_open'), menu=open_submenu)
        open_source_label = t('menu_source_video')
        open_submenu.add_command(
            label=open_source_label,
            command=single_menu_cmd(
                f"{t('menu_open')} / {open_source_label}",
                lambda: open_video_file(selected_video_path),
                action='open_source_video'
            )
        )
        
        # Check if currently encoding (do not rely only on tags; status text can be newer).
        status_code = normalize_status_to_code(status)
        active_status_codes = {
            'encoding',
            'nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search',
            'svt_encoding', 'svt_validation', 'svt_crf_search',
            'denoising',
        }
        is_encoding = any(tag in tags for tag in ('encoding', 'encoding_nvenc', 'encoding_svt'))
        if status_code in active_status_codes:
            is_encoding = True

        # For encoding in progress, check for temp file (video_only.mkv)
        temp_encoding_file = None
        if is_encoding and output_file:
            # During encoding, the temp file has _video_only.mkv extension
            stem = output_file.stem
            if stem.endswith('.av1'):
                stem = stem[:-4]
            video_only_path = output_file.parent / (stem + '.av1_video_only.mkv')
            if video_only_path.exists():
                temp_encoding_file = video_only_path

        # Track which file we successfully opened (to avoid duplicates)
        encoding_file_added = False

        # PRIORITY 1: If encoding, try temp file first (most likely to be accessible)
        if is_encoding and temp_encoding_file and not encoding_file_added:
            try:
                file_size = temp_encoding_file.stat().st_size
                if file_size > 1024 * 1024:  # > 1MB
                    open_encoding_label = t('menu_encoding_video')
                    open_submenu.add_command(
                        label=open_encoding_label,
                        command=single_menu_cmd(
                            f"{t('menu_open')} / {open_encoding_label}",
                            lambda f=temp_encoding_file: open_video_file(f),
                            action='open_encoding_video_temp'
                        )
                    )
                    encoding_file_added = True
            except OSError:
                pass  # File locked or inaccessible, try next option

        # PRIORITY 2: Try output_file (for completed or if temp failed)
        if output_file and output_file.exists() and not encoding_file_added:
            try:
                file_size = output_file.stat().st_size
                # If completed OR encoding and file > 1MB, show option to open it
                if is_completed:
                    open_encoded_label = t('menu_encoded_video')
                    open_submenu.add_command(
                        label=open_encoded_label,
                        command=single_menu_cmd(
                            f"{t('menu_open')} / {open_encoded_label}",
                            lambda: open_video_file(output_file),
                            action='open_encoded_video'
                        )
                    )
                elif is_encoding and file_size > 1024 * 1024:  # > 1MB
                    open_encoding_label = t('menu_encoding_video')
                    open_submenu.add_command(
                        label=open_encoding_label,
                        command=single_menu_cmd(
                            f"{t('menu_open')} / {open_encoding_label}",
                            lambda: open_video_file(output_file),
                            action='open_encoding_video_output'
                        )
                    )
            except OSError:
                pass  # File locked or inaccessible
        # Track Editor csak Kész vagy Ellenőrizendő állapotú fájloknál
        # FIX: Try to find output file if not set or doesn't exist
        track_editor_file = output_file
        if (is_completed or is_needs_check):
            # If output_file is None or doesn't exist, try to find it
            if not track_editor_file or not track_editor_file.exists():
                try:
                    # Try to get output filename using the standard function
                    if hasattr(self, 'source_path') and hasattr(self, 'dest_path'):
                        from .core_audio_video_ops import get_output_filename
                        track_editor_file = get_output_filename(selected_video_path, self.source_path, self.dest_path)
                except Exception:
                    track_editor_file = None

            # For "completed_copy" status, the source file IS the output file
            if not track_editor_file or not track_editor_file.exists():
                status_code = normalize_status_to_code(status)
                if status_code == 'completed_copy' and selected_video_path.exists():
                    track_editor_file = selected_video_path

            # Show Track Editor if we have a valid file
            if track_editor_file and track_editor_file.exists():
                menu.add_separator()
                track_editor_label = t('menu_track_editor')
                menu.add_command(
                    label=track_editor_label,
                    command=single_menu_cmd(
                        track_editor_label,
                        lambda f=track_editor_file: self.open_track_editor(f, selected_video_path),
                        action='open_track_editor'
                    )
                )
        
        # Denoise toggle - available for pending, completed and actively processed rows too.
        # Active videos are handled inside toggle_denoise() by stop + immediate requeue.
        menu.add_separator()

        # Check current denoise value from tree
        denoise_str = values[self.COLUMN_INDEX['denoise']] if len(values) > self.COLUMN_INDEX['denoise'] else ""

        # Determine current denoise level from display value.
        current_denoise_level = display_to_denoise_level(denoise_str)

        # Create denoise submenu
        denoise_menu = tk.Menu(menu, tearoff=0)

        denoise_options = [
            (4, t('menu_denoise_ultra_strong')),
            (3, t('menu_denoise_very_strong')),
            (1, t('menu_denoise_strong')),
            (2, t('menu_denoise_light')),
        ]
        for level, label_text in denoise_options:
            menu_label = f"[OK] {label_text}" if current_denoise_level == level else label_text
            denoise_menu.add_command(
                label=menu_label,
                command=single_menu_cmd(
                    f"{t('menu_denoise_submenu')} / {menu_label}",
                    lambda i=item_id, p=selected_video_path, denoise_level=level: self.toggle_denoise(i, p, denoise_level),
                    action='toggle_denoise',
                    denoise_level=level
                ),
                state=tk.DISABLED if current_denoise_level == level else tk.NORMAL
            )

        # Disable option (0) - Always show, separate with line
        denoise_menu.add_separator()

        disable_label = t('menu_denoise_disable')

        denoise_menu.add_command(
            label=disable_label,
            command=single_menu_cmd(
                f"{t('menu_denoise_submenu')} / {disable_label}",
                lambda i=item_id, p=selected_video_path: self.toggle_denoise(i, p, 0),
                action='toggle_denoise',
                denoise_level=0
            ),
            state=tk.DISABLED if current_denoise_level == 0 else tk.NORMAL
        )

        menu.add_cascade(label=t('menu_denoise_submenu'), menu=denoise_menu)

        # Hard rotate selection (stored only, applied on next re-encode)
        menu.add_separator()
        current_hard_rotate = 0
        if self.COLUMN_INDEX.get('hard_rotate') is not None:
            rotate_idx = self.COLUMN_INDEX['hard_rotate']
            if len(values) > rotate_idx:
                current_hard_rotate = normalize_hard_rotate_degrees(values[rotate_idx])
        if current_hard_rotate == 0:
            try:
                if hasattr(self, 'video_hard_rotate_lock') and self.video_hard_rotate_lock:
                    with self.video_hard_rotate_lock:
                        current_hard_rotate = normalize_hard_rotate_degrees(
                            getattr(self, 'video_hard_rotate_degrees', {}).get(selected_video_path, 0)
                        )
                else:
                    current_hard_rotate = normalize_hard_rotate_degrees(
                        getattr(self, 'video_hard_rotate_degrees', {}).get(selected_video_path, 0)
                    )
            except Exception:
                current_hard_rotate = 0

        rotate_menu = tk.Menu(menu, tearoff=0)
        rotate_options = [
            (0, t('menu_hard_rotate_off')),
            (90, t('menu_hard_rotate_90')),
            (180, t('menu_hard_rotate_180')),
            (270, t('menu_hard_rotate_270')),
        ]
        for deg, label_text in rotate_options:
            label_final = f"[OK] {label_text}" if deg == current_hard_rotate else label_text
            rotate_menu.add_command(
                label=label_final,
                state=tk.DISABLED if deg == current_hard_rotate else tk.NORMAL,
                command=single_menu_cmd(
                    f"{t('menu_hard_rotate_submenu')} / {label_text}",
                    lambda i=item_id, p=selected_video_path, d=deg: self.set_hard_rotate(i, p, d),
                    action='set_hard_rotate',
                    hard_rotate_degrees=deg
                )
            )
        menu.add_cascade(label=t('menu_hard_rotate_submenu'), menu=rotate_menu)

        auto_menu = tk.Menu(menu, tearoff=0)
        auto_auto_label = self._get_context_label('auto', is_completed)
        auto_menu.add_command(
            label=auto_auto_label,
            command=single_menu_cmd(
                auto_auto_label,
                lambda: self.schedule_auto_encode(selected_video_path, item_id, 'auto'),
                action='schedule_auto_encode',
                encoder='auto'
            )
        )
        auto_nvenc_label = self._get_context_label('nvenc', is_completed)
        auto_menu.add_command(
            label=auto_nvenc_label,
            state=tk.NORMAL if self.nvenc_enabled.get() else tk.DISABLED,
            command=single_menu_cmd(
                auto_nvenc_label,
                lambda: self.schedule_auto_encode(selected_video_path, item_id, 'nvenc'),
                action='schedule_auto_encode',
                encoder='nvenc'
            )
        )
        auto_svt_label = self._get_context_label('svt', is_completed)
        auto_menu.add_command(
            label=auto_svt_label,
            command=single_menu_cmd(
                auto_svt_label,
                lambda: self.reencode_with_svt_av1(selected_video_path, item_id),
                action='reencode_with_svt_av1'
            )
        )
        auto_label = t('context_auto_reencode') if is_completed else t('context_auto_encode')
        menu.add_cascade(label=auto_label, menu=auto_menu)
    
        # Manual re-encode submenu - elérhető minden fájlnál (nem csak completed-nél)
        # Jelenlegi CQ érték meghatározása (ha van)
        current_cq = None
        if cq_str and cq_str != "-":
            try:
                current_cq = int(float(cq_str))
            except (ValueError, TypeError):
                current_cq = None
        
        # Encoder típus meghatározása: tree-ből, vagy default NVENC/SVT választás
        # PERFORMANCE FIX: Nem hívunk get_output_file_info()-t (lassú FFprobe)
        # Ha már kódolt fájlról van szó, akkor a CQ értékből próbáljuk meg kitalálni
        encoder_type = "SVT-AV1"  # default
        if current_cq is not None:
            # Kész fájlnál: ha van CQ érték, akkor feltételezzük hogy az encoder típus
            # megegyezik a jelenlegi beállítással (NVENC enabled vagy sem)
            encoder_type = "NVENC" if self.nvenc_enabled.get() else "SVT-AV1"
        else:
            # Kódolatlan fájlnál: automatikus választás
            encoder_type = "NVENC" if self.nvenc_enabled.get() else "SVT-AV1"
        
        reencode_manual_menu = tk.Menu(menu, tearoff=0)
        menu.add_cascade(label=t('menu_reencode_manual'), menu=reencode_manual_menu)
        
        # CQ tartományok definiálása
        cq_ranges = [
            ("10-24", 10, 24),
            ("25-39", 25, 39),
            ("40-54", 40, 54),
            ("55-65", 55, 65)
        ]
        
        # Minőség ellenőrzési opciók
        quality_checks = [
            (t('manual_quality_vmaf'), 'vmaf'),
            (t('manual_quality_psnr'), 'psnr'),
            (t('manual_quality_both'), 'both'),
            (t('manual_quality_none'), 'none')
        ]
        
        # Háromszintű menü felépítése - lusta (lazy) létrehozással a GUI blokkolás elkerüléséhez.
        # A CQ érték almenük csak akkor jönnek létre, amikor a felhasználó megnyitja a tartomány-almenüt.
        for range_label, start_cq, end_cq in cq_ranges:
            range_submenu = tk.Menu(reencode_manual_menu, tearoff=0)

            def _populate_single_cq_range(
                _sub=range_submenu, _rl=range_label, _scq=start_cq, _ecq=end_cq,
                _et=encoder_type, _ccq=current_cq, _vp=selected_video_path, _iid=item_id
            ):
                if _sub.index('end') is not None:
                    return  # Már fel van töltve
                for cq_val in range(_scq, _ecq + 1):
                    cq_sub = tk.Menu(_sub, tearoff=0)
                    cq_lbl = f"CQ {cq_val}"
                    if _ccq is not None and cq_val == _ccq:
                        cq_lbl += " (current)"
                    for chk_label, chk_type in quality_checks:
                        cq_sub.add_command(
                            label=chk_label,
                            command=single_menu_cmd(
                                f"{t('menu_reencode_manual')} / {_rl} / CQ {cq_val} / {chk_label}",
                                lambda v=_vp, i=_iid, c=cq_val, e=_et, ch=chk_type, r=_rl:
                                    self.reencode_with_manual_config(v, i, c, e, ch, r),
                                action='reencode_with_manual_config',
                                cq=cq_val,
                                encoder=_et,
                                quality_check=chk_type,
                                cq_range=_rl
                            ),
                            state=tk.DISABLED if (_ccq is not None and cq_val == _ccq and chk_type == 'none') else tk.NORMAL
                        )
                    _sub.add_cascade(label=cq_lbl, menu=cq_sub)

            range_submenu.configure(postcommand=_populate_single_cq_range)
            reencode_manual_menu.add_cascade(label=range_label, menu=range_submenu)

        
        # VMAF check (for completed or needs_check status with existing encoded file)
        if (is_completed or is_needs_check) and output_file and output_file.exists():
            # Audio track operations moved to Track Editor
    
            menu.add_separator()
            vmaf_menu = tk.Menu(menu, tearoff=0)
            vmaf_full_label = t('menu_vmaf_full')
            vmaf_menu.add_command(
                label=vmaf_full_label,
                command=single_menu_cmd(
                    f"{t('menu_vmaf_full')} / {vmaf_full_label}",
                    lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=True, check_psnr=True),
                    action='request_vmaf_test',
                    check_vmaf=True,
                    check_psnr=True
                )
            )
            vmaf_only_label = t('menu_vmaf_only')
            vmaf_menu.add_command(
                label=vmaf_only_label,
                command=single_menu_cmd(
                    f"{t('menu_vmaf_full')} / {vmaf_only_label}",
                    lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=True, check_psnr=False),
                    action='request_vmaf_test',
                    check_vmaf=True,
                    check_psnr=False
                )
            )
            psnr_only_label = t('menu_psnr_only')
            vmaf_menu.add_command(
                label=psnr_only_label,
                command=single_menu_cmd(
                    f"{t('menu_vmaf_full')} / {psnr_only_label}",
                    lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=False, check_psnr=True),
                    action='request_vmaf_test',
                    check_vmaf=False,
                    check_psnr=True
                )
            )
            menu.add_cascade(label=t('menu_vmaf_full'), menu=vmaf_menu)

            if output_file and output_file.exists() and not is_status_rebuild(status):
                from .i18n import is_status_queue as _is_q
                if not _is_q(status):
                    rebuild_label = t('menu_rebuild_mkv')
                    menu.add_command(
                        label=rebuild_label,
                        command=single_menu_cmd(
                            rebuild_label,
                            lambda: self.schedule_rebuild([(selected_video_path, item_id)]),
                            action='schedule_rebuild'
                        )
                    )

        # Re-read metadata from file (single) - also useful for unencoded videos
        refresh_label = t('menu_refresh_metadata')
        menu.add_command(
            label=refresh_label,
            command=single_menu_cmd(
                refresh_label,
                lambda: self._refresh_metadata_from_file(selected_video_path, item_id),
                action='refresh_metadata'
            )
        )

        # Add "Show all logs" menu item
        menu.add_separator()
        logs_label = t('menu_all_logs')
        menu.add_command(
            label=logs_label,
            command=single_menu_cmd(
                logs_label,
                lambda: self.show_video_logs(selected_video_path, item_id),
                action='show_video_logs'
            )
        )
    
        if menu.index('end') is not None:
            menu.post(event.x_root, event.y_root)

    # --- needs_check -> completed status transition ---
    _NEEDS_CHECK_TO_COMPLETED = {
        'needs_check':       'completed',
        'needs_check_nvenc':  'completed_nvenc',
        'needs_check_svt':    'completed_svt',
    }

    def _mark_as_verified(self, item_id, video_path):
        """Mark a single needs_check video as verified (completed).

        Transitions the status from needs_check* to the corresponding completed*
        status, updates the tree display and persists the change to the database.
        """
        try:
            values = self.tree.item(item_id, 'values')
            if not values or len(values) <= self.COLUMN_INDEX['status']:
                return

            current_status = values[self.COLUMN_INDEX['status']]
            current_code = normalize_status_to_code(current_status)

            new_code = self._NEEDS_CHECK_TO_COMPLETED.get(current_code)
            if new_code is None:
                return  # Not a needs_check status - nothing to do

            new_status_text = status_code_to_localized(new_code)

            # Update tree row: replace status text, keep everything else
            new_values = list(values)
            new_values[self.COLUMN_INDEX['status']] = new_status_text
            self.tree.item(item_id, values=new_values, tags=('completed',))

            # Update cached metadata
            self.set_tree_item_meta(item_id, status_code=new_code, status_display=new_status_text)

            # Persist to DB in background thread
            cq_str = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else '-'
            vmaf_str = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else '-'
            psnr_str = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else '-'
            orig_size_str = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else '-'
            new_size_str = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else '-'
            change_str = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else '-'
            completed_date = values[self.COLUMN_INDEX['completed_date']] if len(values) > self.COLUMN_INDEX['completed_date'] else ''

            from .i18n import parse_size_to_bytes
            new_size_mb = parse_size_to_bytes(new_size_str) / (1024 ** 2) if new_size_str != '-' else None
            change_percent = None
            if change_str and change_str != '-':
                try:
                    change_percent = float(change_str.replace('%', '').replace('+', '').strip())
                except (ValueError, AttributeError):
                    pass

            def _save_db():
                try:
                    self.update_single_video_in_db(
                        video_path=Path(video_path) if not isinstance(video_path, Path) else video_path,
                        item_id=item_id,
                        status_text=new_status_text,
                        cq_str=cq_str,
                        vmaf_str=vmaf_str,
                        psnr_str=psnr_str,
                        orig_size_str=orig_size_str,
                        new_size_mb=new_size_mb,
                        change_percent=change_percent,
                        completed_date=completed_date,
                    )
                except Exception as e:
                    print(f"Error saving verified status to DB: {e}")

            if hasattr(self, '_start_db_thread'):
                self._start_db_thread(_save_db, name="MarkVerifiedDB")
            else:
                import threading
                threading.Thread(target=_save_db, daemon=True).start()

            self.update_summary_row()

        except (tk.TclError, KeyError, AttributeError) as e:
            print(f"Error marking as verified: {e}")

    def _bulk_mark_as_verified(self, item_ids):
        """Mark multiple needs_check videos as verified (completed).

        Iterates through the given item IDs and applies the same status
        transition as _mark_as_verified for each one.
        """
        for item_id in item_ids:
            video_path = self._get_video_path_by_item(item_id)
            if video_path:
                self._mark_as_verified(item_id, video_path)

    def open_track_editor(self, video_path, source_path=None):
        """Open the track editor dialog for a specific video.

        Args:
            video_path: Path to the output file to edit.
            source_path: Path to the original source file (for displaying original offsets).
        """
        try:
            from .gui_track_editor import TrackEditorDialog

            # Resolve source video path and item_id for this output file.
            source_video_path = source_path
            item_id = None
            if source_video_path in self.video_items:
                item_id = self.video_items.get(source_video_path)
            else:
                # Fallback: find source path by output mapping.
                for src_path, out_path in self.video_to_output.items():
                    if out_path == video_path:
                        source_video_path = src_path
                        item_id = self.video_items.get(src_path)
                        break

                # Legacy fallback: direct key match (if video_items is keyed differently).
                if item_id is None:
                    for vid_path, vid_item_id in self.video_items.items():
                        if vid_path == video_path:
                            source_video_path = vid_path
                            item_id = vid_item_id
                            break

            # Pass callback, item_id, and source_path
            TrackEditorDialog(
                self.root,
                video_path,
                source_path=source_path,
                item_id=item_id,
                on_completion=lambda: self._on_track_editor_complete(video_path, source_video_path, item_id)
            )
        except Exception as e:
            print(f"Error opening track editor: {e}")
            tk.messagebox.showerror(t('msg_error'), t('msg_open_editor_failed').format(error=e))

    def _on_track_editor_complete(self, output_path, source_video_path, item_id):
        """Called after track editor successfully completes.

        Updates the tree item with new file size information after track editing.
        """
        if not item_id:
            return

        try:
            # Get current tree values to preserve existing data
            current_values = self.tree.item(item_id, 'values')
            if not current_values or len(current_values) < 8:
                return

            # Calculate new file sizes (source file was modified in-place)
            from .i18n import format_localized_number
            from .i18n import parse_size_to_bytes
            from datetime import datetime

            output_file = Path(output_path)  # Track editor modifies the file in-place
            output_stat = output_file.stat()
            new_size = output_stat.st_size
            new_size_mb = new_size / (1024 * 1024)

            # Keep original/source size display as-is in the tree row.
            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

            # Calculate change from original source size.
            change_percent = 0.0
            original_source_size = self.get_tree_item_meta(item_id, 'source_size_bytes')
            if original_source_size is None:
                original_source_size = parse_size_to_bytes(orig_size_str)
            if original_source_size and original_source_size > 0:
                change_percent = ((new_size - original_source_size) / original_source_size) * 100

            new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
            change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Send queue update message (same format as audio_worker)
            status = current_values[self.COLUMN_INDEX['status']]
            # If status was changed to "track_saving", restore original completed status from metadata
            if normalize_status_to_code(status) == 'track_saving':
                saved_code = self.get_tree_item_meta(item_id, 'status_code')
                if saved_code:
                    status = status_code_to_localized(saved_code)
            cq = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
            vmaf = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
            psnr = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"

            self.encoding_queue.put((
                "update", item_id, status, cq, vmaf, psnr, "100%",
                orig_size_str, new_size_str, change_str, completed_date,
                {
                    'output_size_bytes': new_size,
                    'output_modified_timestamp': output_stat.st_mtime,
                    'source_size_bytes': original_source_size,
                    'status_code': normalize_status_to_code(status)
                }
            ))
            self.encoding_queue.put(("tag", item_id, self._get_metadata_refresh_tag_for_status(status)))
            self.encoding_queue.put(("update_summary",))

            # Refresh raw cache fields immediately for faster later DB saves.
            self.set_tree_item_meta(
                item_id,
                output_size_bytes=new_size,
                output_modified_timestamp=output_stat.st_mtime,
                source_size_bytes=original_source_size,
                orig_size_display=orig_size_str,
                new_size_display=new_size_str,
                size_change_display=change_str,
                status_display=status,
                progress_display="100%",
                completed_date=completed_date
            )

            # Update only this video in DB (operation-end path, includes probe/cache logic).
            def update_db():
                try:
                    target_video_path = source_video_path
                    if target_video_path is None:
                        for src_path, out_path in self.video_to_output.items():
                            if out_path == output_file:
                                target_video_path = src_path
                                break
                    if target_video_path and hasattr(self, 'update_single_video_in_db'):
                        target_video_path = Path(target_video_path)
                        self.update_single_video_in_db(
                            target_video_path,
                            item_id,
                            status,
                            cq,
                            vmaf,
                            psnr,
                            orig_size_str,
                            new_size_mb,
                            change_percent,
                            completed_date
                        )
                except Exception as db_error:
                    print(f"DB update error after track edit: {db_error}")

                try:
                    if target_video_path and hasattr(self, 'schedule_track_editor_cache_build'):
                        self.schedule_track_editor_cache_build(target_video_path, output_file)
                except Exception:
                    pass

            if hasattr(self, '_start_db_thread'):
                self._start_db_thread(update_db, name="TrackEditDB")
            else:
                import threading
                db_thread = threading.Thread(target=update_db, daemon=True)
                db_thread.start()

            def schedule_post_track_rebuild():
                try:
                    # Ensure the completion update above is visible before rebuild
                    # captures previous_status from the tree row.
                    if hasattr(self, 'check_encoding_queue'):
                        self.check_encoding_queue()

                    target_video_path = source_video_path
                    if target_video_path is None:
                        for src_path, out_path in self.video_to_output.items():
                            if out_path == output_file:
                                target_video_path = src_path
                                break

                    if not target_video_path or not item_id:
                        return

                    target_video_path = Path(target_video_path)
                    if not output_file.exists():
                        return

                    # completed_copy rows may use the source file as the edited
                    # output. Rebuild needs a source->output mapping to find it.
                    if not self.video_to_output.get(target_video_path):
                        self.video_to_output[target_video_path] = output_file

                    if hasattr(self, 'schedule_rebuild'):
                        self.schedule_rebuild([(target_video_path, item_id)])
                except Exception as rebuild_error:
                    print(f"Track edit post-rebuild scheduling error: {rebuild_error}")

            try:
                if hasattr(self, 'check_encoding_queue'):
                    self.root.after_idle(self.check_encoding_queue)
                self.root.after(250, schedule_post_track_rebuild)
            except Exception:
                schedule_post_track_rebuild()

        except Exception as e:
            print(f"Error updating after track edit: {e}")

    def on_double_click(self, event):
        """Handle double-click event on Treeview items.
        
        Opens the encoded video file with the default system player if it exists.
        """
        # Check if click happened in header region
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            # If clicked on header, do nothing
            return
        
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        tags = self.tree.item(item_id, 'tags')
        if 'subtitle' in tags:
            return
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                values = self.tree.item(item_id, 'values')
                status = values[self.COLUMN_INDEX['status']] if 'values' in self.tree.item(item_id) and len(values) > self.COLUMN_INDEX['status'] else ""
                
                output_file = self.video_to_output.get(video_path)

                # Check if item is being encoded (status-aware, not tags-only).
                status_code = normalize_status_to_code(status)
                active_status_codes = {
                    'encoding',
                    'nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search',
                    'svt_encoding', 'svt_validation', 'svt_crf_search',
                    'denoising',
                }
                is_encoding = any(tag in tags for tag in ('encoding', 'encoding_nvenc', 'encoding_svt'))
                if status_code in active_status_codes:
                    is_encoding = True
                elif hasattr(self, '_is_video_actively_encoding'):
                    try:
                        is_active, _, _ = self._is_video_actively_encoding(video_path)
                        if is_active and status_code not in ('nvenc_queue', 'svt_queue'):
                            is_encoding = True
                    except Exception:
                        pass

                # For encoding in progress, check for temp file (video_only.mkv)
                temp_encoding_file = None
                if is_encoding and output_file:
                    # During encoding, the temp file has _video_only.mkv extension
                    video_only_path = output_file.parent / (output_file.stem.replace('.av1', '') + '.av1_video_only.mkv')
                    if video_only_path.exists():
                        temp_encoding_file = video_only_path

                # If completed OR (encoding in progress AND file size > 1MB) -> Open output file
                should_open_output = False
                file_to_open = None

                # PRIORITY 1: If encoding, try temp file first (most likely to be accessible)
                if is_encoding and temp_encoding_file and not should_open_output:
                    try:
                        if temp_encoding_file.stat().st_size > 1024 * 1024:  # > 1MB
                            should_open_output = True
                            file_to_open = temp_encoding_file
                    except OSError:
                        pass  # File locked or inaccessible, try next option

                # PRIORITY 2: Try output_file (for completed or if temp failed)
                if output_file and output_file.exists() and not should_open_output:
                    if is_status_completed(status):
                        should_open_output = True
                        file_to_open = output_file
                    elif is_encoding:
                        # Kérés: "ha 1MB-nál már nagyobb a célvideó mérete"
                        try:
                            # 1 MB = 1024 * 1024 bytes
                            if output_file.stat().st_size > 1024 * 1024:
                                should_open_output = True
                                file_to_open = output_file
                        except OSError:
                            pass  # File locked or inaccessible

                if should_open_output and file_to_open:
                    open_video_file(file_to_open)
                else:
                    # Otherwise (pending, failed, or output too small/missing) -> Open source file
                    if video_path and video_path.exists():
                        open_video_file(video_path)
                    else:
                        messagebox.showinfo(t('msg_info'), t('status_source_missing'))
                break
    
    def show_video_logs(self, video_path, item_id):
        """Show all logs for a specific video in a popup window."""
        if not video_path:
            return
        
        # Normalize for display
        if not isinstance(video_path, Path):
            video_path = Path(video_path)
        target_key = make_video_log_key(video_path)
        
        # Create popup window
        log_window = tk.Toplevel(self.root)
        log_window.title(t('log_window_title').format(filename=video_path.name))
        log_window.geometry("1000x600")
        
        # Create frame for content
        main_frame = ttk.Frame(log_window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header with video info
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        video_name_label = ttk.Label(header_frame, text=t('log_window_video').format(filename=video_path.name), font=("Arial", 12, "bold"))
        video_name_label.pack(anchor=tk.W)
        video_path_label = ttk.Label(header_frame, text=t('log_window_path').format(path=video_path), font=("Arial", 9))
        video_path_label.pack(anchor=tk.W)
        log_count_label = ttk.Label(header_frame, text="", font=("Arial", 9))
        log_count_label.pack(anchor=tk.W, pady=(5, 0))
        
        # Create scrollable text widget
        text_frame = ttk.Frame(main_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        log_text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, font=("Consolas", 9))
        log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=log_text.yview)
        
        def refresh_logs():
            """Refresh log entries from video_logs dictionary."""
            # Snapshot under lock to keep UI-side work deterministic and fast.
            with self.video_logs_lock:
                video_logs = dict(getattr(self, 'video_logs', {}))

            logs = video_logs.get(target_key)
            if logs is None:
                # Legacy fallback (older sessions may still use Path keys).
                logs = []
                for stored_key, stored_logs in video_logs.items():
                    if make_video_log_key(stored_key) == target_key:
                        logs = stored_logs
                        break
            
            # Update log count label
            log_count_label.config(text=t('log_window_count').format(count=len(logs)))
            
            # Clear and populate log text widget
            log_text.config(state=tk.NORMAL)
            log_text.delete(1.0, tk.END)
            
            if logs:
                # Sort logs by timestamp (oldest first)
                sorted_logs = sorted(logs, key=lambda x: x[0] if len(x) > 0 else "")

                rendered_lines = []
                for timestamp, log_type, message in sorted_logs:
                    # Format: [timestamp] [TYPE] message
                    line = f"[{timestamp}] [{log_type}] {message}"
                    if not line.endswith("\n"):
                        line += "\n"
                    rendered_lines.append(line)
                log_text.insert(tk.END, "".join(rendered_lines))
            else:
                log_text.insert(tk.END, t('log_window_no_entries'))
            
            log_text.config(state=tk.DISABLED)
            # Scroll to end to show latest logs
            log_text.see(tk.END)
        
        # Initial load (deferred so the popup can paint before heavy insert).
        log_window.after(10, refresh_logs)
        
        # Button frame with refresh and close buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(button_frame, text=t('btn_refresh'), command=refresh_logs).pack(side=tk.LEFT)
        ttk.Button(button_frame, text=t('btn_close'), command=log_window.destroy).pack(side=tk.RIGHT)

    def set_hard_rotate(self, item_id, video_path, hard_rotate_degrees):
        """Set hard-rotate value for a video (applies only on future re-encode)."""
        try:
            rotate_val = normalize_hard_rotate_degrees(hard_rotate_degrees)
            rotate_display = hard_rotate_to_display(rotate_val)

            current_values = list(self.tree.item(item_id, 'values'))
            if len(current_values) < len(self.COLUMN_INDEX):
                current_values.extend([''] * (len(self.COLUMN_INDEX) - len(current_values)))
            rotate_idx = self.COLUMN_INDEX.get('hard_rotate')
            if rotate_idx is None:
                return
            current_values[rotate_idx] = rotate_display
            self.tree.item(item_id, values=current_values)

            if hasattr(self, 'video_hard_rotate_lock') and self.video_hard_rotate_lock:
                with self.video_hard_rotate_lock:
                    self.video_hard_rotate_degrees[video_path] = rotate_val
            else:
                self.video_hard_rotate_degrees[video_path] = rotate_val

            self.set_tree_item_meta(
                item_id,
                hard_rotate_degrees=rotate_val,
                hard_rotate_display=rotate_display
            )

            status_text_snapshot = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            cq_str_snapshot = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
            vmaf_str_snapshot = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
            psnr_str_snapshot = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
            orig_size_str_snapshot = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
            new_size_str_snapshot = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
            change_percent_str_snapshot = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
            completed_date_snapshot = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""

            from .i18n import parse_size_to_bytes
            new_size_mb_snapshot = parse_size_to_bytes(new_size_str_snapshot) / (1024**2) if new_size_str_snapshot != "-" else None
            if change_percent_str_snapshot and change_percent_str_snapshot != "-":
                try:
                    change_percent_snapshot = float(change_percent_str_snapshot.replace("%", "").replace("+", "").strip())
                except (ValueError, AttributeError):
                    change_percent_snapshot = None
            else:
                change_percent_snapshot = None

            def save_hard_rotate_to_db():
                try:
                    self.update_single_video_in_db(
                        video_path=video_path,
                        item_id=item_id,
                        status_text=status_text_snapshot,
                        cq_str=cq_str_snapshot,
                        vmaf_str=vmaf_str_snapshot,
                        psnr_str=psnr_str_snapshot,
                        orig_size_str=orig_size_str_snapshot,
                        new_size_mb=new_size_mb_snapshot,
                        change_percent=change_percent_snapshot,
                        completed_date=completed_date_snapshot,
                        hard_rotate_degrees=rotate_val
                    )
                except Exception as e:
                    print(f"[ERROR] Error saving hard rotate to DB: {e}")

            if hasattr(self, '_start_db_thread'):
                self._start_db_thread(save_hard_rotate_to_db, name="SaveHardRotate")
            else:
                threading.Thread(target=save_hard_rotate_to_db, daemon=True).start()
        except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"Error setting hard rotate: {e}")

    def bulk_toggle_denoise(self, item_ids, denoise_level):
        """Apply a denoise level to several selected videos at once.

        Categorizes the selection into pending / actively-encoding / completed rows,
        shows ONE aggregated confirmation (only when destructive rows are involved:
        completed ones get their output deleted + re-encoded, active ones get stopped +
        requeued), then delegates each row to toggle_denoise(..., confirmed=True) so the
        existing per-category logic is reused without duplication.

        Args:
            item_ids: Iterable of Treeview item IDs (subtitle rows are ignored).
            denoise_level: 0=disabled, 1=strong, 2=light, 3=very-strong, 4=ultra-strong
        """
        denoise_level = normalize_denoise_level(denoise_level)

        pending, active, completed = [], [], []
        for item_id in item_ids:
            try:
                tags = self.tree.item(item_id, 'tags') or ()
                if 'subtitle' in tags:
                    continue
                video_path = self._get_video_path_by_item(item_id)
                if not video_path:
                    continue
                values = self.tree.item(item_id, 'values')
                status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
            except (tk.TclError, KeyError, AttributeError):
                continue

            is_active = False
            try:
                is_active = bool(self._is_video_actively_encoding(video_path)[0])
            except Exception:
                is_active = False

            entry = (item_id, video_path)
            if is_active:
                active.append(entry)
            elif is_status_completed(status):
                completed.append(entry)
            else:
                pending.append(entry)

        all_entries = pending + completed + active
        if not all_entries:
            return

        denoise_label_map = {
            0: t('menu_denoise_disable'),
            1: t('menu_denoise_strong'),
            2: t('menu_denoise_light'),
            3: t('menu_denoise_very_strong'),
            4: t('menu_denoise_ultra_strong'),
        }
        denoise_label = denoise_label_map.get(denoise_level, "*")

        # Only prompt when destructive rows (completed/active) are involved; a pure
        # pending selection is non-destructive, just like the single-item toggle.
        if completed or active:
            lines = []
            if pending:
                lines.append(t('denoise_multi_line_pending').format(count=len(pending)))
            if completed:
                lines.append(t('denoise_multi_line_completed').format(count=len(completed)))
            if active:
                lines.append(t('denoise_multi_line_active').format(count=len(active)))
            msg = "{0}\n\n{1}\n\n{2}".format(
                t('denoise_multi_header').format(count=len(all_entries), denoise=denoise_label),
                "\n".join(lines),
                t('denoise_multi_footer')
            )
            if not messagebox.askokcancel(t('msg_confirm_reencode_title'), msg):
                return

        self._write_main_log_line(
            f"[context_menu] Bulk denoise requested | count={len(all_entries)} "
            f"(pending={len(pending)}, completed={len(completed)}, active={len(active)}) "
            f"| level={denoise_level} ({denoise_label})"
        )

        for item_id, video_path in all_entries:
            try:
                self.toggle_denoise(item_id, video_path, denoise_level, confirmed=True)
            except Exception as e:
                self.log_status(f"[WARN] Bulk denoise failed for {getattr(video_path, 'name', video_path)}: {e}")

    def toggle_denoise(self, item_id, video_path, denoise_level, confirmed=False):
        """Toggle denoise setting for a video and save to DB immediately.

        Args:
            item_id: The Treeview item ID
            video_path: The Path to the video file
            denoise_level: 0=disabled, 1=strong, 2=light, 3=very-strong, 4=ultra-strong
            confirmed: When True, suppresses the per-item confirmation dialogs (active
                stop+requeue and completed delete+re-encode). Used by bulk_toggle_denoise(),
                which already asks for one aggregated confirmation up front.
        """
        denoise_level = normalize_denoise_level(denoise_level)

        # Active-encoding override: stop current process and immediately requeue
        # with the new denoise setting (including manual CQ/QC parameters if present).
        try:
            current_values = self.tree.item(item_id, 'values')
            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            tags = self.tree.item(item_id, 'tags') or ()
            is_queue_waiting = status in (t('status_svt_queue'), t('status_nvenc_queue'))

            is_actively_processing = False
            active_queue_type = None

            # Primary detection: processing sets are the most reliable source.
            try:
                with self.nvenc_selection_lock:
                    if hasattr(self, 'svt_processing_videos') and video_path in self.svt_processing_videos:
                        is_actively_processing = True
                        active_queue_type = 'svt'
                    elif hasattr(self, 'nvenc_processing_videos') and video_path in self.nvenc_processing_videos:
                        is_actively_processing = True
                        active_queue_type = 'nvenc'
            except Exception:
                pass

            # Fallback: legacy/stale UI tag detection.
            if not is_actively_processing:
                is_encoding_tag = any(tag_name in tags for tag_name in ('encoding', 'encoding_nvenc', 'encoding_svt'))
                if is_encoding_tag and not is_queue_waiting:
                    is_actively_processing = True

            active_task_info = {}
            if hasattr(self, '_is_video_actively_encoding'):
                try:
                    _, task_info, detected_queue_type = self._is_video_actively_encoding(video_path)
                    if isinstance(task_info, dict):
                        active_task_info = dict(task_info)
                    if active_queue_type is None and detected_queue_type in ('svt', 'nvenc', 'nvenc_manual'):
                        active_queue_type = 'nvenc' if detected_queue_type == 'nvenc_manual' else detected_queue_type
                except Exception:
                    pass

            if is_actively_processing:
                denoise_label_map = {
                    0: t('menu_denoise_disable'),
                    1: t('menu_denoise_strong'),
                    2: t('menu_denoise_light'),
                    3: t('menu_denoise_very_strong'),
                    4: t('menu_denoise_ultra_strong'),
                }
                denoise_label = denoise_label_map.get(denoise_level, "*")

                if not confirmed:
                    confirm_msg = t('msg_confirm_stop_and_reencode_denoise').format(
                        filename=video_path.name,
                        denoise=denoise_label
                    )
                    if not messagebox.askokcancel(t('msg_confirm_reencode_title'), confirm_msg):
                        return

                self.log_status(f"[INFO] Active denoise change requested -> stop and requeue: {video_path.name} -> {denoise_label}")
                self._write_main_log_line(
                    f"[context_menu] Active denoise change requested | video={video_path.name} | new_denoise={denoise_label} | action=stop_and_requeue"
                )

                if not hasattr(self, 'stop_encoding_for_video'):
                    messagebox.showerror(t('msg_error'), t('msg_restart_encoding_failed').format(filename=video_path.name))
                    return

                current_values_snapshot = list(current_values)
                current_tags_snapshot = tuple(tags or ())
                status_snapshot = str(status)
                active_queue_type_snapshot = active_queue_type
                active_task_info_snapshot = dict(active_task_info)
                tree_order_index_snapshot = None
                try:
                    tree_order_index_snapshot = int(self.tree.index(item_id))
                except Exception:
                    tree_order_index_snapshot = None
                nvenc_enabled_snapshot = bool(self.nvenc_enabled.get())

                try:
                    initial_min_vmaf_snapshot = float(self.min_vmaf.get())
                except Exception:
                    initial_min_vmaf_snapshot = 97.5
                try:
                    vmaf_step_snapshot = float(self.vmaf_step.get())
                except Exception:
                    vmaf_step_snapshot = 0.25
                try:
                    max_encoded_snapshot = float(self.max_encoded_percent.get())
                except Exception:
                    max_encoded_snapshot = 72.0
                resize_enabled_snapshot = bool(self.resize_enabled.get())
                resize_height_snapshot = self.resize_height.get()
                audio_compression_enabled_snapshot = bool(self.audio_compression_enabled.get())
                audio_compression_method_snapshot = self.audio_compression_method.get()

                def _show_restart_error_async(error_text):
                    try:
                        self.root.after(0, lambda: messagebox.showerror(t('msg_error'), error_text))
                    except Exception:
                        pass

                def _safe_int(value):
                    try:
                        if value is None:
                            return None
                        return int(value)
                    except (TypeError, ValueError):
                        return None

                def _restart_active_encoding_worker():
                    try:
                        self.log_status(f"[INFO] Denoise active restart requested: {video_path.name}")
                        stop_success = self.stop_encoding_for_video(video_path, cleanup_denoised_master=False)
                        if not stop_success:
                            _show_restart_error_async(t('msg_restart_encoding_failed').format(filename=video_path.name))
                            return

                        if not self._wait_for_video_stop_completion(video_path):
                            self.log_status(f"[WARN] Denoise restart timeout waiting for stop-event cleanup: {video_path.name}")
                            _show_restart_error_async(t('msg_restart_encoding_failed').format(filename=video_path.name))
                            return

                        queue_type = active_queue_type_snapshot
                        if queue_type not in ('svt', 'nvenc'):
                            status_upper = status_snapshot.upper()
                            if 'SVT' in status_upper:
                                queue_type = 'svt'
                            elif 'NVENC' in status_upper:
                                queue_type = 'nvenc'
                            else:
                                queue_type = 'nvenc' if nvenc_enabled_snapshot else 'svt'

                        output_file = self.video_to_output.get(video_path)
                        if not output_file:
                            from .core_audio_video_ops import get_output_filename
                            output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                            self.video_to_output[video_path] = output_file

                        if output_file and output_file.exists():
                            try:
                                if hasattr(self, '_delete_output_file_with_probe_guard'):
                                    self._delete_output_file_with_probe_guard(
                                        output_file,
                                        video_path=video_path,
                                        item_id=item_id,
                                        log_context="active_denoise_restart"
                                    )
                                else:
                                    output_file.unlink()
                            except Exception:
                                pass

                        valid_subtitles = active_task_info_snapshot.get('subtitle_files')
                        invalid_subtitles = active_task_info_snapshot.get('invalid_subtitles')
                        if not isinstance(valid_subtitles, list) or invalid_subtitles is None:
                            valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)

                        manual_cq_value = _safe_int(active_task_info_snapshot.get('manual_cq_value'))
                        if manual_cq_value is None:
                            manual_cq_value = _safe_int(active_task_info_snapshot.get('target_cq'))
                        manual_quality_check = active_task_info_snapshot.get('manual_quality_check')
                        manual_cq_range = active_task_info_snapshot.get('manual_cq_range')

                        if manual_cq_value is None and "(M " in status_snapshot and "CQ:" in status_snapshot:
                            try:
                                manual_part = status_snapshot.split("(M ", 1)[1].split(")", 1)[0]
                                manual_cq_value = _safe_int(manual_part.split("CQ:", 1)[1].split()[0])
                                manual_upper = manual_part.upper()
                                if manual_quality_check is None:
                                    if "VMAF" in manual_upper and "PSNR" in manual_upper:
                                        manual_quality_check = 'both'
                                    elif "VMAF" in manual_upper:
                                        manual_quality_check = 'vmaf'
                                    elif "PSNR" in manual_upper:
                                        manual_quality_check = 'psnr'
                                    else:
                                        manual_quality_check = 'none'
                            except Exception:
                                pass

                        is_manual = manual_cq_value is not None
                        status_suffix = ""
                        if is_manual:
                            try:
                                if hasattr(self, '_format_manual_status_suffix'):
                                    status_suffix = self._format_manual_status_suffix(manual_cq_value, manual_quality_check)
                            except Exception:
                                status_suffix = ""
                            if not status_suffix:
                                status_suffix = f" (M CQ:{manual_cq_value})"

                        orig_size_str = current_values_snapshot[self.COLUMN_INDEX['orig_size']] if len(current_values_snapshot) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values_snapshot[self.COLUMN_INDEX['completed_date']] if len(current_values_snapshot) > self.COLUMN_INDEX['completed_date'] else ""

                        queued = False
                        if queue_type == 'svt':
                            new_status_text = t('status_svt_queue') + status_suffix
                            queued = self.add_to_svt_queue(
                                video_path=video_path,
                                item_id=item_id,
                                task_type='encode',
                                is_manual=is_manual,
                                output_file=output_file,
                                subtitle_files=valid_subtitles,
                                invalid_subtitles=invalid_subtitles,
                                orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf_snapshot,
                                vmaf_step=vmaf_step_snapshot,
                                max_encoded=max_encoded_snapshot,
                                resize_enabled=resize_enabled_snapshot,
                                resize_height=resize_height_snapshot,
                                audio_compression_enabled=audio_compression_enabled_snapshot,
                                audio_compression_method=audio_compression_method_snapshot,
                                target_cq=manual_cq_value if is_manual else None,
                                skip_crf_search=is_manual,
                                manual_cq_range=manual_cq_range if is_manual else None,
                                manual_cq_value=manual_cq_value if is_manual else None,
                                manual_quality_check=manual_quality_check if is_manual else None,
                                denoise_enabled=denoise_level,
                                pre_cached_values=current_values_snapshot,
                                pre_cached_tags=current_tags_snapshot,
                                pre_cached_tree_index=tree_order_index_snapshot,
                                queue_front=True,
                                reason='denoise_toggle_active_restart'
                            )
                            if queued and hasattr(self, '_ensure_svt_workers_running'):
                                self._ensure_svt_workers_running()
                        else:
                            new_status_text = t('status_nvenc_queue') + status_suffix
                            queued = self.add_to_nvenc_queue(
                                video_path=video_path,
                                item_id=item_id,
                                is_manual=is_manual,
                                output_file=output_file,
                                subtitle_files=valid_subtitles,
                                invalid_subtitles=invalid_subtitles,
                                orig_size_str=orig_size_str,
                                initial_min_vmaf=initial_min_vmaf_snapshot,
                                vmaf_step=vmaf_step_snapshot,
                                max_encoded=max_encoded_snapshot,
                                resize_enabled=resize_enabled_snapshot,
                                resize_height=resize_height_snapshot,
                                audio_compression_enabled=audio_compression_enabled_snapshot,
                                audio_compression_method=audio_compression_method_snapshot,
                                target_cq=manual_cq_value if is_manual else None,
                                skip_crf_search=is_manual,
                                manual_cq_range=manual_cq_range if is_manual else None,
                                manual_cq_value=manual_cq_value if is_manual else None,
                                manual_quality_check=manual_quality_check if is_manual else None,
                                denoise_enabled=denoise_level,
                                pre_cached_values=current_values_snapshot,
                                pre_cached_tags=current_tags_snapshot,
                                pre_cached_tree_index=tree_order_index_snapshot,
                                queue_front=True,
                                reason='denoise_toggle_active_restart'
                            )
                            if queued and hasattr(self, '_ensure_nvenc_workers_running'):
                                self._ensure_nvenc_workers_running()

                        if not queued:
                            _show_restart_error_async(t('msg_restart_encoding_failed').format(filename=video_path.name))
                            return

                        self.log_status(f"[OK] Denoise active restart queued: {video_path.name} -> {new_status_text}")
                        self.encoding_queue.put(("update", item_id, new_status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                    except Exception as worker_error:
                        print(f"Error in denoise restart worker: {worker_error}")
                        import traceback
                        traceback.print_exc()
                        _show_restart_error_async(t('msg_restart_encoding_failed').format(filename=video_path.name))

                import threading
                threading.Thread(target=_restart_active_encoding_worker, daemon=True, name="DenoiseActiveRestart").start()
        except Exception as e:
            print(f"Error in denoise toggle encoding check: {e}")
            import traceback
            traceback.print_exc()


        try:
            # Check if video is already completed
            current_values = list(self.tree.item(item_id, 'values'))
            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            is_completed = is_status_completed(status)
            
            if is_completed:
                # Ask callback confirmation (skipped when called from bulk_toggle_denoise)
                if not confirmed and not messagebox.askokcancel(t('msg_confirm_reencode_title'), t('msg_confirm_reencode_denoise')):
                    return
                    
                # Delete output file
                output_file = self.video_to_output.get(video_path)
                if output_file and output_file.exists():
                    try:
                        if hasattr(self, '_delete_output_file_with_probe_guard'):
                            self._delete_output_file_with_probe_guard(
                                output_file,
                                video_path=video_path,
                                item_id=item_id,
                                log_context="context_completed_denoise_change"
                            )
                        else:
                            os.remove(output_file)
                    except Exception as e:
                        messagebox.showerror(t('msg_error'), f"{t('msg_delete_failed')} {e}")
                        return
                
                # Reset status to queue based on configuration
                nvenc_enabled = self.nvenc_enabled.get()
                new_status_code = 'nvenc_queue' if nvenc_enabled else 'svt_queue'
                new_status_text = status_code_to_localized(new_status_code)
                
                # Update status in values
                current_values[self.COLUMN_INDEX['status']] = new_status_text
                
                # Clear progress and size columns
                for col in ('progress', 'new_size', 'size_change', 'vmaf', 'psnr', 'completed_date'):
                     if col in self.COLUMN_INDEX:
                        idx = self.COLUMN_INDEX[col]
                        if idx < len(current_values):
                             current_values[idx] = "-"
                             
                # Remove 'completed' tag and ensure 'pending' tag
                tags = list(self.tree.item(item_id, 'tags'))
                if 'completed' in tags:
                    tags.remove('completed')
                if 'completed_copy' in tags:
                    tags.remove('completed_copy')
                if 'pending' not in tags:
                    tags.append('pending')
                self.tree.item(item_id, tags=tuple(tags))
                
                # CRITICAL FIX: If encoding is already running, explicitly add to queue
                # Otherwise the worker will not pick up this video (only status changed, but queue not updated)
                if self.is_encoding:
                    # Prepare task parameters
                    if not output_file:
                        from .core_audio_video_ops import get_output_filename
                        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                    
                    valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                    
                    # Get current encoding settings
                    initial_min_vmaf = float(self.min_vmaf.get())
                    vmaf_step = float(self.vmaf_step.get())
                    max_encoded = float(self.max_encoded_percent.get())
                    resize_enabled = self.resize_enabled.get()
                    resize_height = self.resize_height.get()
                    audio_compression_enabled = self.audio_compression_enabled.get()
                    audio_compression_method = self.audio_compression_method.get()
                    
                    # Add to appropriate queue
                    if new_status_code == 'svt_queue':
                        self.add_to_svt_queue(
                            video_path=video_path,
                            item_id=item_id,
                            task_type='encode',
                            is_manual=False,
                            output_file=output_file,
                            subtitle_files=valid_subtitles,
                            invalid_subtitles=invalid_subtitles,
                            orig_size_str=orig_size_str,
                            initial_min_vmaf=initial_min_vmaf,
                            vmaf_step=vmaf_step,
                            max_encoded=max_encoded,
                            resize_enabled=resize_enabled,
                            resize_height=resize_height,
                            audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method,
                            denoise_enabled=denoise_level,
                            reason='denoise_toggle'
                        )
                        # Update status to reflect it's in queue
                        self.encoding_queue.put(("update", item_id, new_status_text, "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                    elif new_status_code == 'nvenc_queue':
                        self.add_to_nvenc_queue(
                            video_path=video_path,
                            item_id=item_id,
                            is_manual=False,
                            output_file=output_file,
                            subtitle_files=valid_subtitles,
                            invalid_subtitles=invalid_subtitles,
                            orig_size_str=orig_size_str,
                            initial_min_vmaf=initial_min_vmaf,
                            vmaf_step=vmaf_step,
                            max_encoded=max_encoded,
                            resize_enabled=resize_enabled,
                            resize_height=resize_height,
                            audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method,
                            denoise_enabled=denoise_level,
                            reason='denoise_toggle'
                        )
                        # Update status to reflect it's in queue
                        self.encoding_queue.put(("update", item_id, new_status_text, "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                        self.encoding_queue.put(("tag", item_id, "pending"))

            # Determine display value based on denoise_level.
            new_value = denoise_level_to_display(denoise_level)
            
            current_values[self.COLUMN_INDEX['denoise']] = new_value
            self.tree.item(item_id, values=current_values)
            
            # Store in internal dict (thread-safe) - now stores integer level
            with self.video_denoise_lock:
                self.video_denoise_enabled[video_path] = denoise_level

            self.set_tree_item_meta(
                item_id,
                denoise_enabled=denoise_level,
                denoise_display=new_value,
                status_display=current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else "-",
                progress_display=current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else "-",
                orig_size_display=current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                new_size_display=current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                size_change_display=current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                completed_date=current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
            )
            
            # CRITICAL FIX: Save denoise setting to database IMMEDIATELY
            # Previous _save_settings_debounced() was WRONG - it only saves global settings,
            # NOT video-specific data like denoise_enabled!
            # Use update_single_video_in_db() to save ONLY this video with denoise_enabled parameter
            status_text_snapshot = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            cq_str_snapshot = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
            vmaf_str_snapshot = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
            psnr_str_snapshot = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
            orig_size_str_snapshot = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
            new_size_str_snapshot = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
            change_percent_str_snapshot = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
            completed_date_snapshot = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""

            from .i18n import parse_size_to_bytes
            new_size_mb_snapshot = parse_size_to_bytes(new_size_str_snapshot) / (1024**2) if new_size_str_snapshot != "-" else None
            if change_percent_str_snapshot and change_percent_str_snapshot != "-":
                try:
                    change_percent_snapshot = float(change_percent_str_snapshot.replace("%", "").replace("+", "").strip())
                except (ValueError, AttributeError):
                    change_percent_snapshot = None
            else:
                change_percent_snapshot = None

            def save_denoise_to_db():
                try:
                    print(f"[DEBUG] save_denoise_to_db() called for {video_path.name}, denoise_level={denoise_level}")

                    print(f"[DEBUG] Calling update_single_video_in_db with denoise_enabled={denoise_level}")

                    # Call update with denoise_enabled parameter
                    self.update_single_video_in_db(
                        video_path=video_path,
                        item_id=item_id,
                        status_text=status_text_snapshot,
                        cq_str=cq_str_snapshot,
                        vmaf_str=vmaf_str_snapshot,
                        psnr_str=psnr_str_snapshot,
                        orig_size_str=orig_size_str_snapshot,
                        new_size_mb=new_size_mb_snapshot,
                        change_percent=change_percent_snapshot,
                        completed_date=completed_date_snapshot,
                        denoise_enabled=denoise_level  # CRITICAL: Pass denoise level to DB
                    )
                    
                    print(f"[DEBUG] update_single_video_in_db completed successfully")
                except Exception as e:
                    print(f"[ERROR] Error saving denoise to DB: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Run in background thread to avoid GUI freeze
            print(f"[DEBUG] Starting save_denoise_to_db() thread...")
            import threading
            if hasattr(self, '_start_db_thread'):
                self._start_db_thread(save_denoise_to_db, name="SaveDenoise")
            else:
                threading.Thread(target=save_denoise_to_db, daemon=True).start()
            
            # Update summary row if status changed
            self.update_summary_row()
            
        except (tk.TclError, KeyError, AttributeError, TypeError) as e:
            print(f"Error toggling denoise: {e}")

    # ── Re-read metadata from file ──────────────────────────────────────

    def _ensure_metadata_refresh_async_support(self):
        """Initialize async metadata refresh worker queue and poller."""
        if getattr(self, '_metadata_refresh_support_initialized', False):
            return
        self._metadata_refresh_support_initialized = True
        self._metadata_refresh_queue = queue.Queue()
        self._metadata_refresh_inflight = set()
        try:
            self.root.after(100, self._poll_metadata_refresh_queue)
        except tk.TclError:
            pass

    def _get_metadata_refresh_denoise_sidecar_level(self, video_path, output_file):
        """Read denoise level sidecar for an encoded output, if the master still exists."""
        try:
            if not video_path or not output_file:
                return 0
            video_path = Path(video_path)
            output_file = Path(output_file)
            denoised_master_path = output_file.parent / f"{video_path.stem}_denoised_master.mkv"
            level_file = denoised_master_path.with_suffix('.denoise_level')
            if not level_file.exists():
                return 0
            from .gui_shared_worker import read_denoise_level_file
            return normalize_denoise_level(read_denoise_level_file(level_file))
        except Exception:
            return 0

    def _get_metadata_refresh_reset_queue_status_code(self):
        """Return the queue status to use when a failed item is made retryable again."""
        try:
            if hasattr(self, '_get_nvenc_enabled_snapshot'):
                nvenc_enabled = self._get_nvenc_enabled_snapshot()
            elif hasattr(self, 'nvenc_enabled'):
                nvenc_enabled = bool(self.nvenc_enabled.get())
            else:
                nvenc_enabled = bool(getattr(self, 'current_nvenc_enabled', False))
        except Exception:
            nvenc_enabled = bool(getattr(self, 'current_nvenc_enabled', False))
        return 'nvenc_queue' if nvenc_enabled else 'svt_queue'

    def _get_metadata_refresh_tag_for_status(self, status_text):
        """Map refreshed status text to the Treeview tag used by queue logic."""
        status_code = normalize_status_to_code(status_text)
        if is_status_needs_check(status_text):
            return "needs_check"
        if status_code == 'completed_copy':
            return "completed_copy"
        if is_status_failed(status_text):
            return "failed"
        if is_status_completed(status_text):
            return "completed"
        if status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search'):
            return "encoding_nvenc"
        if status_code in ('svt_encoding', 'svt_validation', 'svt_crf_search', 'vmaf_calculating', 'vmaf_only', 'psnr_only'):
            return "encoding_svt"
        return "pending"

    def _refresh_metadata_probe_worker(self, video_path, item_id, output_file):
        """Background probe worker for metadata refresh."""
        from .core_audio_video_ops import get_video_info, get_video_extra_metadata, extract_settings_from_file

        probe_registration = None
        try:
            video_path = Path(video_path)
            output_file = Path(output_file) if output_file else None
            source_exists = video_path.exists()
            has_output_file = bool(output_file and output_file.exists())
            probe_targets = [video_path]
            if has_output_file:
                probe_targets.append(output_file)

            if hasattr(self, '_try_begin_metadata_probe_activity'):
                probe_allowed, probe_registration = self._try_begin_metadata_probe_activity(*probe_targets)
                if not probe_allowed:
                    raise RuntimeError("metadata probe skipped because the file is pending delete/re-encode")

            output_cq_crf = None
            output_vmaf = None
            output_psnr = None
            output_frame_count = None
            new_size_bytes = None
            output_modified_date = None
            output_encoder_type = None
            output_duration_seconds = None
            output_denoise_info = None
            output_denoise_sidecar_level = 0
            output_settings_str = ""

            source_duration_seconds = None
            source_frame_count = None
            source_fps = None
            try:
                src_dur, src_fps = get_video_info(video_path)
                if src_dur is not None:
                    source_duration_seconds = src_dur
                    source_fps = src_fps
                    if src_dur and src_fps:
                        source_frame_count = int(src_dur * src_fps)
            except Exception:
                pass

            source_size_bytes = None
            try:
                source_size_bytes = video_path.stat().st_size
            except OSError:
                pass

            output_stat_mtime = None
            if has_output_file:
                (output_cq_crf, output_vmaf, output_psnr, output_frame_count,
                 new_size_bytes, output_modified_date, output_encoder_type,
                 _should_delete, output_duration_seconds,
                 output_denoise_info) = get_output_file_info(output_file)
                output_settings_str = extract_settings_from_file(output_file)
                output_denoise_sidecar_level = self._get_metadata_refresh_denoise_sidecar_level(video_path, output_file)

            if has_output_file and (new_size_bytes is None or new_size_bytes <= 0):
                try:
                    new_size_bytes = output_file.stat().st_size
                except OSError:
                    new_size_bytes = None
            if has_output_file:
                try:
                    output_stat_mtime = output_file.stat().st_mtime
                except OSError:
                    output_stat_mtime = None

            result = {
                'source_exists': source_exists,
                'has_output_file': has_output_file,
                'output_cq_crf': output_cq_crf,
                'output_vmaf': output_vmaf,
                'output_psnr': output_psnr,
                'output_frame_count': output_frame_count,
                'new_size_bytes': new_size_bytes,
                'output_modified_date': output_modified_date,
                'output_encoder_type': output_encoder_type,
                'output_duration_seconds': output_duration_seconds,
                'output_denoise_info': output_denoise_info,
                'output_denoise_sidecar_level': output_denoise_sidecar_level,
                'output_settings_str': output_settings_str,
                'source_duration_seconds': source_duration_seconds,
                'source_frame_count': source_frame_count,
                'source_fps': source_fps,
                'source_size_bytes': source_size_bytes,
                'output_modified_timestamp': output_stat_mtime,
                'source_extra_metadata': get_video_extra_metadata(video_path),
                'output_extra_metadata': get_video_extra_metadata(output_file) if has_output_file else None,
            }
            self._metadata_refresh_queue.put(('success', video_path, item_id, result))
        except Exception as e:
            self._metadata_refresh_queue.put(('error', Path(video_path), item_id, str(e)))
        finally:
            if probe_registration and hasattr(self, '_finish_metadata_probe_activity'):
                self._finish_metadata_probe_activity(*probe_registration)

    def _apply_refreshed_metadata_result(self, video_path, item_id, result):
        """Apply probed metadata to tree immediately, then persist to DB."""
        from .i18n import format_size_mb

        if not hasattr(self, 'tree') or self.tree is None:
            return
        try:
            if not self.tree.exists(item_id):
                return
        except tk.TclError:
            return

        current_values = self.tree.item(item_id, 'values')
        if not current_values or len(current_values) < 14:
            return

        current_values = list(current_values)

        output_cq_crf = result.get('output_cq_crf')
        output_vmaf = result.get('output_vmaf')
        output_psnr = result.get('output_psnr')
        output_frame_count = result.get('output_frame_count')
        new_size_bytes = result.get('new_size_bytes')
        output_encoder_type = result.get('output_encoder_type')
        output_duration_seconds = result.get('output_duration_seconds')
        output_denoise_info = result.get('output_denoise_info')
        output_settings_str = result.get('output_settings_str')
        source_duration_seconds = result.get('source_duration_seconds')
        source_frame_count = result.get('source_frame_count')
        source_fps = result.get('source_fps')
        source_size_bytes = result.get('source_size_bytes')
        output_modified_timestamp = result.get('output_modified_timestamp')
        source_extra_metadata = result.get('source_extra_metadata')
        output_extra_metadata = result.get('output_extra_metadata')
        source_exists = bool(result.get('source_exists', True))
        has_output_file = bool(result.get('has_output_file'))

        video_name_display = current_values[self.COLUMN_INDEX['video_name']]
        completed_date = current_values[self.COLUMN_INDEX['completed_date']]
        hard_rotate_display = current_values[self.COLUMN_INDEX['hard_rotate']]
        displayed_status_text = current_values[self.COLUMN_INDEX['status']]
        original_refresh_status = self.get_tree_item_meta(item_id, 'metadata_refresh_original_status')
        if normalize_status_to_code(displayed_status_text) == 'metadata_refresh' and original_refresh_status:
            current_status_text = original_refresh_status
        else:
            current_status_text = displayed_status_text
        current_progress_text = current_values[self.COLUMN_INDEX['progress']]
        current_status_code = normalize_status_to_code(current_status_text) or self.get_tree_item_meta(item_id, 'status_code')
        reset_failed_for_retry = (
            not has_output_file and
            source_exists and
            current_status_code in ('failed', 'source_missing', 'file_missing', 'load_error', 'vmaf_error')
        )

        cq_str = str(output_cq_crf) if output_cq_crf is not None else current_values[self.COLUMN_INDEX['cq']]
        vmaf_str = format_localized_number(output_vmaf, decimals=2) if output_vmaf is not None else current_values[self.COLUMN_INDEX['vmaf']]
        psnr_str = format_localized_number(output_psnr, decimals=2) if output_psnr is not None else current_values[self.COLUMN_INDEX['psnr']]

        orig_size_str = format_size_mb(source_size_bytes) if source_size_bytes else current_values[self.COLUMN_INDEX['orig_size']]

        if new_size_bytes is not None and new_size_bytes > 0:
            new_size_mb = new_size_bytes / (1024 * 1024)
            new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
        else:
            new_size_mb = None
            new_size_str = current_values[self.COLUMN_INDEX['new_size']]

        change_percent = None
        if source_size_bytes and source_size_bytes > 0 and new_size_bytes:
            change_percent = ((new_size_bytes - source_size_bytes) / source_size_bytes) * 100
            change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
        else:
            change_str = current_values[self.COLUMN_INDEX['size_change']]

        if has_output_file:
            duration_str, frames_str = self._build_duration_frames_display(
                source_duration_seconds=source_duration_seconds,
                source_frame_count=source_frame_count,
                target_duration_seconds=output_duration_seconds,
                target_frame_count=output_frame_count,
                show_target=True
            )

            current_status_code = self.get_tree_item_meta(item_id, 'status_code', current_status_code or 'completed')
            if output_encoder_type:
                if output_encoder_type == 'nvenc':
                    status_code = 'completed_nvenc'
                elif output_encoder_type == 'svt-av1':
                    status_code = 'completed_svt'
                else:
                    status_code = 'completed'
            else:
                status_code = current_status_code
            status_text = status_code_to_localized(status_code)
            manual_info = infer_manual_cq_from_settings(output_settings_str, output_cq_crf)
            manual_cq_value = self.get_tree_item_meta(item_id, 'manual_cq_value')
            manual_quality_check = self.get_tree_item_meta(item_id, 'manual_quality_check')
            manual_cq_range = self.get_tree_item_meta(item_id, 'manual_cq_range')
            if manual_info:
                manual_cq_value = manual_info.get('manual_cq_value', manual_cq_value)
                manual_quality_check = manual_info.get('manual_quality_check', manual_quality_check)
                manual_cq_range = manual_info.get('manual_cq_range', manual_cq_range)
            if manual_cq_value is not None and hasattr(self, '_rebuild_manual_status_text'):
                status_text = self._rebuild_manual_status_text(status_text, manual_cq_value, manual_quality_check)
            progress_text = "100%"
        else:
            manual_cq_value = self.get_tree_item_meta(item_id, 'manual_cq_value')
            manual_quality_check = self.get_tree_item_meta(item_id, 'manual_quality_check')
            manual_cq_range = self.get_tree_item_meta(item_id, 'manual_cq_range')
            duration_str, frames_str = self._build_duration_frames_display(
                source_duration_seconds=source_duration_seconds,
                source_frame_count=source_frame_count,
                show_target=False
            )
            if reset_failed_for_retry:
                status_code = self._get_metadata_refresh_reset_queue_status_code()
                base_status_text = status_code_to_localized(status_code)
                if hasattr(self, '_build_saved_manual_queue_status'):
                    status_text = self._build_saved_manual_queue_status(
                        status_code,
                        current_status_text,
                        manual_cq_value,
                        manual_quality_check
                    )
                elif hasattr(self, '_build_manual_queue_status_text'):
                    status_text = self._build_manual_queue_status_text(
                        base_status_text,
                        manual_cq_value,
                        manual_quality_check
                    )
                else:
                    status_text = base_status_text
                cq_str = "-"
                vmaf_str = "-"
                psnr_str = "-"
                new_size_mb = None
                new_size_str = "-"
                change_percent = None
                change_str = "-"
                completed_date = ""
                progress_text = "-"
            else:
                status_code = self.get_tree_item_meta(item_id, 'status_code', current_status_code or 'pending')
                if str(status_code) == 'metadata_refresh' or normalize_status_to_code(status_code) == 'metadata_refresh':
                    status_code = self._get_metadata_refresh_reset_queue_status_code()
                    status_text = status_code_to_localized(status_code)
                else:
                    status_text = current_status_text or status_code_to_localized(status_code)
                progress_text = current_progress_text or "-"

        denoise_val = normalize_denoise_level(
            self.get_tree_item_meta(item_id, 'denoise_enabled', 0)
        )
        if denoise_val == 0:
            denoise_val = display_to_denoise_level(
                current_values[self.COLUMN_INDEX['denoise']]
                if len(current_values) > self.COLUMN_INDEX['denoise']
                else ""
            )
        inferred_denoise_val = normalize_denoise_level(
            infer_denoise_level_from_filter_info(output_denoise_info)
        )
        if inferred_denoise_val > 0:
            denoise_val = inferred_denoise_val
        else:
            sidecar_denoise_val = normalize_denoise_level(result.get('output_denoise_sidecar_level'))
            if sidecar_denoise_val > 0:
                denoise_val = sidecar_denoise_val
        denoise_str = denoise_level_to_display(denoise_val)

        # Preset: read from the output's Settings metadata ("... - Preset N - ..."), fall
        # back to whatever was previously stored. Cleared when the output is gone (retry).
        output_preset = parse_preset_from_settings(output_settings_str)
        if reset_failed_for_retry:
            preset_display = "-"
            preset_for_meta = None
        else:
            preset_for_meta = output_preset if output_preset is not None else self.get_tree_item_meta(item_id, 'encoder_preset')
            preset_encoder = output_encoder_type or self.get_tree_item_meta(item_id, 'output_encoder_type')
            preset_display = format_preset_display(preset_encoder, preset_for_meta)

        self.tree.item(item_id, values=(
            denoise_str, hard_rotate_display, video_name_display,
            status_text, cq_str, vmaf_str, psnr_str, progress_text,
            orig_size_str, new_size_str, change_str,
            duration_str, frames_str, completed_date, preset_display
        ))
        self.tree.item(item_id, tags=(self._get_metadata_refresh_tag_for_status(status_text),))

        meta_update = {
            'status_code': status_code,
            'status_display': status_text,
            'progress_display': progress_text,
            'denoise_enabled': denoise_val,
            'manual_cq_range': manual_cq_range,
            'manual_cq_value': manual_cq_value,
            'manual_quality_check': manual_quality_check,
            'orig_size_display': orig_size_str,
            'new_size_display': new_size_str,
            'size_change_display': change_str,
            'duration_display': duration_str,
            'frames_display': frames_str,
            'completed_date': completed_date,
        }
        if source_size_bytes:
            meta_update['source_size_bytes'] = source_size_bytes
        if new_size_bytes:
            meta_update['output_size_bytes'] = new_size_bytes
        if output_encoder_type:
            meta_update['encoder_type'] = output_encoder_type
            meta_update['output_encoder_type'] = output_encoder_type
        if preset_for_meta not in (None, '', '-'):
            meta_update['encoder_preset'] = str(preset_for_meta)
        if output_duration_seconds:
            meta_update['output_duration_seconds'] = output_duration_seconds
        if output_frame_count:
            meta_update['output_frame_count'] = output_frame_count
        if source_duration_seconds:
            meta_update['source_duration_seconds'] = source_duration_seconds
        if source_frame_count:
            meta_update['source_frame_count'] = source_frame_count
        if source_fps:
            meta_update['source_fps'] = source_fps
        if output_duration_seconds and output_frame_count:
            try:
                meta_update['output_fps'] = output_frame_count / output_duration_seconds
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        if output_modified_timestamp:
            meta_update['output_modified_timestamp'] = output_modified_timestamp
        if output_cq_crf is not None:
            meta_update['cq'] = output_cq_crf
        if output_vmaf is not None:
            meta_update['vmaf'] = output_vmaf
        if output_psnr is not None:
            meta_update['psnr'] = output_psnr

        if reset_failed_for_retry:
            self.remove_tree_item_meta_keys(
                item_id,
                'output_size_bytes',
                'output_modified_timestamp',
                'output_extra_metadata',
                'output_frame_count',
                'output_duration_seconds',
                'output_fps',
                'encoder_type',
                'output_encoder_type',
                'encoder_preset',
                'cq',
                'vmaf',
                'psnr'
            )

        self.set_tree_item_meta(item_id, **meta_update)
        self.set_tree_item_meta(
            item_id,
            source_extra_metadata=source_extra_metadata,
            output_extra_metadata=output_extra_metadata
        )

        if hasattr(self, 'video_denoise_lock') and hasattr(self, 'video_denoise_enabled'):
            with self.video_denoise_lock:
                self.video_denoise_enabled[video_path] = denoise_val

        def _save_to_db():
            try:
                if hasattr(self, 'update_single_video_in_db'):
                    self.update_single_video_in_db(
                        video_path, item_id, status_text,
                        cq_str, vmaf_str, psnr_str,
                        orig_size_str, new_size_mb if has_output_file else None, change_percent if has_output_file else None,
                        completed_date,
                        denoise_enabled=denoise_val,
                        manual_cq_range=manual_cq_range,
                        manual_cq_value=manual_cq_value,
                        manual_quality_check=manual_quality_check,
                        source_extra_metadata=source_extra_metadata,
                        output_extra_metadata=output_extra_metadata,
                        clear_output_state=reset_failed_for_retry
                    )

                if has_output_file:
                    output_file_path = self.video_to_output.get(video_path)
                    if output_file_path and output_file_path.exists() and hasattr(self, 'schedule_track_editor_cache_build'):
                        self.schedule_track_editor_cache_build(video_path, output_file_path)
            except Exception as db_err:
                print(f"[ERROR] Metadata refresh DB save error: {db_err}")

        if hasattr(self, '_start_db_thread'):
            self._start_db_thread(_save_to_db, name="RefreshMetadataDB")
        else:
            threading.Thread(target=_save_to_db, daemon=True).start()

        if hasattr(self, 'update_summary_row'):
            try:
                self.update_summary_row()
            except Exception:
                pass

        self.log_status(f"[OK] Metaadat újraolvasás kész: {video_path.name}")
        self._write_main_log_line(
            f"[metadata_refresh] Completed | video={video_path.name} | status={status_text} | cq={cq_str} | vmaf={vmaf_str} | psnr={psnr_str} | denoise={denoise_val} | encoder={output_encoder_type or '-'} | size={new_size_str} | reset_for_retry={reset_failed_for_retry}"
        )

    def _poll_metadata_refresh_queue(self):
        """Main-thread poller that applies finished metadata refresh results."""
        def _restore_original_status(video_path, item_id, error_text=None):
            try:
                if self.tree.exists(item_id):
                    original_status = self.get_tree_item_meta(item_id, 'metadata_refresh_original_status')
                    if original_status:
                        current_values = list(self.tree.item(item_id, 'values'))
                        if len(current_values) > self.COLUMN_INDEX['status']:
                            current_values[self.COLUMN_INDEX['status']] = original_status
                            self.tree.item(item_id, values=tuple(current_values))
                            self.set_tree_item_meta(
                                item_id,
                                status_display=original_status,
                                status_code=normalize_status_to_code(original_status)
                            )
                            self.tree.item(item_id, tags=(self._get_metadata_refresh_tag_for_status(original_status),))
            except Exception:
                pass
            self.remove_tree_item_meta_keys(item_id, 'metadata_refresh_in_progress', 'metadata_refresh_original_status')
            if error_text is not None:
                self._write_main_log_line(
                    f"[metadata_refresh] Failed | video={Path(video_path).name} | error={error_text}"
                )

        try:
            while True:
                message_type, video_path, item_id, payload = self._metadata_refresh_queue.get_nowait()
                self._metadata_refresh_inflight.discard(str(item_id))

                if message_type == 'success':
                    try:
                        self._apply_refreshed_metadata_result(video_path, item_id, payload)
                        self.remove_tree_item_meta_keys(item_id, 'metadata_refresh_in_progress', 'metadata_refresh_original_status')
                    except Exception as apply_error:
                        self.log_status(f"[ERROR] Metaadat újraolvasás lezárási hiba: {Path(video_path).name}")
                        _restore_original_status(video_path, item_id, apply_error)
                else:
                    self.log_status(f"[ERROR] Metaadat újraolvasás sikertelen: {Path(video_path).name}")
                    _restore_original_status(video_path, item_id, payload)
        except queue.Empty:
            pass
        except Exception as e:
            print(f"[ERROR] Metadata refresh poller error: {e}")
        finally:
            try:
                self.root.after(100, self._poll_metadata_refresh_queue)
            except tk.TclError:
                pass

    def _refresh_metadata_from_file(self, video_path, item_id):
        """Start async metadata refresh without blocking the desktop GUI."""
        try:
            self._ensure_metadata_refresh_async_support()
            video_path = Path(video_path)

            output_file = self.video_to_output.get(video_path)
            if not output_file:
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                if output_file:
                    self.video_to_output[video_path] = output_file

            output_exists = bool(output_file and Path(output_file).exists())
            if output_exists and hasattr(self, '_is_metadata_probe_blocked_for_path') and self._is_metadata_probe_blocked_for_path(output_file):
                self.log_status(f"[INFO] Metaadat újraolvasás kihagyva, törlés/újrakódolás folyamatban: {video_path.name}")
                self._write_main_log_line(
                    f"[metadata_refresh] Skipped | video={video_path.name} | item_id={item_id} | reason=output_pending_delete"
                )
                return

            inflight_key = str(item_id)
            if inflight_key in self._metadata_refresh_inflight:
                self.log_status(f"[INFO] Metaadat újraolvasás már fut: {video_path.name}")
                self._write_main_log_line(
                    f"[metadata_refresh] Already running | video={video_path.name} | item_id={item_id}"
                )
                return

            self._metadata_refresh_inflight.add(inflight_key)
            self.log_status(f"[INFO] Metaadat újraolvasás indítva: {video_path.name}")
            self._write_main_log_line(
                f"[metadata_refresh] Started | video={video_path.name} | item_id={item_id} | output={Path(output_file) if output_file else '-'} | output_exists={output_exists} | async=true"
            )

            try:
                if hasattr(self, 'tree') and self.tree and self.tree.exists(item_id):
                    current_values = self.tree.item(item_id, 'values')
                    if current_values and len(current_values) > self.COLUMN_INDEX['status']:
                        current_values = list(current_values)
                        original_status = current_values[self.COLUMN_INDEX['status']]
                        if normalize_status_to_code(original_status) == 'metadata_refresh':
                            if output_exists:
                                stored_status_code = self.get_tree_item_meta(item_id, 'status_code')
                                if (
                                    stored_status_code and
                                    str(stored_status_code) != 'metadata_refresh' and
                                    normalize_status_to_code(stored_status_code) != 'metadata_refresh'
                                ):
                                    original_status = status_code_to_localized(stored_status_code)
                            else:
                                original_status = status_code_to_localized(self._get_metadata_refresh_reset_queue_status_code())
                        self.set_tree_item_meta(
                            item_id,
                            metadata_refresh_in_progress=True,
                            metadata_refresh_original_status=original_status
                        )
                        current_values[self.COLUMN_INDEX['status']] = t('status_metadata_refresh')
                        self.tree.item(item_id, values=tuple(current_values))
            except Exception:
                pass

            if not hasattr(self, '_metadata_refresh_semaphore'):
                self._metadata_refresh_semaphore = threading.Semaphore(2)

            def _run_with_semaphore():
                self._metadata_refresh_semaphore.acquire()
                try:
                    self._refresh_metadata_probe_worker(video_path, item_id, Path(output_file) if output_exists else None)
                finally:
                    self._metadata_refresh_semaphore.release()

            threading.Thread(
                target=_run_with_semaphore,
                daemon=True,
                name="RefreshMetadataProbe"
            ).start()

        except Exception as e:
            print(f"[ERROR] _refresh_metadata_from_file failed for {video_path}: {e}")
            try:
                self._metadata_refresh_inflight.discard(str(item_id))
                if hasattr(self, 'tree') and self.tree and self.tree.exists(item_id):
                    original_status = self.get_tree_item_meta(item_id, 'metadata_refresh_original_status')
                    if original_status:
                        current_values = list(self.tree.item(item_id, 'values'))
                        if len(current_values) > self.COLUMN_INDEX['status']:
                            current_values[self.COLUMN_INDEX['status']] = original_status
                            self.tree.item(item_id, values=tuple(current_values))
                            self.set_tree_item_meta(
                                item_id,
                                status_display=original_status,
                                status_code=normalize_status_to_code(original_status)
                            )
                            self.tree.item(item_id, tags=(self._get_metadata_refresh_tag_for_status(original_status),))
                self.remove_tree_item_meta_keys(item_id, 'metadata_refresh_in_progress', 'metadata_refresh_original_status')
            except Exception:
                pass
            self.log_status(f"[ERROR] Metaadat újraolvasás sikertelen: {Path(video_path).name}")
            self._write_main_log_line(
                f"[metadata_refresh] Failed | video={Path(video_path).name} | error={e}"
            )
            import traceback
            traceback.print_exc()

    def _refresh_metadata_from_file_multiple(self, selected_video_items):
        """Re-read metadata for multiple selected videos."""
        self._write_main_log_line(
            f"[metadata_refresh] Batch started | count={len(selected_video_items)}"
        )
        for selected_item in selected_video_items:
            try:
                if isinstance(selected_item, (tuple, list)) and len(selected_item) == 2:
                    video_path, item_id = selected_item
                else:
                    item_id = selected_item
                    video_path = self._get_video_path_by_item(item_id)
                    if video_path is None:
                        raise ValueError(f"Video path not found for item_id={item_id}")
                self._refresh_metadata_from_file(video_path, item_id)
            except Exception as e:
                print(f"[ERROR] Metadata refresh failed for {selected_item}: {e}")
                self._write_main_log_line(
                    f"[metadata_refresh] Batch item failed | item={selected_item} | error={e}"
                )
        self._write_main_log_line(
            f"[metadata_refresh] Batch completed | count={len(selected_video_items)}"
        )
