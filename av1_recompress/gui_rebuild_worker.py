from .gui_imports import *
from .gui_shared import is_app_closing


class RebuildWorkerMixin:
    def _init_rebuild_state(self):
        self.rebuild_queue = []
        self.rebuild_queue_lock = threading.Lock()
        self.rebuild_stop_event = threading.Event()
        self.rebuild_worker_thread = None
        self.rebuild_running = False
        self._rebuild_ffmpeg = ''
        self._rebuild_ffprobe = ''

    def _refresh_rebuild_snapshot(self):
        self._rebuild_ffmpeg = self.ffmpeg_path.get().strip() if hasattr(self, 'ffmpeg_path') else ''
        try:
            from .core_preamble_and_imports import FFPROBE_PATH
            self._rebuild_ffprobe = str(FFPROBE_PATH) if FFPROBE_PATH else ''
        except Exception:
            self._rebuild_ffprobe = ''

    def schedule_rebuild(self, video_paths_with_items):
        self._refresh_rebuild_snapshot()
        for video_path, item_id in video_paths_with_items:
            with self.rebuild_queue_lock:
                already_queued = any(vp == video_path for vp, _ in self.rebuild_queue)
            if already_queued:
                continue
            orig_values = self.tree.item(item_id, 'values')
            ci = self.COLUMN_INDEX

            def _cv(key, default="-"):
                idx = ci.get(key, -1)
                return orig_values[idx] if idx >= 0 and len(orig_values) > idx else default

            orig_size_str = _cv('orig_size', "-")
            completed_date = _cv('completed_date', "")
            new_size_str = _cv('new_size', "-")
            cq_str = _cv('cq', "-")
            vmaf_str = _cv('vmaf', "-")
            psnr_str = _cv('psnr', "-")
            progress_str = _cv('progress', "-")
            change_str = _cv('change', "-")
            previous_status = _cv('status', "")
            if hasattr(self, 'set_tree_item_meta'):
                self.set_tree_item_meta(item_id, rebuild_previous_status_display=previous_status)

            with self.rebuild_queue_lock:
                self.rebuild_queue.append((video_path, item_id))

            rebuild_status = t('status_rebuild_queued')
            self.encoding_queue.put_nowait(("update", item_id, rebuild_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "pending"))
            self._save_rebuild_state_to_db(item_id, 'rebuild_queued', previous_status=previous_status)

        with self.rebuild_queue_lock:
            if self.rebuild_queue and not self.rebuild_running:
                self._start_rebuild_worker()
        self.root.after(100, self.check_encoding_queue)

    def _start_rebuild_worker(self):
        self.rebuild_stop_event.clear()
        self.rebuild_running = True
        self.rebuild_worker_thread = threading.Thread(target=self._rebuild_worker, daemon=True)
        self.rebuild_worker_thread.start()

    def _stop_rebuild_worker(self):
        self.rebuild_stop_event.set()

    def _rebuild_worker(self):
        try:
            while True:
                if is_app_closing():
                    break
                if self.rebuild_stop_event.is_set():
                    break
                with self.rebuild_queue_lock:
                    if not self.rebuild_queue:
                        break
                    video_path, item_id = self.rebuild_queue.pop(0)
                self._process_rebuild_single(video_path, item_id)
        finally:
            self.rebuild_running = False
            with self.rebuild_queue_lock:
                if not self.rebuild_queue:
                    self.rebuild_stop_event.clear()

    def _process_rebuild_single(self, video_path, item_id):
        from .i18n import is_status_completed, is_status_needs_check, is_status_rebuild
        temp_dir = None
        input_file = None
        try:
            output_file = self.video_to_output.get(video_path)
            if output_file is None:
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
            if not output_file or not output_file.exists():
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] Rebuild: kimeneti fájl nem található: {video_path.name}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                with self.rebuild_queue_lock:
                    self.rebuild_queue = [(vp, iid) for vp, iid in self.rebuild_queue if vp != video_path]
                return

            if hasattr(self, 'svt_processing_videos') and video_path in getattr(self, 'svt_processing_videos', set()):
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] Rebuild skipped: {video_path.name} is currently being encoded\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                return

            orig_values = self.tree.item(item_id, 'values')
            ci = self.COLUMN_INDEX

            def _cv(key, default="-"):
                idx = ci.get(key, -1)
                return orig_values[idx] if idx >= 0 and len(orig_values) > idx else default

            previous_status = _cv('status', "")
            orig_size_str = _cv('orig_size', "-")
            cq_str = _cv('cq', "-")
            vmaf_str = _cv('vmaf', "-")
            psnr_str = _cv('psnr', "-")
            progress_str = _cv('progress', "-")
            new_size_str = _cv('new_size', "-")
            change_str = _cv('change', "-")
            completed_date = _cv('completed_date', "")
            if is_status_rebuild(previous_status) and hasattr(self, 'get_tree_item_meta'):
                previous_status = self.get_tree_item_meta(item_id, 'rebuild_previous_status_display') or previous_status

            def _is_restorable_final_status(status_text):
                return is_status_completed(status_text) or is_status_needs_check(status_text)

            def _tag_for_restored_status(status_text):
                if hasattr(self, '_get_metadata_refresh_tag_for_status'):
                    try:
                        return self._get_metadata_refresh_tag_for_status(status_text)
                    except Exception:
                        pass
                if is_status_completed(status_text):
                    return "completed"
                if is_status_needs_check(status_text):
                    return "needs_check"
                return "pending"

            rebuild_in_progress_status = t('status_rebuild_in_progress')
            self.encoding_queue.put_nowait(("update", item_id, rebuild_in_progress_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
            self.encoding_queue.put_nowait(("tag", item_id, "encoding_svt"))
            self._save_rebuild_state_to_db(item_id, 'rebuild_in_progress', previous_status=previous_status)
            self.root.after(50, self.check_encoding_queue)

            if LOG_WRITER:
                try:
                    LOG_WRITER.write(t('log_rebuild_start').format(name=output_file.name) + "\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass

            input_file = Path(output_file)
            original_size = input_file.stat().st_size if input_file.exists() else 0

            backup_path = input_file.with_suffix('.original.mkv')
            if backup_path.exists():
                try:
                    backup_path.unlink()
                except OSError:
                    pass

            temp_dir = Path(tempfile.mkdtemp(prefix="av1_rebuild_"))

            success = self._do_rebuild(input_file, temp_dir, backup_path)

            was_stopped = self.rebuild_stop_event.is_set()

            if success:
                rebuilt_file = input_file
                new_size = rebuilt_file.stat().st_size if rebuilt_file.exists() else original_size
                new_size_mb = new_size / (1024 ** 2)
                new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                source_size_bytes = None
                if hasattr(self, 'get_tree_item_meta'):
                    source_size_bytes = self.get_tree_item_meta(item_id, 'source_size_bytes')
                if source_size_bytes is None:
                    try:
                        source_size_bytes = parse_size_to_bytes(orig_size_str)
                    except Exception:
                        source_size_bytes = None
                if source_size_bytes and source_size_bytes > 0:
                    change_pct = ((new_size - source_size_bytes) / source_size_bytes) * 100
                    change_str = f"{format_localized_number(change_pct, decimals=2, show_sign=True)}%"
                else:
                    change_pct = 0.0
                    change_str = "-"

                restore_status = previous_status if _is_restorable_final_status(previous_status) else t('status_completed')
                self.encoding_queue.put_nowait(("update", item_id, restore_status, cq_str, vmaf_str, psnr_str, "100%", orig_size_str, new_size_str, change_str, completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, _tag_for_restored_status(restore_status)))
                self._clear_rebuild_state_from_db(item_id)
                if hasattr(self, 'remove_tree_item_meta_keys'):
                    self.remove_tree_item_meta_keys(item_id, 'rebuild_previous_status_display')

                old_size_mb = original_size / (1024 ** 2) if original_size else 0
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(t('log_rebuild_done').format(name=output_file.name, old_size=old_size_mb, new_size=new_size_mb) + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                if hasattr(self, 'log_status'):
                    try:
                        self.log_status(f"[OK] Rebuild: {output_file.name} ({format_localized_number(old_size_mb, decimals=1)} MB -> {format_localized_number(new_size_mb, decimals=1)} MB)")
                    except Exception:
                        pass

                if hasattr(self, '_refresh_metadata_from_file'):
                    try:
                        current_frames = None
                        if self.tree.exists(item_id):
                            try:
                                cv = self.tree.item(item_id, 'values')
                                fi = self.COLUMN_INDEX.get('frames', -1)
                                if fi >= 0 and len(cv) > fi:
                                    current_frames = cv[fi]
                            except Exception:
                                pass
                        self._refresh_metadata_from_file(video_path, item_id)
                    except Exception:
                        pass

                def _restore_rebuild_status(prev=restore_status, iid=item_id, saved_frames=current_frames):
                    try:
                        if self.tree.exists(iid):
                            vals = list(self.tree.item(iid, 'values'))
                            if len(vals) > self.COLUMN_INDEX['status']:
                                vals[self.COLUMN_INDEX['status']] = prev
                            if saved_frames is not None:
                                fi = self.COLUMN_INDEX.get('frames', -1)
                                if fi >= 0 and len(vals) > fi:
                                    vals[fi] = saved_frames
                            self.tree.item(iid, values=vals)
                    except Exception:
                        pass
                self.root.after(8000, _restore_rebuild_status)

                def _update_rebuilt_db():
                    try:
                        new_size_bytes = rebuilt_file.stat().st_size if rebuilt_file.exists() else 0
                        new_size_mb_for_db = new_size_bytes / (1024 ** 2) if new_size_bytes else None
                        self.update_single_video_in_db(
                            video_path, item_id, restore_status,
                            cq_str, vmaf_str, psnr_str,
                            orig_size_str, new_size_mb_for_db,
                            change_pct,
                            completed_date
                        )
                    except (OSError, IOError, sqlite3.Error, AttributeError, ValueError):
                        pass
                self._start_db_thread(_update_rebuilt_db, name="RebuildCompletionDB", daemon=True)

            elif was_stopped:
                self._restore_original_from_backup(output_file)
                rollback_status = t('status_rebuild_queued')
                self.encoding_queue.put_nowait(("update", item_id, rollback_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                self._save_rebuild_state_to_db(item_id, 'rebuild_queued', previous_status=previous_status)
                with self.rebuild_queue_lock:
                    already = any(vp == video_path for vp, _ in self.rebuild_queue)
                    if not already:
                        self.rebuild_queue.insert(0, (video_path, item_id))
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(t('log_rebuild_stopped') + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            else:
                self._restore_original_from_backup(output_file)
                rollback_status = previous_status if _is_restorable_final_status(previous_status) else t('status_completed')
                self.encoding_queue.put_nowait(("update", item_id, rollback_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, _tag_for_restored_status(rollback_status)))
                self._clear_rebuild_state_from_db(item_id)
                if hasattr(self, 'remove_tree_item_meta_keys'):
                    self.remove_tree_item_meta_keys(item_id, 'rebuild_previous_status_display')

        except Exception as e:
            if input_file:
                self._restore_original_from_backup(input_file)
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[ERROR] Rebuild worker váratlan hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            if hasattr(self, 'log_status'):
                try:
                    self.log_status(f"[ERROR] Rebuild váratlan hiba: {e}")
                except Exception:
                    pass
        finally:
            if temp_dir and temp_dir.exists():
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception:
                    pass

    def _rebuild_log(self, msg):
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"[REBUILD] {msg}\n")
                LOG_WRITER.flush()
            except Exception:
                pass
        if hasattr(self, 'log_status'):
            try:
                self.log_status(msg)
            except Exception:
                pass

    @staticmethod
    def _format_disposition_value(is_default, is_forced):
        if is_default and is_forced:
            return 'default+forced'
        if is_default:
            return 'default'
        if is_forced:
            return 'forced'
        return '0'

    @staticmethod
    def _generate_attachment_filename(index, mimetype):
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
        ext = mime_to_ext.get(mimetype, 'dat')
        return f"attachment_{index}.{ext}"

    def _do_rebuild(self, input_file, temp_dir, backup_path):
        ffmpeg = self._rebuild_ffmpeg
        ffprobe = self._rebuild_ffprobe
        stop_evt = self.rebuild_stop_event

        if not ffmpeg or not os.path.isfile(ffmpeg):
            self._rebuild_log(f"[ERROR] FFmpeg útvonal érvénytelen: '{ffmpeg}'")
            return False

        self._rebuild_log(f"Videó stream kicsomagolása: {input_file.name}")
        video_only = temp_dir / f"{input_file.stem}_video.mkv"
        cmd = [ffmpeg, '-y', '-i', str(input_file), '-map', '0:v:0', '-c:v', 'copy', str(video_only)]
        if not self._run_ffmpeg(cmd, stop_evt):
            self._rebuild_log(f"[ERROR] Videó kicsomagolás sikertelen: {input_file.name}")
            return False
        if stop_evt.is_set():
            return False

        audio_tracks = self._extract_audio_tracks(input_file, temp_dir, ffmpeg, ffprobe)
        if stop_evt.is_set():
            return False

        subtitle_tracks = self._extract_subtitle_tracks(input_file, temp_dir, ffmpeg, ffprobe)
        if stop_evt.is_set():
            return False

        self._rebuild_log(f"Újraépítés: {len(audio_tracks)} audio, {len(subtitle_tracks)} felirat -> {input_file.name}")
        rebuilt = temp_dir / f"{input_file.stem}_rebuilt.mkv"
        if not self._assemble_mkv(video_only, audio_tracks, subtitle_tracks, input_file, rebuilt, ffmpeg, ffprobe):
            self._rebuild_log(f"[ERROR] Újraépítés sikertelen: {input_file.name}")
            return False
        if stop_evt.is_set():
            return False
        if not rebuilt.exists() or rebuilt.stat().st_size == 0:
            self._rebuild_log(f"[ERROR] Újraépített fájl üres vagy hiányzik: {input_file.name}")
            return False

        try:
            shutil.move(str(input_file), str(backup_path))
            shutil.move(str(rebuilt), str(input_file))
            if backup_path.exists():
                try:
                    backup_path.unlink()
                except OSError:
                    pass
        except (OSError, IOError) as e:
            self._rebuild_log(f"[ERROR] Fájlcsere hiba: {e}")
            if backup_path.exists() and not input_file.exists():
                try:
                    shutil.move(str(backup_path), str(input_file))
                except Exception:
                    pass
            return False

        return True

    def _run_ffmpeg(self, cmd, stop_evt, timeout=1800):
        try:
            proc = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0)
            try:
                while proc.poll() is None:
                    if stop_evt.is_set():
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=3)
                        return False
                    try:
                        proc.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        continue
                return proc.returncode == 0
            except Exception:
                try:
                    proc.kill()
                except OSError:
                    pass
                return False
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[ERROR] FFmpeg indítás hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            return False

    def _extract_audio_tracks(self, input_file, temp_dir, ffmpeg, ffprobe):
        tracks = []
        try:
            probe_cmd = [
                ffprobe, '-v', 'error',
                '-select_streams', 'a',
                '-show_entries', 'stream=index,codec_name,channels,channel_layout,start_time',
                '-show_entries', 'stream_tags=language,title',
                '-show_entries', 'stream_disposition=default,forced',
                '-of', 'json',
                str(input_file)
            ]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=60,
                                    creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0)
            data = json.loads(result.stdout)
            for audio_idx, stream in enumerate(data.get('streams', [])):
                tags = stream.get('tags', {})
                disposition = stream.get('disposition', {})
                out_path = temp_dir / f"{input_file.stem}_audio_{audio_idx}.mka"
                cmd = [ffmpeg, '-y', '-i', str(input_file), '-map', f'0:a:{audio_idx}', '-c:a', 'copy', str(out_path)]
                if self._run_ffmpeg(cmd, self.rebuild_stop_event, timeout=600) and out_path.exists():
                    tracks.append({
                        'path': out_path,
                        'index': audio_idx,
                        'language': tags.get('language', '') or 'und',
                        'title': tags.get('title', ''),
                        'default': disposition.get('default', 0) == 1,
                        'forced': disposition.get('forced', 0) == 1,
                        'codec': stream.get('codec_name', 'unknown'),
                        'channels': stream.get('channels', 0),
                        'channel_layout': stream.get('channel_layout', ''),
                        'start_time': stream.get('start_time'),
                    })
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Audio extraction hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
        return tracks

    def _extract_subtitle_tracks(self, input_file, temp_dir, ffmpeg, ffprobe):
        tracks = []
        try:
            probe_cmd = [
                ffprobe, '-v', 'error',
                '-select_streams', 's',
                '-show_entries', 'stream=index,codec_name,start_time',
                '-show_entries', 'stream_tags=language,title',
                '-show_entries', 'stream_disposition=default,forced',
                '-of', 'json',
                str(input_file)
            ]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=60,
                                    creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0)
            data = json.loads(result.stdout)
            for sub_idx, stream in enumerate(data.get('streams', [])):
                tags = stream.get('tags', {})
                disposition = stream.get('disposition', {})
                track_info = {
                    'index': sub_idx,
                    'language': tags.get('language', '') or 'und',
                    'title': tags.get('title', ''),
                    'default': disposition.get('default', 0) == 1,
                    'forced': disposition.get('forced', 0) == 1,
                    'start_time': stream.get('start_time'),
                    'source_fallback': False,
                }
                out_path = temp_dir / f"{input_file.stem}_sub_{sub_idx}.mks"
                track_info['path'] = out_path
                cmd = [ffmpeg, '-y', '-i', str(input_file), '-map', f'0:s:{sub_idx}', '-c:s', 'copy', '-f', 'matroska', str(out_path)]
                if self._run_ffmpeg(cmd, self.rebuild_stop_event, timeout=300) and out_path.exists() and out_path.stat().st_size > 0:
                    tracks.append(track_info)
                else:
                    track_info['path'] = None
                    track_info['source_fallback'] = True
                    tracks.append(track_info)
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Subtitle extraction hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
        return tracks

    def _get_video_metadata(self, source_path, ffprobe):
        default_metadata = {
            'sample_aspect_ratio': None,
            'display_aspect_ratio': None,
            'rotation': 0,
            'color_space': None,
            'color_primaries': None,
            'color_transfer': None,
            'color_range': None,
            'video_language': None,
            'video_title': None,
            'video_default': False,
            'video_forced': False,
            'video_start_time': None,
            'audio_start_times': [],
            'subtitle_start_times': [],
            'audio_delay_ms': 0,
        }
        try:
            metadata_cmd = [
                ffprobe, '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries',
                'stream=sample_aspect_ratio,display_aspect_ratio,color_space,color_primaries,color_transfer,color_range'
                ':stream_tags=rotate,language,title:stream_disposition=default,forced:stream_side_data=rotation',
                '-of', 'json',
                str(source_path)
            ]
            metadata_result = subprocess.run(
                metadata_cmd, capture_output=True, text=True, encoding='utf-8',
                errors='replace', timeout=60,
                creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0
            )
            if metadata_result.returncode == 0 and metadata_result.stdout.strip():
                data = json.loads(metadata_result.stdout)
                streams = data.get('streams', [])
                if streams:
                    stream = streams[0]
                    for key in ('sample_aspect_ratio', 'display_aspect_ratio'):
                        value = stream.get(key)
                        if value and str(value).lower() not in ('n/a', 'unknown'):
                            default_metadata[key] = value
                    for key in ('color_space', 'color_primaries', 'color_transfer', 'color_range'):
                        value = stream.get(key)
                        if value and str(value).lower() not in ('n/a', 'unknown'):
                            default_metadata[key] = value
                    rotation = 0
                    for side_data in stream.get('side_data_list', []):
                        if side_data.get('rotation') is not None:
                            try:
                                rotation = int(round(float(side_data['rotation'])))
                                break
                            except (ValueError, TypeError):
                                pass
                    if not rotation:
                        tags = stream.get('tags', {}) or {}
                        for tag_key in ('rotate', 'ROTATE', 'Rotate'):
                            if tag_key in tags:
                                try:
                                    rotation = int(round(float(tags[tag_key])))
                                    break
                                except (ValueError, TypeError):
                                    pass
                    default_metadata['rotation'] = rotation
                    tags = stream.get('tags', {}) or {}
                    disposition = stream.get('disposition', {}) or {}
                    if tags.get('language'):
                        default_metadata['video_language'] = tags.get('language')
                    if tags.get('title'):
                        default_metadata['video_title'] = tags.get('title')
                    default_metadata['video_default'] = disposition.get('default', 0) == 1
                    default_metadata['video_forced'] = disposition.get('forced', 0) == 1
        except Exception as e:
            self._rebuild_log(f"[WARN] Video metadata query error: {e}")

        try:
            delay_cmd = [
                ffprobe, '-v', 'error',
                '-show_entries', 'stream=codec_type,start_time',
                '-of', 'default=noprint_wrappers=1',
                str(source_path)
            ]
            delay_result = subprocess.run(
                delay_cmd, capture_output=True, text=True, encoding='utf-8',
                errors='replace', timeout=60,
                creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0
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
                default_metadata['video_start_time'] = video_start if video_start is not None else 0.0
                default_metadata['audio_start_times'] = audio_starts
                default_metadata['subtitle_start_times'] = subtitle_starts
                if video_start is not None and audio_starts:
                    delay_sec = audio_starts[0] - video_start
                    default_metadata['audio_delay_ms'] = int(round(delay_sec * 1000))
        except Exception as e:
            self._rebuild_log(f"[WARN] Stream timing query error: {e}")

        return default_metadata

    def _get_attachment_info(self, source_path, ffprobe):
        try:
            cmd = [
                ffprobe, '-v', 'error',
                '-select_streams', 't',
                '-show_entries', 'stream=index',
                '-show_entries', 'stream_tags=filename,mimetype',
                '-of', 'json',
                str(source_path)
            ]
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding='utf-8',
                errors='replace', timeout=600,
                creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0
            )
            if result.returncode != 0:
                return []
            data = json.loads(result.stdout)
            streams = data.get('streams', [])
            attachment_info = []
            for idx, stream in enumerate(streams):
                tags = stream.get('tags', {})
                stream_index = stream.get('index', idx)
                attachment_info.append({
                    'index': stream_index,
                    'filename': tags.get('filename'),
                    'mimetype': tags.get('mimetype', 'application/octet-stream'),
                })
            return attachment_info
        except Exception as e:
            self._rebuild_log(f"[WARN] Attachment info query error: {e}")
            return []

    def _assemble_mkv(self, video_path, audio_tracks, subtitle_tracks, source_path, output_path, ffmpeg, ffprobe):
        video_metadata = self._get_video_metadata(source_path, ffprobe)

        cmd = [ffmpeg, '-y']

        video_start_time = video_metadata.get('video_start_time', 0.0) or 0.0
        if video_start_time not in (0, 0.0, None):
            cmd.extend(['-itsoffset', str(video_start_time)])
        cmd.extend(['-i', str(video_path)])

        audio_start_times = video_metadata.get('audio_start_times', [])
        for track_idx, track in enumerate(audio_tracks):
            start_time = track.get('start_time')
            try:
                start_time_float = float(start_time)
            except (ValueError, TypeError):
                fallback_start = audio_start_times[track_idx] if track_idx < len(audio_start_times) else 0.0
                start_time_float = float(fallback_start or 0.0)
            if start_time_float not in (0.0, -0.0):
                cmd.extend(['-itsoffset', str(start_time_float)])
            cmd.extend(['-i', str(track['path'])])

        subtitle_start_times = video_metadata.get('subtitle_start_times', [])
        for track_idx, track in enumerate(subtitle_tracks):
            if track.get('source_fallback'):
                continue
            if not track.get('path') or not Path(track['path']).exists():
                track['source_fallback'] = True
                continue
            start_time = track.get('start_time')
            try:
                start_time_float = float(start_time)
            except (ValueError, TypeError):
                fallback_start = subtitle_start_times[track_idx] if track_idx < len(subtitle_start_times) else 0.0
                start_time_float = float(fallback_start or 0.0)
            if start_time_float not in (0.0, -0.0):
                cmd.extend(['-itsoffset', str(start_time_float)])
            cmd.extend(['-i', str(track['path'])])

        extracted_subtitle_indices = [
            idx for idx, track in enumerate(subtitle_tracks)
            if not track.get('source_fallback') and track.get('path') and Path(track['path']).exists()
        ]
        subtitle_input_map = {
            track_idx: 1 + len(audio_tracks) + order_idx
            for order_idx, track_idx in enumerate(extracted_subtitle_indices)
        }
        source_input_idx = 1 + len(audio_tracks) + len(extracted_subtitle_indices)
        cmd.extend(['-i', str(source_path)])

        cmd.extend(['-map', '0:v:0'])

        for i in range(len(audio_tracks)):
            cmd.extend(['-map', f'{i + 1}:a:0'])

        mapped_subtitle_tracks = []
        for track_idx, track in enumerate(subtitle_tracks):
            if track_idx in subtitle_input_map:
                cmd.extend(['-map', f'{subtitle_input_map[track_idx]}:s:0'])
            else:
                cmd.extend(['-map', f'{source_input_idx}:s:{track_idx}'])
            mapped_subtitle_tracks.append(track_idx)

        try:
            attachment_info = self._get_attachment_info(source_path, ffprobe)
            if attachment_info:
                for att in attachment_info:
                    cmd.extend(['-map', f'{source_input_idx}:{att["index"]}'])
                for output_idx, att in enumerate(attachment_info):
                    filename = att['filename']
                    if not filename:
                        filename = self._generate_attachment_filename(output_idx, att['mimetype'])
                    cmd.extend([f'-metadata:s:t:{output_idx}', f'filename={filename}'])
                    if att['mimetype']:
                        cmd.extend([f'-metadata:s:t:{output_idx}', f'mimetype={att["mimetype"]}'])
            else:
                cmd.extend(['-map', f'{source_input_idx}:t?'])
        except Exception:
            cmd.extend(['-map', f'{source_input_idx}:t?'])

        cmd.extend(['-map_chapters', str(source_input_idx)])
        cmd.extend(['-map_metadata', str(source_input_idx)])
        cmd.extend(['-c', 'copy', '-copy_unknown'])

        dar = video_metadata.get('display_aspect_ratio')
        if dar and str(dar).lower() not in ('n/a', 'unknown'):
            cmd.extend(['-aspect:v:0', str(dar)])
        rotation = video_metadata.get('rotation', 0) or 0
        if rotation:
            cmd.extend(['-metadata:s:v:0', f'rotate={rotation}'])
        for opt, key in [
            ('-colorspace:v:0', 'color_space'),
            ('-color_primaries:v:0', 'color_primaries'),
            ('-color_trc:v:0', 'color_transfer'),
            ('-color_range:v:0', 'color_range'),
        ]:
            val = video_metadata.get(key)
            if val:
                cmd.extend([opt, str(val)])
        if video_metadata.get('video_language'):
            cmd.extend(['-metadata:s:v:0', f'language={video_metadata["video_language"]}'])
        if video_metadata.get('video_title'):
            cmd.extend(['-metadata:s:v:0', f'title={video_metadata["video_title"]}'])
        cmd.extend([
            '-disposition:v:0',
            self._format_disposition_value(video_metadata.get('video_default'), video_metadata.get('video_forced'))
        ])

        for output_idx, track in enumerate(audio_tracks):
            if track.get('language'):
                cmd.extend([f'-metadata:s:a:{output_idx}', f'language={track["language"]}'])
            if track.get('title'):
                cmd.extend([f'-metadata:s:a:{output_idx}', f'title={track["title"]}'])
            cmd.extend([
                f'-disposition:a:{output_idx}',
                self._format_disposition_value(track.get('default'), track.get('forced'))
            ])

        for output_sub_idx, track_idx in enumerate(mapped_subtitle_tracks):
            track = subtitle_tracks[track_idx]
            if track.get('language'):
                cmd.extend([f'-metadata:s:s:{output_sub_idx}', f'language={track["language"]}'])
            if track.get('title'):
                cmd.extend([f'-metadata:s:s:{output_sub_idx}', f'title={track["title"]}'])
            cmd.extend([
                f'-disposition:s:{output_sub_idx}',
                self._format_disposition_value(track.get('default'), track.get('forced'))
            ])

        cmd.append(str(output_path))
        return self._run_ffmpeg(cmd, self.rebuild_stop_event, timeout=1800)

    def _restore_original_from_backup(self, output_file):
        output_path = Path(output_file)
        backup_path = output_path.with_suffix('.original.mkv')
        new_path = output_path.parent / f"{output_path.stem}_new.mkv"

        for p in [new_path]:
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass

        if backup_path.exists():
            try:
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except OSError:
                        pass
                shutil.move(str(backup_path), str(output_path))
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(t('log_rebuild_recovery').format(name=output_path.name) + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            except (OSError, IOError) as e:
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[ERROR] Backup visszaállítás sikertelen: {e}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

    def _save_rebuild_state_to_db(self, item_id, rebuild_state, previous_status=""):
        if not hasattr(self, 'db_path') or not self.db_path:
            return
        try:
            video_path = None
            for vp, iid in self.video_items.items():
                if iid == item_id:
                    video_path = vp
                    break
            if not video_path:
                return

            def _do_save():
                with self.db_lock:
                    conn = sqlite3.connect(str(self.db_path))
                    try:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO rebuild_state 
                            (video_path, item_id, rebuild_state, previous_status, updated_at)
                            VALUES (?, ?, ?, ?, datetime('now'))
                        ''', (str(video_path), item_id, rebuild_state, previous_status))
                        conn.commit()
                    finally:
                        conn.close()
            self._start_db_thread(_do_save, name="RebuildStateSave", daemon=True)
        except Exception:
            pass

    def _clear_rebuild_state_from_db(self, item_id):
        if not hasattr(self, 'db_path') or not self.db_path:
            return
        try:
            video_path = None
            for vp, iid in self.video_items.items():
                if iid == item_id:
                    video_path = vp
                    break
            if not video_path:
                return

            def _do_clear():
                with self.db_lock:
                    conn = sqlite3.connect(str(self.db_path))
                    try:
                        cursor = conn.cursor()
                        cursor.execute('DELETE FROM rebuild_state WHERE video_path = ?', (str(video_path),))
                        conn.commit()
                    finally:
                        conn.close()
            self._start_db_thread(_do_clear, name="RebuildStateClear", daemon=True)
        except Exception:
            pass

    def _ensure_rebuild_state_table(self):
        try:
            with self.db_lock:
                conn = sqlite3.connect(str(self.db_path))
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS rebuild_state (
                            video_path TEXT PRIMARY KEY,
                            item_id TEXT,
                            rebuild_state TEXT,
                            previous_status TEXT,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    conn.commit()
                finally:
                    conn.close()
        except Exception:
            pass

    def _recover_interrupted_rebuilds(self):
        self._ensure_rebuild_state_table()
        try:
            with self.db_lock:
                conn = sqlite3.connect(str(self.db_path))
                conn.row_factory = sqlite3.Row
                try:
                    cursor = conn.cursor()
                    cursor.execute('SELECT video_path, item_id, rebuild_state, previous_status FROM rebuild_state')
                    rows = cursor.fetchall()
                finally:
                    conn.close()

            if not rows:
                return

            recovered = []
            for row in rows:
                video_path = Path(row['video_path'])
                item_id = row['item_id']
                rebuild_state = row['rebuild_state']
                previous_status = row['previous_status'] or ""

                if not video_path.exists():
                    self._clear_rebuild_state_from_db(item_id)
                    continue

                output_file = self.video_to_output.get(video_path)
                if output_file is None:
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                if not output_file:
                    continue

                if rebuild_state == 'rebuild_in_progress':
                    backup_path = output_file.with_suffix('.original.mkv')
                    new_path = output_file.parent / f"{output_file.stem}_new.mkv"

                    for p in [new_path]:
                        if p.exists():
                            try:
                                p.unlink()
                            except OSError:
                                pass

                    if backup_path.exists():
                        try:
                            if output_file.exists():
                                try:
                                    output_file.unlink()
                                except OSError:
                                    pass
                            shutil.move(str(backup_path), str(output_file))
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(t('log_rebuild_recovery').format(name=output_file.name) + "\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except (OSError, IOError):
                            pass

                    temp_files = list(output_file.parent.glob("av1_rebuild_*"))
                    for tf in temp_files:
                        if tf.is_dir():
                            try:
                                shutil.rmtree(tf, ignore_errors=True)
                            except OSError:
                                pass

                orig_values = self.tree.item(item_id, 'values') if self.tree.exists(item_id) else None
                if orig_values is None:
                    self._clear_rebuild_state_from_db(item_id)
                    continue

                rebuild_queued_status = t('status_rebuild_queued')
                ci = self.COLUMN_INDEX

                def _cv(key, default="-"):
                    idx = ci.get(key, -1)
                    return orig_values[idx] if idx >= 0 and len(orig_values) > idx else default

                orig_size_str = _cv('orig_size', "-")
                cq_str = _cv('cq', "-")
                vmaf_str = _cv('vmaf', "-")
                psnr_str = _cv('psnr', "-")
                progress_str = _cv('progress', "-")
                new_size_str = _cv('new_size', "-")
                change_str = _cv('change', "-")
                completed_date = _cv('completed_date', "")

                self.encoding_queue.put_nowait(("update", item_id, rebuild_queued_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
                self.encoding_queue.put_nowait(("tag", item_id, "pending"))
                self._save_rebuild_state_to_db(item_id, 'rebuild_queued', previous_status=previous_status)
                recovered.append((video_path, item_id))

            if recovered:
                with self.rebuild_queue_lock:
                    for vp, iid in recovered:
                        already = any(v == vp for v, _ in self.rebuild_queue)
                        if not already:
                            self.rebuild_queue.append((vp, iid))
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[INFO] Rebuild crash recovery: {len(recovered)} fájl visszaállítva.\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                self._start_rebuild_worker()
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Rebuild crash recovery hiba: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass

    def _delayed_rebuild_recovery(self):
        if not hasattr(self, 'tree') or self.tree is None:
            if not is_app_closing():
                try:
                    if self.root.winfo_exists():
                        self.root.after(500, self._delayed_rebuild_recovery)
                except tk.TclError:
                    pass
            return
        try:
            self._recover_interrupted_rebuilds()
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[WARN] Delayed rebuild recovery error: {e}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
