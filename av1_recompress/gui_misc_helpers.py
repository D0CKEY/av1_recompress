from .gui_imports import *
from .gui_shared import *

class MiscHelpersMixin:
    def _get_validated_subtitles_for_video(self, video_path):
        """Get validated subtitles for a specific video.
        
        Args:
            video_path: Path to the video file.
            
        Returns:
            list: List of validated subtitle files.
        """
        subtitle_files = find_subtitle_files(video_path)
        valid, invalid = split_valid_invalid_subtitles(subtitle_files)
        if invalid:
            self._log_invalid_subtitles(video_path, invalid)
        # Visszaadja a valid subtitle fájlokat és az invalid-okat
        return valid, invalid

    def _log_invalid_subtitles(self, video_path, invalid_entries):
        if not invalid_entries:
            return
        if not hasattr(self, 'logged_invalid_subtitles'):
            self.logged_invalid_subtitles = set()
        for sub_path, _, reason in invalid_entries:
            cache_key = (str(video_path), str(sub_path))
            if cache_key in self.logged_invalid_subtitles:
                continue
            self.logged_invalid_subtitles.add(cache_key)
            reason_text = reason or t('unknown_reason')
            message = t('invalid_subtitle_skipped', filename=sub_path.name, reason=reason_text)
            try:
                self.log_status(message)
            except Exception:
                pass
            if LOAD_DEBUG:
                load_debug_log(message)
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(message + "\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass

    def _copy_invalid_subtitles(self, invalid_subtitles, output_file):
        if not invalid_subtitles or not output_file:
            return
        for sub_path, lang_part, reason in invalid_subtitles:
            try:
                if not sub_path.exists():
                    msg = f"[WARN] Hibás felirat nem található, kihagyva: {sub_path}"
                    if LOAD_DEBUG:
                        load_debug_log(msg)
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(msg + "\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    continue
                dest_name = output_file.stem
                if lang_part:
                    dest_name += f".{lang_part}"
                dest_name += sub_path.suffix
                dest_sub_path = output_file.parent / dest_name
                dest_sub_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(sub_path, dest_sub_path)
                copy_msg = f"[WARN] Hibás felirat külső fájlként átmásolva: {dest_sub_path.name} ({reason})"
                try:
                    self.log_status(copy_msg)
                except Exception:
                    pass
                if LOAD_DEBUG:
                    load_debug_log(copy_msg)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(copy_msg + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            except (OSError, PermissionError) as copy_error:
                err_msg = f"[WARN] Hibás felirat másolási hiba: {sub_path} -> {copy_error}"
                if LOAD_DEBUG:
                    load_debug_log(err_msg)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(err_msg + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

    def _get_context_label(self, mode, completed):
        key_map = {
            'auto': 'context_auto',
            'svt': 'context_svt',
            'nvenc': 'context_nvenc'
        }
        base = key_map.get(mode, 'context_auto')
        suffix = '_reencode' if completed else '_encode'
        return t(base + suffix)

