import os
import sys
import subprocess
import platform
import locale
from pathlib import Path

# Import find_program_in_path moved to local scope to prevent circular import

# Alapértelmezett nyelv (korábbi globális változó kompatibilitásához)
CURRENT_LANGUAGE = 'hu'

DECORATIVE_TOKEN_MAP = (
    ("[OK]", "✓"),
    ("[ERROR]", "❌"),
    ("[WARN]", "⚠️"),
    ("[INFO]", "ℹ️"),
    ("[DIR]", "📁"),
    ("[COPY]", "📋"),
    ("[PROBE]", "🔎"),
    ("[SEARCH]", "🔍"),
    ("[STATS]", "📊"),
    ("[STOP]", "⏹️"),
    ("[PLAY]", "▶️"),
    ("[AUTOTEST]", "🤖"),
    ("[REFRESH]", "🔄"),
)

DECORATIVE_GLYPH_MAP = (
    ("✓", "[OK]"),
    ("✔", "[OK]"),
    ("☑", "[OK]"),
    ("✅", "[OK]"),
    ("❌", "[ERROR]"),
    ("⚠️", "[WARN]"),
    ("⚠", "[WARN]"),
    ("ℹ️", "[INFO]"),
    ("ℹ", "[INFO]"),
    ("📁", "[DIR]"),
    ("📋", "[COPY]"),
    ("🔎", "[PROBE]"),
    ("🔍", "[SEARCH]"),
    ("📊", "[STATS]"),
    ("⏹️", "[STOP]"),
    ("⏹", "[STOP]"),
    ("▶️", "[PLAY]"),
    ("▶", "[PLAY]"),
    ("🤖", "[AUTOTEST]"),
    ("🔄", "[REFRESH]"),
)


def to_decorative_text(value):
    """Convert ASCII status/log tokens to decorative glyphs for GUI display."""
    if value is None:
        return ""
    text = str(value)
    for token, glyph in DECORATIVE_TOKEN_MAP:
        text = text.replace(token, glyph)
    return text


def from_decorative_text(value):
    """Convert decorative glyphs back to ASCII tokens for internal logic."""
    if value is None:
        return ""
    text = str(value)
    for glyph, token in DECORATIVE_GLYPH_MAP:
        text = text.replace(glyph, token)
    return text

def _get_log_writer():
    """Dynamically get LOG_WRITER from core module.
    
    The LOG_WRITER is set in app.py after all modules are loaded,
    so we must fetch it dynamically at runtime.
    """
    try:
        if 'av1_recompress.core' in sys.modules:
            core_mod = sys.modules['av1_recompress.core']
            return getattr(core_mod, 'LOG_WRITER', None)
    except Exception:
        pass
    return None

def set_current_language(lang_code):
    """Set the current global language.
    
    Args:
        lang_code (str): 'hu' or 'en'
    """
    global CURRENT_LANGUAGE
    if lang_code in ('hu', 'en'):
        CURRENT_LANGUAGE = lang_code


# Nyelvi szótárok
TRANSLATIONS = {
    'hu': {
        'app_title': 'AV1 Batch Video Encoder',
        'source': 'Forrás:',
        'dest': 'Cél:',
        'browse': 'Tallózás',
        'debug_mode': 'Hibakereső mód (lépésenkénti, temp megőrzés)',
        'debug_dialog_title': '[STOP] Hibakereső mód',
        'debug_dialog_header': '[STOP] HIBAKERESŐ MEGÁLLÁS',
        'debug_dialog_auto_continue': 'Automatikus folytatás: {seconds} mp',
        'debug_dialog_timeout': 'Idő lejárt - folytatás...',
        'debug_dialog_current': 'Jelenlegi:',
        'debug_dialog_next': 'Következő:',
        'debug_dialog_info': 'Info:',
        'debug_dialog_continue': 'Folytatás',
        'dialog_ok': 'OK',
        'dialog_select_source_folder': 'Forrás mappa',
        'dialog_select_dest_folder': 'Cél mappa',
        'fatal_error_title': 'Végzetes hiba',
        'db_notification_saved': '[OK] Adatbázis mentve',
        'db_notification_updated': '[OK] Adatbázis frissítve',
        'status_waiting_db_save': 'Adatbázis mentés befejezésére várakozás...',
        'status_scanning_folder': 'Mappa keresése...',
        'status_queue_loading_stop_requested': 'Queue feltöltés leállítása...',
        'status_graceful_timeout_immediate': 'Graceful stop timeout -> azonnali leállítás...',
        'status_queue_no_processable_tasks': 'Nincs feldolgozható várólista feladat.',
        'status_queue_loading_stopped': 'Queue feltöltés leállítva',
        'status_queue_loading_stopped_with_count': 'Queue feltöltés leállítva ({count} feladat betöltve)',
        'status_waiting_db_save_elapsed': 'Adatbázis mentés befejezésére várakozás... ({seconds}s)',
        'status_scanning_count': 'Keresés: {count} fájl...',
        'status_filtering_files': 'Szűrés: {processed}/{total} fájl...',
        'status_filtering_videos': 'Szűrés: {processed}/{total} ({videos} videó)...',
        'status_videos_found': '{count} videó találva',
        'status_checking_database': '{count} videó találva - adatbázis ellenőrzése...',
        'status_sorting_videos': '{count} videó rendezése...',
        'status_ordering': 'Sorszámozás...',
        'status_ordering_progress': 'Sorszámozás: {processed}/{total}...',
        'status_comparing_database': 'Adatbázis összehasonlítása...',
        'status_comparing_database_count': '{count} videó - adatbázis összehasonlítása...',
        'status_database_progress': 'Adatbázis: {processed}/{total}...',
        'status_processing_load': 'Feldolgozás: {processed}/{total} ({percent}%){eta}',
        'status_eta_minutes': ' (~{minutes}:{seconds:02d} hátra)',
        'status_eta_seconds': ' (~{seconds} mp hátra)',
        'time_seconds_short': '{seconds:.1f} mp',
        'status_load_finished_phase': 'Kész: {count} videó ({time})',
        'status_load_finished': 'Kész: {count} videó betöltve ({time})',
        'status_crf_search_vmaf': '{encoder} CRF keresés (VMAF: {vmaf})...',
        'status_crf_search_vmaf_fallback': '{encoder} CRF keresés (VMAF fallback: {vmaf})...',
        'msg_missing_tool_paths': 'Hiányzó vagy hibás eszközútvonal(ak): {tools}.\nEllenőrizd a beállításokat, majd indítsd újra.',
        'msg_abav1_fatal_error': 'VÉGZETES HIBA: Az ab-av1.exe nem található vagy nem indítható!\n\nHiba: {error}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban.',
        'status_abav1_not_found': '[ERROR] Ab-av1.exe nem található',
        'hover_meta_title': 'Forrás videó metaadatok',
        'hover_meta_loading': 'Metaadatok betöltése...',
        'hover_meta_unavailable': 'A metaadatok nem olvashatók ki.',
        'hover_meta_resolution': 'Felbontás',
        'hover_meta_pix_fmt': 'Pixel formátum',
        'hover_meta_color_space': 'Színtér',
        'hover_meta_color_primaries': 'Színprimerek',
        'hover_meta_color_transfer': 'Transzferfüggvény',
        'hover_meta_color_range': 'Színtartomány',
        'hover_meta_sar': 'SAR',
        'hover_meta_dar': 'DAR',
        'hover_meta_rotation': 'Metaadat rotáció',
        'hover_meta_file_size': 'Méret',
        'hover_meta_bitrate': 'Bitráta',
        'hover_meta_source_section': 'Forrás',
        'hover_meta_output_section': 'Célfájl',
        'hover_meta_output_missing': 'Célfájl metaadat nem érhető el.',
        'hover_meta_tracks': 'Sávok',
        'auto_vmaf_psnr': 'Automatikus VMAF/PSNR számítás átkódolás után',
        'load_videos': 'Videók betöltése',
        'min_vmaf': 'Min VMAF:',
        'vmaf_fallback': 'VMAF csökkentés:',
        'max_encoded': 'Max átkódolt méret:',
        'max_encoded_mode_full': 'Teljes videó',
        'max_encoded_mode_video': 'Videósáv',
        'resize_height': 'Átméretezés max magasság:',
        'nvenc_workers': 'NVENC workerek száma:',
        'svt_workers': 'SVT-AV1 workerek száma:',
        'svt_preset': 'SVT-AV1 preset:',
        'nvenc_workers_short': 'NVENC:',
        'svt_workers_short': 'SVT:',
        'crf_increment': 'CRF lépésköz:',
        'max_cq_limit': 'Max CQ korlát:',
        'max_cq_auto_hint': '(0=auto)',
        'skip_av1': '.av1.mp4/.av1.mkv fájlok kihagyása (átmásolás)',
        'deband_enabled': 'Zajszűrésnél deband kikapcsolása',
        'denoised_master_8bit_test': '8-bit denoised master tesztmód',
        'audio_compression': 'Hangdinamika kompresszió (5.1->2.0)',
        'audio_compression_fast': 'Gyors, mozihoz jó',
        'audio_compression_dialogue': 'Párbeszéd-központú',
        'nvenc_enabled': 'NVENC engedélyezve (40xx/50xx GPU)',
        'vdub_validation_disabled': 'VirtualDub2 validálás kikapcsolása',
        'menu_rebuild_mkv': 'MKV újjáépítés (interleaving javítás)',
        'menu_rebuild_mkv_multi': 'MKV újjáépítés ({count} fájl)',
        'status_rebuild_queued': 'Újjáépítésre jelölve',
        'status_rebuild_in_progress': 'Újjáépítés folyamatban...',
        'log_rebuild_start': 'Újjáépítés indítása: {name}',
        'log_rebuild_done': 'Újjáépítés kész: {name} ({old_size:.1f} MB → {new_size:.1f} MB)',
        'log_rebuild_failed': 'Újjáépítés hiba: {name} - {error}',
        'log_rebuild_stopped': 'Újjáépítés leállítva',
        'log_rebuild_recovery': 'Crash recovery: {name} visszaállítva',
        'msg_rebuild_no_output': 'Nincs kimeneti fájl a kiválasztott videóknál.',
        'msg_rebuild_already_queued': '{count} fájl már újjáépítésre van jelölve.',
        'msg_rebuild_ffmpeg_missing': 'FFmpeg útvonal nincs beállítva, újjáépítés nem lehetséges.',
        'videos_tab': 'Videók',
        'nvenc_console': 'NVENC konzol',
        'svt_console': 'SVT-AV1 konzol',
        'language': 'Nyelv:',
        'ffmpeg_path': 'FFmpeg helye:',
        'virtualdub_path': 'VirtualDub2 helye:',
        'abav1_path': 'ab-av1 helye:',
        'auto_detected': '(automatikusan észlelve)',
        'not_found': '(nem található)',
        'hungarian': 'Magyar',
        'english': 'English',
        'column_order': 'Sorszám',
        'column_denoise': 'Zajszűrés',
        'column_hard_rotate': 'Hard rot',
        'column_video': 'Videó',
        'column_status': 'Státusz',
        'column_cq': 'CQ',
        'column_vmaf': 'VMAF',
        'column_psnr': 'PSNR',
        'column_progress': 'Előrehaladás',
        'column_orig_size': 'Eredeti',
        'column_new_size': 'Új',
        'column_size_change': 'Változás',
        'column_duration': 'Hossz',
        'column_frames': 'Képkockák',
        'column_completed': 'Befejezés',
        'column_preset': 'Preset',
        'txt_frames': 'képkocka',
        'btn_start': 'Start',
        'btn_stop': 'Leállítás',
        'btn_immediate_stop': 'Azonnali leállítás',
        'confirm_immediate_stop_title': 'Azonnali leállítás megerősítése',
        'confirm_immediate_stop_message': 'Biztosan azonnal leállítod a kódolást*\n\nEz azonnal megszakít minden folyamatban lévő munkát.',
        'btn_clear_table': 'Táblázat törlése',
        'btn_hide_completed': 'Elkészültek elrejtése',
        'status_ready': 'Kész áll',
        'status_nvenc_queue': 'NVENC queue-ban vár...',
        'status_svt_queue': 'SVT-AV1 queue-ban vár...',
        'status_completed': '[OK] Kész',
        'status_completed_nvenc': '[OK] Kész (NVENC)',
        'status_completed_svt': '[OK] Kész (SVT-AV1)',
        'status_completed_copy': '[OK] Kész (másolva)',
        'status_completed_exists': '[OK] Kész (már létezik)',
        'status_failed': '[ERROR] Sikertelen',
        'status_source_missing': '[ERROR] Forrás videó hiányzik',
        'status_file_missing': '[ERROR] Fájl hiányzik',
        'status_load_error': '[ERROR] Betöltési hiba',
        'status_vmaf_waiting': 'VMAF ellenőrzésre vár...',
        'status_vmaf_psnr_waiting': 'VMAF/PSNR számításra vár...',
        'status_vmaf_pending': 'VMAF számításra vár...',
        'status_psnr_pending': 'PSNR számításra vár...',
        'status_vmaf_psnr_pending': 'VMAF/PSNR számításra vár...',
        'status_psnr_waiting': 'PSNR ellenőrzésre vár...',
        'status_vmaf_calculating': 'VMAF/PSNR számítás...',
        'status_vmaf_only': 'VMAF számítás...',
        'status_psnr_only': 'PSNR számítás...',
        'status_vmaf_error': '[ERROR] VMAF/PSNR számítás hiba',
        'status_denoising': 'zajszűrés...',
        'status_audio_edit_queue': 'Hangsáv eltávolításra vár...',
        'status_audio_editing': 'hangsáv eltávolítás...',
        'status_audio_edit_done': '[OK] Kész (hangsáv módosítva)',
        'status_audio_edit_failed': '[ERROR] Hangsáv eltávolítás hiba',
        'status_track_saving': 'sávok mentése...',
        'status_metadata_refresh': 'metaadat újraolvasás...',
        'status_nvenc_encoding': 'NVENC kódolás...',
        'status_nvenc_validation': 'NVENC validálás...',
        'status_nvenc_crf_search': 'NVENC CRF keresés...',
        'status_svt_encoding': 'SVT-AV1 kódolás...',
        'status_svt_validation': 'SVT-AV1 validálás...',
        'status_svt_crf_search': 'SVT-AV1 CRF keresés...',
        'status_needs_check': '[WARN] Ellenőrizendő',
        'status_needs_check_nvenc': '[WARN] Ellenőrizendő (NVENC)',
        'status_needs_check_svt': '[WARN] Ellenőrizendő (SVT)',
        'menu_open': 'Megnyitás',
        'menu_source_video': 'Forrás videó',
        'menu_encoded_video': 'Átkódolt videó',
        'menu_encoding_video': 'Készülő videó',
        'menu_vmaf_test': 'Teljes VMAF/PSNR ellenőrzés',
        'menu_vmaf_test_multiple': 'Teljes VMAF/PSNR ellenőrzés ({count} videó)',
        'menu_refresh_metadata': 'Metaadat újraolvasása fájlból',
        'menu_refresh_metadata_multiple': 'Metaadat újraolvasása ({count} videó)',
        'menu_audio_tracks': 'Hangsávok',
        'menu_audio_remove_action': 'Hangsáv eltávolítása',
        'menu_audio_remove_confirm': 'Biztosan eltávolítod ezt a hangsávot*',
        'menu_audio_convert': 'Hangsáv 2.0 konverzió',
        'menu_audio_convert_confirm': 'Létrehozod a kiválasztott térhatású hangsáv 2.0 változatát*\n\n{track}\nMódszer: {method}',
        'audio_convert_title_fast': '2.0 (Gyors kompresszió)',
        'audio_convert_title_dialogue': '2.0 (Párbeszéd kiemelés)',
        'context_auto_encode': 'Automata átkódolás',
        'context_auto_reencode': 'Automata újrakódolás',
        'context_svt_encode': 'SVT-AV1 átkódolás',
        'context_svt_reencode': 'SVT-AV1 újrakódolás',
        'context_nvenc_encode': 'NVENC átkódolás',
        'context_nvenc_reencode': 'NVENC újrakódolás',
        'context_multi_encode_menu': 'Átkódolási opciók',
        'context_multi_reencode_menu': 'Újrakódolási opciók',
        'context_multi_manual_reencode_menu': 'Manuális újrakódolás',
        'menu_vmaf_submenu': 'VMAF/PSNR ellenőrzés',
        'menu_vmaf_full': 'Teljes VMAF/PSNR ellenőrzés',
        'menu_vmaf_only': 'Csak VMAF ellenőrzés',
        'menu_psnr_only': 'Csak PSNR ellenőrzés',
        'menu_reencode': 'Újrakódolás',
        'menu_reencode_manual': 'Manuális újrakódolás',
        'menu_reencode_svt': 'SVT-AV1 újrakódolás',
        'manual_quality_vmaf': 'VMAF ellenőrzéssel',
        'manual_quality_psnr': 'PSNR ellenőrzéssel',
        'manual_quality_both': 'VMAF + PSNR ellenőrzéssel',
        'manual_quality_none': 'Ellenőrzés nélkül',
        'msg_no_video': 'Nincs videó!',
        'msg_invalid_source': 'Érvénytelen forrás!',
        'msg_video_not_exists': 'Videó még nem létezik',
        'msg_output_not_found': 'Az átkódolt videó nem található!',
        'msg_file_info_missing': 'Nem található kimeneti fájl információ!',
        'msg_svt_already_processing': 'Ez a videó már SVT-AV1 feldolgozás alatt áll!',
        'msg_svt_reencode_confirm': 'Biztosan újrakódolod ezt a videót SVT-AV1-gyel*',
        'msg_reencode_confirm': 'Biztosan újrakódolod ezt a videót*',
        'msg_reencode_confirm_bulk': 'Biztosan újrakódolod ezeket a videókat ({count} db)*',
        'msg_reencode_confirm_bulk_note': 'A meglévő kimeneti fájlok felülírásra kerülnek.',
        'msg_delete_failed': 'Nem sikerült törölni a meglévő fájlt:',
        'msg_svt_added': 'SVT-AV1 újrakódolás hozzáadva a sorhoz:',
        'msg_reencode_added': 'Újrakódolás hozzáadva a {encoder} queue-hoz:',
        'msg_restart_encoding_failed': 'Nem sikerült újraindítani az átkódolást: {filename}',
        'msg_stop_active_encoding_failed': 'Nem sikerült leállítani a folyamatban lévő átkódolást:\n{filename}',
        'msg_overwrite_existing_file': 'Ez felülírja a meglévő fájlt!',
        'msg_keep_denoised_master_note': 'Megjegyzés: A zajszűrt mesterfájl (ha van) megmarad és újrafelhasználódik.',
        'msg_active_reencode_svt_confirm': 'Ez a videó jelenleg {task_type} átkódolás alatt van:\n\n{video_label}: {filename}\nQueue: {queue}\n\nA folyamatban lévő átkódolást le szeretnéd állítani és újrakezdeni SVT-AV1 encoderrel*\n\n{overwrite_note}\n\n{keep_master_note}',
        'msg_active_reencode_config_confirm': 'Ez a videó jelenleg {task_type} átkódolás alatt van:\n\n{video_label}: {filename}\nQueue: {queue}\n\nA folyamatban lévő átkódolást le szeretnéd állítani és újrakezdeni a következő beállításokkal*\n\n{encoder_label}: {encoder}\nCQ/CRF: {cq}\n\n{overwrite_note}\n\n{keep_master_note}',
        'msg_active_reencode_manual_confirm': 'Ez a videó jelenleg {task_type} átkódolás alatt van:\n\n{video_label}: {filename}\nQueue: {queue}\n\nA folyamatban lévő átkódolást le szeretnéd állítani és újrakezdeni a következő manuális beállításokkal?\n\n{encoder_label}: {encoder}\nCQ/CRF: {cq} ({cq_range})\n{quality_label}: {quality_check}\n\n{keep_master_note}',
        'task_type_auto': 'automata',
        'task_type_manual': 'manuális',
        'label_video': 'Videó',
        'label_encoder': 'Encoder',
        'label_quality_check': 'Minőség ellenőrzés',
        'msg_clear_confirm': 'Biztosan törlöd az összes videót a táblázatból*',
        'msg_state_load_title': 'Előző állapot betöltése',
        'msg_state_load': 'Található előző mentett állapot!',
        'msg_load_state_content': 'Előző mentett állapot található!\n\nForrás: {}\nCél: {}\n\nVisszatölti az előző állapotot*\n\n(A forrás és cél mappák automatikusan beállításra kerülnek)',
        'status_processing_videos': 'Videók feldolgozása',
        'status_stopped': 'Leállítva',
        'status_starting': 'Kódolás indítása...',
        'status_immediate_stopping': 'Azonnali leállítás...',
        'status_encoding_progress': 'Kódolás: {completed}/{total} kész ({percent}%) - {remaining} hátra',
        'status_paused': 'Leállítva (folytatáshoz \'{start_btn}\' gomb)',
        'status_encoding_finished': 'Kódolás befejezve!',
        'status_processing_finished': 'Feldolgozás befejezve!',
        'msg_encoding_ok': 'Sikeres: {count}',
        'msg_processing_done': 'Feldolgozás kész!',
        'msg_encoding_needs_check': 'Ellenőrizendő: {count}',
        'msg_encoding_failed': 'Sikertelen: {count}',
        # Track Editor
        'track_editor_title': 'Sávok szerkesztése',
        'track_editor_error': 'Hiba',
        'track_editor_loading': 'Sávok betöltése...',
        'track_editor_read_error': 'Nem sikerült olvasni a sávokat vagy a fájl nem található!',
        'track_editor_file_label': 'Fájl: {filename}',
        'track_editor_audio_label': 'Hangsávok (húzd vagy használd a nyilakat)',
        'track_editor_subtitle_label': 'Feliratok',
        'track_editor_col_title': 'Cím',
        'track_editor_col_lang': 'Nyelv',
        'track_editor_col_codec': 'Codec',
        'track_editor_col_channels': 'Csatornák',
        'track_editor_col_size': 'Méret',
        'track_editor_col_bitrate': 'Bitráta',
        'track_editor_col_default': 'Default',
        'track_editor_col_forced': 'Forced',
        'track_editor_btn_save': 'Mentés és Futtatás',
        'track_editor_btn_remove': 'Eltávolítás',
        'track_editor_btn_cancel': 'Mégse',
        'track_editor_progress_title': 'Sávok mentése folyamatban...',
        'track_editor_init_msg': 'Ablak inicializálva. Folyamat indítása...',
        'track_editor_start_msg': 'A művelet elindul...',
        'track_editor_wait_msg': 'Kérlek várj, ez eltarthat 10-30 másodpercig...',
        'track_editor_success': 'KÉSZ! A művelet sikeres volt.',
        'track_editor_auto_close': 'Az ablak automatikusan bezáródik 2 másodperc múlva...',
        'track_editor_error_occurred': 'HIBA TÖRTÉNT! Ellenőrizd a fenti üzeneteket.',
        'track_editor_error_keep_open': 'Az ablak nyitva marad a hibakereséshez.',
        'track_editor_process_running': 'A folyamat még fut! Kérlek várj.',
        'track_editor_warning': 'Figyelem',
        'track_editor_convert_menu': 'Konvertálás 2.0-ra',
        'track_editor_convert_new': 'Új sávként',
        'track_editor_convert_replace': 'Meglévő cseréje',
        'track_editor_converted_suffix': '(2.0 konvertált)',
        'track_editor_btn_load_external': 'Külső betöltése',
        'track_editor_load_subtitle_title': 'Külső felirat betöltése',
        'track_editor_invalid_subtitle': 'Érvénytelen feliratfájl: {filename}\n\nOk: {reason}',
        'track_editor_video_label': 'Videó sáv',
        'track_editor_video_info': 'Stream #{index} | {codec} | {width}x{height}',
        'track_editor_col_offset': 'Offset (ms)',
        'track_editor_col_original': 'Eredeti',
        'track_editor_offset_edit_title': 'Offset beállítása',
        'track_editor_offset_edit_label': 'Offset érték (ms):',
        'track_editor_offset_edit_hint': 'Pozitív = később indul, negatív = korábban',
        'track_editor_rotation_label': 'Forgatás ({degrees}°)',
        'track_editor_rotation_normal': 'Változatlan',
        'track_editor_rotation_right90': 'Jobbra 90°',
        'track_editor_rotation_180': '180°',
        'track_editor_rotation_left90': 'Balra 90°',
        'track_editor_no_video_stream': 'Nem található videósáv',
        'track_editor_btn_default': 'Alapértelmezett',
        'track_editor_btn_forced': 'Kényszerített',
        'track_editor_invalid_offset_value': 'Érvénytelen offset érték',
        'lang_input_title': 'Felirat nyelvi adatok',
        'lang_input_file_label': 'Feliratfájl: {filename}',
        'lang_input_lang_label': 'Nyelvi kód (kötelező):',
        'lang_input_lang_examples': 'Példák: hu, en, de, fr, es',
        'lang_input_title_label': 'Cím/leírás (opcionális):',
        'lang_input_error_empty': 'A nyelvi kód megadása kötelező!',
        'lang_input_error_invalid': 'Érvénytelen nyelvi kód! Használj 2-3 betűs ISO kódot (pl. hu, en).',
        'lang_input_warning_unknown': 'A(z) "{lang}" nyelvi kód ismeretlen.\n\nFolytatod így is*',
        # Additional GUI strings
        'msg_error': 'Hiba',
        'msg_warning': 'Figyelem',
        'msg_setting_change_while_encoding': 'A beállítás {old} értékről {new} értékre módosul kódolás közben.\n\nBiztosan folytatod*',
        'msg_resize_while_encoding': 'A felbontás módosult kódolás közben.\nA változás csak az új fájloknál lép érvénybe.',
        'msg_info': 'Info',
        'msg_started': 'Elindítva',
        'msg_added': 'Hozzáadva',
        'msg_done': 'Kész',
        'msg_no_videos': 'Nincs videó!',
        'msg_open_editor_failed': 'Nem sikerült megnyitni a szerkesztőt: {error}',
        'menu_track_editor': 'Sávok szerkesztése...',
        'menu_faststart': 'Faststart korrekció',
        'menu_denoise_submenu': 'Zajcsökkentés',
        'menu_denoise_multi': 'Zajcsökkentés ({count} tétel)',
        'menu_denoise_ultra_strong': 'Ultra erős zajcsökkentés',
        'menu_denoise_very_strong': 'Nagyon erős zajcsökkentés',
        'menu_denoise_strong': 'Erős zajcsökkentés',
        'menu_denoise_light': 'Gyenge zajcsökkentés',
        'menu_denoise_disable': 'Zajcsökkentés kikapcsolása',
        'menu_hard_rotate_submenu': 'Hard forgatás',
        'menu_hard_rotate_off': '0° (kikapcsolva)',
        'menu_hard_rotate_90': 'Jobbra 90°',
        'menu_hard_rotate_180': '180°',
        'menu_hard_rotate_270': 'Balra 90°',
        'faststart_title': 'Faststart optimalizálás',
        'faststart_start_msg': 'Faststart optimalizálás indul...',
        'faststart_wait_msg': 'Kérlek várj, ez a művelet néhány másodpercig tart...',
        'faststart_success': 'KÉSZ! Faststart optimalizálás sikeres.',
        'faststart_error': 'HIBA TÖRTÉNT! Faststart optimalizálás sikertelen.',
        'menu_mark_verified': '[OK] Ellenőrizve - Elfogadás',
        'menu_mark_verified_multi': '[OK] Ellenőrizve - {count} videó elfogadása',
        'menu_all_logs': 'Összes napló',
        'log_window_title': 'Naplóbejegyzések: {filename}',
        'log_window_video': 'Videó: {filename}',
        'log_window_path': 'Útvonal: {path}',
        'log_window_count': 'Naplóbejegyzések száma: {count}',
        'log_window_no_entries': 'Nincs naplóbejegyzés ehhez a videóhoz.',
        'btn_refresh': 'Frissítés',
        'btn_close': 'Bezárás',
        'status_graceful_stopping': 'Leállítás folyamatban - aktuális kódolás befejezése...',
        'status_queue_filling': 'Queue feltöltés: {current}/{total}...',
        'status_immediate_stopping_progress': 'Azonnali leállítás... ({current}/{total})',
        'db_saving': 'Adatbázis mentése...',
        'db_saving_waiting': 'Adatbázis mentés folyamatban... várás...',
        'db_timeout': 'Adatbázis timeout',
        'db_error': 'Adatbázis hiba',
        'copy_error': 'Hiba nem-videó fájlok másolásakor: {error}',
        'msg_nvenc_disabled': 'Az NVENC le van tiltva, SVT-AV1 lesz használva.',
        # Console/Log messages
        'log_immediate_stop_vmaf': '[STOP] Azonnali leállítás -> VMAF/PSNR worker megszakítva',
        'log_graceful_stop_vmaf': '[STOP] Leállítás kérése -> VMAF/PSNR worker befejezve (nincs több feladat)',
        'log_vmaf_task_received': '[NOTE] VMAF/PSNR feladat megkapva: {filename}',
        'log_cpu_lock_acquiring': '[LOCK] CPU_WORKER_LOCK megszerzése...',
        'log_cpu_lock_acquired': '[OK] CPU_WORKER_LOCK megkapva, VMAF számítás elindítása...',
        'log_status_update_vmaf': '[STATS] Státusz frissítés: VMAF számítás folyamatban...',
        'log_vmaf_test': '[STATS] VMAF/PSNR TESZT: {filename}',
        'log_calling_vmaf': '[SCAN] calculate_full_vmaf hívása...',
        'log_files_exist': 'Fájlok léteznek: reference={ref}, encoded={enc}',
        'log_error_files_missing': '[ERROR] Hiba: Fájlok nem léteznek!',
        'log_files_verified': '[OK] Fájlok ellenőrizve, calculate_full_vmaf hívása...',
        'log_vmaf_returned': '[OK] calculate_full_vmaf visszatért: {result}',
        'log_vmaf_psnr_done': '[OK] VMAF/PSNR teszt kész: {filename} - VMAF: {vmaf} / PSNR: {psnr}',
        'log_vmaf_done': '[OK] VMAF teszt kész: {filename} - VMAF: {vmaf}',
        'log_psnr_done': '[OK] PSNR teszt kész: {filename} - PSNR: {psnr}',
        'log_vmaf_calc_error': '[ERROR] VMAF/PSNR számítás hiba: {filename}',
        'log_vmaf_test_error': '[ERROR] VMAF teszt hiba: {error}',
        'log_graceful_stop_vmaf_done': '[STOP] Leállítás kérése -> VMAF/PSNR worker befejezve',
        'log_immediate_stop_svt': '[STOP] Azonnali leállítás -> SVT-AV1 worker megszakítva',
        'log_stop_request_svt': '[STOP] Leállítás kérés -> SVT-AV1 worker megszakítva',
        'log_graceful_stop_svt': '[STOP] Leállítás kérése -> SVT-AV1 worker megszakítva',
        'log_svt_worker_finished': '### SVT-AV1 WORKER BEFEJEZVE ###',
        'log_source_not_found': '[WARN] Hiba: A forrás videó nem található: {path}',
        'log_svt_processing': 'SVT-AV1 FELDOLGOZÁS: {filename}',
        'log_full_path': 'TELJES ÚTVONAL: {path}',
        'log_reason': 'Ok: {reason}',
        'log_probing_source': '[SCAN] Probing source video with VirtualDub2...',
        'log_probe_failed': '[WARN] Source video probe failed! Fallback to copy.',
        'log_waiting_svt_slot': '[WAIT] Várakozás SVT-AV1 slot-ra...',
        'log_svt_slot_acquired': '[OK] SVT-AV1 slot megszerzve, CRF keresés kezdése...',
        'log_svt_manual_reencode': '[INFO] SVT-AV1 manuális újrakódolás (CRF keresés kihagyva): {filename}',
        'log_target_crf': 'Cél CRF: {crf}',
        'log_svt_crf_search': '[INFO] SVT-AV1 CRF keresés indul: {filename}',
        'log_crf_file_check': '[SCAN] CRF keresés fájl ellenőrzés (teljes útvonal): {path}',
        'log_immediate_stop_nvenc': '[STOP] Azonnali leállítás -> NVENC worker megszakítva',
        'log_stop_request_nvenc': '[STOP] Leállítás kérés -> NVENC worker megszakítva',
        'log_graceful_stop_nvenc': '[STOP] Leállítás kérése -> NVENC worker megszakítva',
        'log_nvenc_worker_finished': '### NVENC WORKER BEFEJEZVE ###',
        'log_nvenc_processing': 'NVENC FELDOLGOZÁS: {filename}',
        'log_waiting_nvenc_slot': '[WAIT] Várakozás NVENC slot-ra...',
        'log_nvenc_slot_acquired': '[OK] NVENC slot megszerzve, CRF keresés kezdése...',
        'log_nvenc_manual_reencode': '[INFO] NVENC manuális újrakódolás (CRF keresés kihagyva): {filename}',
        'log_nvenc_crf_search': '[INFO] NVENC CRF keresés indul: {filename}',
        'log_vmaf_stopped': '[STOP] VMAF számítás megszakítva: {filename}',
        'log_scan_error': 'Keresési hiba: {error}',
        'log_copy_av1_error': '[ERROR] Hiba .av1 fájl másolásakor ({filename}): {error}',
        'log_critical_error': 'KRITIKUS HIBA a(z) {worker} workerben: {error}',
        'log_status_revert_error': 'Hiba státusz visszaállításakor: {error}',
        'log_error': 'Hiba: {error}',
        'status_error_with_msg': '[ERROR] Hiba: {msg}',
        'status_error_copy_failed': '[ERROR] Hiba (másolás sikertelen)',
        'status_source_missing': '[ERROR] Forrás videó hiányzik',
        'log_gui_update_error': '[WARN] Hiba GUI frissítés során: {error}',
        'log_file_delete_error': '[ERROR] Hiba a fájl törlésekor: {error}',
        'summary_failed': 'Hiba: {count}',
        # HTTP API messages
        'api_starting': 'Kódolás indítása...',
        'api_stopping': 'Leállítás folyamatban...',
        'api_immediate_stop': 'Azonnali leállítás...',
        'api_loading': 'Videók betöltése...',
        'api_settings_updated': 'Beállítások frissítve',
        # Additional log messages
        'log_immediate_stop_vmaf_interrupted': '[STOP] Azonnali leállítás -> VMAF számítás megszakítva: {filename}',
        'log_vmaf_interrupted': '[STOP] VMAF számítás megszakítva: {filename}',
        'log_immediate_stop_encoding': '[STOP] Azonnali leállítás – encoding worker megszakítva',
        'log_files_not_exist': 'Fájlok nem léteznek: reference={ref}, encoded={enc}',
        'log_tree_access_error': '[ERROR] KRITIKUS HIBA: Tree elérés sikertelen GUI thread-ben: {error}',
        'log_db_update_error': '[WARN] [vmaf_worker] Adatbázis frissítés hiba: {error} | video: {video}',
        'status_initializing': 'Inicializálás...',
        # Settings change during encoding warning
        'settings_change_during_encoding_title': 'Beállítás módosítás',
        'settings_change_during_encoding_message': 'Átkódolás folyamatban!\n\nA változtatás csak a következő sorrakerülő videótól lesz figyelembe véve.\n\nOK: Alkalmazza a változtatást\nMégse: Visszaállítja az előző értéket',
        # HTTP Server
        'http_server_enabled': 'HTTP szerver',
        'http_port': 'Port:',
        'http_server_title': 'HTTP szerver',
        'http_server_fallback_title': 'HTTP szerver (Fallback)',
        'http_server_error_title': 'HTTP szerver hiba',
        'http_server_started_message': 'HTTP szerver elindult!\n\nHelyi elérés: http://127.0.0.1:{port}\nHálózati elérés: http://<IP>:{port}\n\nNyisd meg a böngészőben!',
        'http_server_started_fallback_message': 'HTTP szerver elindult!\n(Flask nem elérhető, fallback mód)\n\nHelyi elérés: http://127.0.0.1:{port}\nHálózati elérés: http://{local_ip}:{port}\n\nNyisd meg a böngészőben!',
        'http_server_started_simple_message': 'HTTP szerver elindult!\n\nHelyi elérés: http://127.0.0.1:{port}\n\nNyisd meg a böngészőben!',
        'http_server_start_error_message': 'Nem sikerült elindítani a HTTP szervert!\n\nHiba: {error}\n\nPort: {port}\nPróbálj meg másik portot (pl. 8080, 8000)!',
        # Invalid subtitle messages
        'unknown_reason': 'ismeretlen ok',
        'invalid_subtitle_skipped': '[WARN] Hibás felirat kihagyva a beágyazásból: {filename} ({reason}). Külső fájlként kerül átmásolásra.',
        'msg_confirm_reencode_title': 'Újrakódolás megerősítése',
        'msg_confirm_reencode_denoise': 'A kiválasztott videó már át lett kódolva.\nA zajszűrés beállításának módosításához a jelenlegi kódolt fájl törlésre kerül, és a videó visszakerül a várólistára.\n\nBiztosan folytatja*',
        'denoise_multi_header': 'Zajszűrés beállítása {count} tételre: {denoise}',
        'denoise_multi_line_pending': '• {count} függő tétel: beállítás',
        'denoise_multi_line_completed': '• {count} kész tétel: a jelenlegi kódolt fájl TÖRLÉSRE kerül és újrakódolódik',
        'denoise_multi_line_active': '• {count} aktívan kódolódó tétel: leáll és új beállítással újrasorolódik',
        'denoise_multi_footer': 'Biztosan folytatja?',
        'msg_confirm_stop_and_reencode_denoise': '"{filename}" jelenleg átkódolás alatt áll!\n\nA zajszűrés módosítása ({denoise}) LEÁLLÍTJA az aktuális átkódolást, törli a részben kész fájlt, és visszakerül a várólistára az új beállítással.\n\nBiztosan folytatod*',
        # Hybrid SMDegrain
        'hybrid_path': 'Hybrid helye:',
        'select_hybrid_folder': 'Hybrid mappa kiválasztása',
        'smdegrain_master_creating': '🔇 SMDegrain zajszűrés folyamatban (VapourSynth)...',
        'smdegrain_master_created': '[OK] SMDegrain zajszűrés kész',
        'smdegrain_fallback': '[WARN] SMDegrain sikertelen, vaguedenoiser használata',
        'smdegrain_cleanup': '[DEL] SMDegrain ideiglenes fájlok törölve',
        'smdegrain_vpy_error': '[WARN] VPY script hiba: {error}',
        'smdegrain_vspipe_error': '[WARN] VSPipe hiba: {error}',
        'confirm_exit_title': 'Kilépés megerősítése',
        'confirm_exit_listening': 'A kódolás folyamatban van!\n\nBiztosan ki akarsz lépni*',
        # HTTP GUI
        'http_folders': 'Mappák',
        'http_encoding_settings': 'Kódolási beállítások',
        'http_resize': 'Átméretezés',
        'http_save_settings': 'Beállítások mentése',
        'http_total': 'Összesen',
        'http_in_progress': 'Folyamatban',
        'http_pending': 'Várakozik',
        'http_eta': 'ETA',
        'http_loading': 'BETÖLTÉS...',
        'http_stopping': 'LEÁLLÍTÁS...',
        'http_encoding': 'KÓDOLÁS',
        'http_idle': 'TÉTLEN',
        'http_confirm_stop_immediate': 'Biztosan azonnal leállítod* A folyamatban lévő kódolások megszakadnak!',
        'http_settings_saved': 'Beállítások mentve!',
        'http_saved_with_warning': '[WARN] {warning}\n\nBeállítások mentve!',
        'http_load_hint': 'Adj meg egy forrás mappát és kattints a "Videók betöltése" gombra.',
        'http_auto_refresh': 'Automatikus frissítés: 1.5 mp',
        'http_loading_status': 'Betöltés...',
        'http_elapsed': 'Eltelt idő:',
        'pending_quality_resume_title': 'VMAF/PSNR számítás folytatása',
        'pending_quality_resume_message': 'Újraindítás után {count} videó vár VMAF/PSNR számításra.\n\nAkarod most elindítani a minőség ellenőrzést?',
        'pending_quality_started': '{count} VMAF/PSNR számítás elindítva...',
        'mobile_orig_label': 'Eredeti:',
        'mobile_new_label': 'Átkódolt:',
        'mobile_savings_label': 'Megtakarítás:',
        'http_sum_total': 'Összesen',
        'http_sum_pending': 'Várakozik',
        'http_sum_encoding': 'Kódolás',
        'http_sum_completed': 'Kész',
        'http_sum_failed': 'Hiba',
        'http_sum_orig': 'Eredeti',
        'http_sum_new': 'Átkódolt',
        'http_sum_savings': 'Megtakarítás',
    },
    'en': {
        'app_title': 'AV1 Batch Video Encoder',
        'source': 'Source:',
        'dest': 'Destination:',
        'browse': 'Browse',
        'debug_mode': 'Debug mode (step-by-step, keep temp)',
        'debug_dialog_title': '[STOP] Debug Mode',
        'debug_dialog_header': '[STOP] DEBUG STOP',
        'debug_dialog_auto_continue': 'Auto-continue in: {seconds} seconds',
        'debug_dialog_timeout': 'Timeout reached - continuing...',
        'debug_dialog_current': 'Current:',
        'debug_dialog_next': 'Next:',
        'debug_dialog_info': 'Info:',
        'debug_dialog_continue': 'Continue',
        'dialog_ok': 'OK',
        'dialog_select_source_folder': 'Source Folder',
        'dialog_select_dest_folder': 'Destination Folder',
        'fatal_error_title': 'Fatal Error',
        'db_notification_saved': '[OK] Database saved',
        'db_notification_updated': '[OK] Database updated',
        'status_waiting_db_save': 'Waiting for database save to finish...',
        'status_scanning_folder': 'Scanning folder...',
        'status_queue_loading_stop_requested': 'Stopping queue loading...',
        'status_graceful_timeout_immediate': 'Graceful stop timeout -> immediate stop...',
        'status_queue_no_processable_tasks': 'No processable queued task.',
        'status_queue_loading_stopped': 'Queue loading stopped',
        'status_queue_loading_stopped_with_count': 'Queue loading stopped ({count} task(s) loaded)',
        'status_waiting_db_save_elapsed': 'Waiting for database save to finish... ({seconds}s)',
        'status_scanning_count': 'Scanning: {count} file(s)...',
        'status_filtering_files': 'Filtering: {processed}/{total} file(s)...',
        'status_filtering_videos': 'Filtering: {processed}/{total} ({videos} video(s))...',
        'status_videos_found': '{count} video(s) found',
        'status_checking_database': '{count} video(s) found - checking database...',
        'status_sorting_videos': 'Sorting {count} video(s)...',
        'status_ordering': 'Numbering...',
        'status_ordering_progress': 'Numbering: {processed}/{total}...',
        'status_comparing_database': 'Comparing database...',
        'status_comparing_database_count': '{count} video(s) - comparing database...',
        'status_database_progress': 'Database: {processed}/{total}...',
        'status_processing_load': 'Processing: {processed}/{total} ({percent}%){eta}',
        'status_eta_minutes': ' (~{minutes}:{seconds:02d} remaining)',
        'status_eta_seconds': ' (~{seconds}s remaining)',
        'time_seconds_short': '{seconds:.1f}s',
        'status_load_finished_phase': 'Done: {count} video(s) ({time})',
        'status_load_finished': 'Done: {count} video(s) loaded ({time})',
        'status_crf_search_vmaf': '{encoder} CRF search (VMAF: {vmaf})...',
        'status_crf_search_vmaf_fallback': '{encoder} CRF search (VMAF fallback: {vmaf})...',
        'msg_missing_tool_paths': 'Missing or invalid tool path(s): {tools}.\nCheck the settings, then restart.',
        'msg_abav1_fatal_error': 'FATAL ERROR: ab-av1.exe not found or cannot be started!\n\nError: {error}\n\nThe program cannot start ab-av1.exe, so CRF search is not possible.\n\nCheck if ab-av1.exe exists at the specified path, or set the correct path in settings.',
        'status_abav1_not_found': '[ERROR] Ab-av1.exe not found',
        'hover_meta_title': 'Source video metadata',
        'hover_meta_loading': 'Loading metadata...',
        'hover_meta_unavailable': 'Metadata is unavailable.',
        'hover_meta_resolution': 'Resolution',
        'hover_meta_pix_fmt': 'Pixel format',
        'hover_meta_color_space': 'Color space',
        'hover_meta_color_primaries': 'Color primaries',
        'hover_meta_color_transfer': 'Transfer function',
        'hover_meta_color_range': 'Color range',
        'hover_meta_sar': 'SAR',
        'hover_meta_dar': 'DAR',
        'hover_meta_rotation': 'Metadata rotation',
        'hover_meta_file_size': 'Size',
        'hover_meta_bitrate': 'Bitrate',
        'hover_meta_source_section': 'Source',
        'hover_meta_output_section': 'Output',
        'hover_meta_output_missing': 'Output metadata is unavailable.',
        'hover_meta_tracks': 'Tracks',
        'auto_vmaf_psnr': 'Automatic VMAF/PSNR calculation after encoding',
        'load_videos': 'Load Videos',
        'min_vmaf': 'Min VMAF:',
        'vmaf_fallback': 'VMAF Reduction:',
        'max_encoded': 'Max Re-encoded Size:',
        'max_encoded_mode_full': 'Full video',
        'max_encoded_mode_video': 'Video track',
        'resize_height': 'Resize Max Height:',
        'nvenc_workers': 'NVENC workers:',
        'svt_workers': 'SVT-AV1 workers:',
        'svt_preset': 'SVT-AV1 preset:',
        'nvenc_workers_short': 'NVENC:',
        'svt_workers_short': 'SVT:',
        'crf_increment': 'CRF Increment:',
        'max_cq_limit': 'Max CQ limit:',
        'max_cq_auto_hint': '(0=auto)',
        'skip_av1': 'Skip .av1.mp4/.av1.mkv re-encoding (copy)',
        'deband_enabled': 'Disable deband during denoise',
        'denoised_master_8bit_test': '8-bit denoised master test mode',
        'audio_compression': 'Audio dynamics compression (5.1->2.0)',
        'audio_compression_fast': 'Fast, cinema-ready',
        'audio_compression_dialogue': 'Dialogue-centered',
        'nvenc_enabled': 'NVENC enabled (40xx/50xx GPU)',
        'vdub_validation_disabled': 'Disable VirtualDub2 validation',
        'menu_rebuild_mkv': 'MKV Rebuild (interleave fix)',
        'menu_rebuild_mkv_multi': 'MKV Rebuild ({count} files)',
        'status_rebuild_queued': 'Queued for rebuild',
        'status_rebuild_in_progress': 'Rebuilding...',
        'log_rebuild_start': 'Starting rebuild: {name}',
        'log_rebuild_done': 'Rebuild done: {name} ({old_size:.1f} MB → {new_size:.1f} MB)',
        'log_rebuild_failed': 'Rebuild failed: {name} - {error}',
        'log_rebuild_stopped': 'Rebuild stopped',
        'log_rebuild_recovery': 'Crash recovery: {name} restored',
        'msg_rebuild_no_output': 'No output file for selected videos.',
        'msg_rebuild_already_queued': '{count} files already queued for rebuild.',
        'msg_rebuild_ffmpeg_missing': 'FFmpeg path not set, rebuild not possible.',
        'videos_tab': 'Videos',
        'nvenc_console': 'NVENC Console',
        'svt_console': 'SVT-AV1 Console',
        'language': 'Language:',
        'ffmpeg_path': 'FFmpeg path:',
        'virtualdub_path': 'VirtualDub2 path:',
        'abav1_path': 'ab-av1 path:',
        'auto_detected': '(auto-detected)',
        'not_found': '(not found)',
        'hungarian': 'Magyar',
        'english': 'English',
        'column_order': 'Order',
        'column_denoise': 'Denoise',
        'column_hard_rotate': 'Hard Rot',
        'column_video': 'Video',
        'column_status': 'Status',
        'column_cq': 'CQ',
        'column_vmaf': 'VMAF',
        'column_psnr': 'PSNR',
        'column_progress': 'Progress',
        'column_orig_size': 'Original',
        'column_new_size': 'New',
        'column_size_change': 'Change',
        'column_duration': 'Duration',
        'column_frames': 'Frames',
        'column_completed': 'Completed',
        'column_preset': 'Preset',
        'txt_frames': 'frames',
        'btn_start': 'Start Encoding',
        'btn_stop': 'Stop',
        'btn_immediate_stop': 'Immediate Stop',
        'confirm_immediate_stop_title': 'Confirm Immediate Stop',
        'confirm_immediate_stop_message': 'Are you sure you want to immediately stop encoding*\n\nThis will immediately terminate all work in progress.',
        'btn_clear_table': 'Clear Table',
        'btn_hide_completed': 'Hide Completed',
        'status_ready': 'Ready',
        'status_nvenc_queue': 'NVENC queue waiting...',
        'status_svt_queue': 'SVT-AV1 queue waiting...',
        'status_completed': '[OK] Done',
        'status_completed_nvenc': '[OK] Done (NVENC)',
        'status_completed_svt': '[OK] Done (SVT-AV1)',
        'status_completed_copy': '[OK] Done (copied)',
        'status_completed_exists': '[OK] Done (already exists)',
        'status_failed': '[ERROR] Failed',
        'status_source_missing': '[ERROR] Source video missing',
        'status_file_missing': '[ERROR] File missing',
        'status_load_error': '[ERROR] Load error',
        'status_vmaf_waiting': 'VMAF check waiting...',
        'status_vmaf_psnr_waiting': 'VMAF/PSNR calculation waiting...',
        'status_vmaf_pending': 'VMAF calculation waiting...',
        'status_psnr_pending': 'PSNR calculation waiting...',
        'status_vmaf_psnr_pending': 'VMAF/PSNR calculation waiting...',
        'status_psnr_waiting': 'PSNR check waiting...',
        'status_vmaf_calculating': 'VMAF/PSNR calculation...',
        'status_vmaf_only': 'VMAF calculation...',
        'status_psnr_only': 'PSNR calculation...',
        'status_vmaf_error': '[ERROR] VMAF/PSNR calculation error',
        'status_denoising': 'denoising...',
        'status_audio_edit_queue': 'Audio track removal queued...',
        'status_audio_editing': 'audio track removal...',
        'status_audio_edit_done': '[OK] Done (audio updated)',
        'status_audio_edit_failed': '[ERROR] Audio track removal error',
        'status_track_saving': 'saving tracks...',
        'status_metadata_refresh': 're-reading metadata...',
        'status_nvenc_encoding': 'NVENC encoding...',
        'status_nvenc_validation': 'NVENC validation...',
        'status_nvenc_crf_search': 'NVENC CRF search...',
        'status_svt_encoding': 'SVT-AV1 encoding...',
        'status_svt_validation': 'SVT-AV1 validation...',
        'status_svt_crf_search': 'SVT-AV1 CRF search...',
        'status_needs_check': '[WARN] Needs Check',
        'status_needs_check_nvenc': '[WARN] Needs Check (NVENC)',
        'status_needs_check_svt': '[WARN] Needs Check (SVT)',
        'menu_open': 'Open',
        'menu_source_video': 'Source Video',
        'menu_encoded_video': 'Encoded Video',
        'menu_encoding_video': 'Video in Progress',
        'menu_vmaf_test': 'Full VMAF/PSNR Check',
        'menu_vmaf_test_multiple': 'Full VMAF/PSNR Check ({count} videos)',
        'menu_refresh_metadata': 'Re-read metadata from file',
        'menu_refresh_metadata_multiple': 'Re-read metadata ({count} videos)',
        'menu_audio_tracks': 'Audio Tracks',
        'menu_audio_remove_action': 'Remove audio track',
        'menu_audio_remove_confirm': 'Are you sure you want to remove this audio track*',
        'menu_audio_convert': 'Convert Surround -> 2.0',
        'menu_audio_convert_confirm': 'Create a 2.0 copy from the selected surround track*\n\n{track}\nMethod: {method}',
        'audio_convert_title_fast': '2.0 (Dynamics Compression)',
        'audio_convert_title_dialogue': '2.0 (Dialogue Boost)',
        'context_auto_encode': 'Automatic encode',
        'context_auto_reencode': 'Automatic re-encode',
        'context_svt_encode': 'SVT-AV1 encode',
        'context_svt_reencode': 'SVT-AV1 re-encode',
        'context_nvenc_encode': 'NVENC encode',
        'context_nvenc_reencode': 'NVENC re-encode',
        'context_multi_encode_menu': 'Encoding options',
        'context_multi_reencode_menu': 'Re-encode options',
        'context_multi_manual_reencode_menu': 'Manual re-encode',
        'menu_vmaf_submenu': 'VMAF/PSNR Check',
        'menu_vmaf_full': 'Full VMAF/PSNR Check',
        'menu_vmaf_only': 'VMAF Check only',
        'menu_psnr_only': 'PSNR Check only',
        'menu_reencode': 'Re-encode',
        'menu_reencode_manual': 'Manual re-encode',
        'menu_reencode_svt': 'SVT-AV1 Re-encode',
        'manual_quality_vmaf': 'With VMAF check',
        'manual_quality_psnr': 'With PSNR check',
        'manual_quality_both': 'With VMAF + PSNR check',
        'manual_quality_none': 'No quality check',
        'msg_no_video': 'No videos!',
        'msg_invalid_source': 'Invalid source!',
        'msg_video_not_exists': 'Video does not exist yet',
        'msg_output_not_found': 'Encoded video not found!',
        'msg_file_info_missing': 'Output file information not found!',
        'msg_svt_already_processing': 'This video is already being processed by SVT-AV1!',
        'msg_svt_reencode_confirm': 'Are you sure you want to re-encode this video with SVT-AV1*',
        'msg_reencode_confirm': 'Are you sure you want to re-encode this video*',
        'msg_reencode_confirm_bulk': 'Are you sure you want to re-encode these videos ({count} items)*',
        'msg_reencode_confirm_bulk_note': 'Existing output files will be overwritten.',
        'msg_delete_failed': 'Failed to delete existing file:',
        'msg_svt_added': 'SVT-AV1 re-encoding added to queue:',
        'msg_reencode_added': 'Re-encoding added to {encoder} queue:',
        'msg_restart_encoding_failed': 'Failed to restart encoding for: {filename}',
        'msg_stop_active_encoding_failed': 'Failed to stop the active encoding:\n{filename}',
        'msg_overwrite_existing_file': 'This will overwrite the existing file!',
        'msg_keep_denoised_master_note': 'Note: The denoised master file, if present, will be kept and reused.',
        'msg_active_reencode_svt_confirm': 'This video is currently being {task_type} encoded:\n\n{video_label}: {filename}\nQueue: {queue}\n\nDo you want to stop the active encoding and restart it with the SVT-AV1 encoder*\n\n{overwrite_note}\n\n{keep_master_note}',
        'msg_active_reencode_config_confirm': 'This video is currently being {task_type} encoded:\n\n{video_label}: {filename}\nQueue: {queue}\n\nDo you want to stop the active encoding and restart it with these settings*\n\n{encoder_label}: {encoder}\nCQ/CRF: {cq}\n\n{overwrite_note}\n\n{keep_master_note}',
        'msg_active_reencode_manual_confirm': 'This video is currently being {task_type} encoded:\n\n{video_label}: {filename}\nQueue: {queue}\n\nDo you want to stop the active encoding and restart it with these manual settings?\n\n{encoder_label}: {encoder}\nCQ/CRF: {cq} ({cq_range})\n{quality_label}: {quality_check}\n\n{keep_master_note}',
        'task_type_auto': 'automatic',
        'task_type_manual': 'manual',
        'label_video': 'Video',
        'label_encoder': 'Encoder',
        'label_quality_check': 'Quality check',
        'msg_clear_confirm': 'Are you sure you want to clear all videos from the table*',
        'msg_state_load_title': 'Load Previous State',
        'msg_state_load': 'Previous saved state found!',
        'msg_load_state_content': 'Previous saved state found!\n\nSource: {}\nDest: {}\n\nLoad previous state*\n\n(Source and destination folders will be set automatically)',
        'status_processing_videos': 'Processing videos',
        'status_stopped': 'Stopped',
        'status_starting': 'Starting encoding...',
        'status_immediate_stopping': 'Immediate stop...',
        'status_encoding_progress': 'Encoding: {completed}/{total} done ({percent}%) - {remaining} remaining',
        'status_paused': 'Paused (press \'{start_btn}\' to continue)',
        'status_encoding_finished': 'Encoding finished!',
        'status_processing_finished': 'Processing finished!',
        'msg_encoding_ok': 'Successful: {count}',
        'msg_processing_done': 'Processing complete!',
        'msg_encoding_needs_check': 'Needs check: {count}',
        'msg_encoding_failed': 'Failed: {count}',
        # Track Editor
        'track_editor_title': 'Edit Tracks',
        'track_editor_error': 'Error',
        'track_editor_loading': 'Loading tracks...',
        'track_editor_read_error': 'Failed to read tracks or file not found!',
        'track_editor_file_label': 'File: {filename}',
        'track_editor_audio_label': 'Audio Tracks (drag or use arrows)',
        'track_editor_subtitle_label': 'Subtitles',
        'track_editor_col_title': 'Title',
        'track_editor_col_lang': 'Language',
        'track_editor_col_codec': 'Codec',
        'track_editor_col_channels': 'Channels',
        'track_editor_col_size': 'Size',
        'track_editor_col_bitrate': 'Bitrate',
        'track_editor_col_default': 'Default',
        'track_editor_col_forced': 'Forced',
        'track_editor_btn_save': 'Save and Run',
        'track_editor_btn_remove': 'Remove',
        'track_editor_btn_cancel': 'Cancel',
        'track_editor_progress_title': 'Saving tracks...',
        'track_editor_init_msg': 'Window initialized. Starting process...',
        'track_editor_start_msg': 'Operation starting...',
        'track_editor_wait_msg': 'Please wait, this may take 10-30 seconds...',
        'track_editor_success': 'DONE! Operation completed successfully.',
        'track_editor_auto_close': 'Window will close automatically in 2 seconds...',
        'track_editor_error_occurred': 'ERROR OCCURRED! Check the messages above.',
        'track_editor_error_keep_open': 'Window remains open for debugging.',
        'track_editor_process_running': 'Process is still running! Please wait.',
        'track_editor_warning': 'Warning',
        'track_editor_convert_menu': 'Convert to 2.0',
        'track_editor_convert_new': 'As new track',
        'track_editor_convert_replace': 'Replace existing',
        'track_editor_converted_suffix': '(2.0 converted)',
        'track_editor_btn_load_external': 'Load External',
        'track_editor_load_subtitle_title': 'Load External Subtitle',
        'track_editor_invalid_subtitle': 'Invalid subtitle file: {filename}\n\nReason: {reason}',
        'track_editor_video_label': 'Video Track',
        'track_editor_video_info': 'Stream #{index} | {codec} | {width}x{height}',
        'track_editor_col_offset': 'Offset (ms)',
        'track_editor_col_original': 'Original',
        'track_editor_offset_edit_title': 'Set Offset',
        'track_editor_offset_edit_label': 'Offset value (ms):',
        'track_editor_offset_edit_hint': 'Positive = starts later, negative = starts earlier',
        'track_editor_rotation_label': 'Rotation ({degrees}°)',
        'track_editor_rotation_normal': 'Unchanged',
        'track_editor_rotation_right90': 'Right 90°',
        'track_editor_rotation_180': '180°',
        'track_editor_rotation_left90': 'Left 90°',
        'track_editor_no_video_stream': 'No video stream found',
        'track_editor_btn_default': 'Default',
        'track_editor_btn_forced': 'Forced',
        'track_editor_invalid_offset_value': 'Invalid offset value',
        'lang_input_title': 'Subtitle Language Settings',
        'lang_input_file_label': 'Subtitle file: {filename}',
        'lang_input_lang_label': 'Language code (required):',
        'lang_input_lang_examples': 'Examples: hu, en, de, fr, es',
        'lang_input_title_label': 'Title/description (optional):',
        'lang_input_error_empty': 'Language code is required!',
        'lang_input_error_invalid': 'Invalid language code! Use 2-3 letter ISO code (e.g., hu, en).',
        'lang_input_warning_unknown': 'Language code "{lang}" is not recognized.\n\nContinue anyway*',
        # Additional GUI strings
        'msg_error': 'Error',
        'msg_warning': 'Warning',
        'msg_setting_change_while_encoding': 'The setting is being changed from {old} to {new} during encoding.\n\nAre you sure you want to continue*',
        'msg_resize_while_encoding': 'Resolution changed during encoding.\nThe change will only apply to new files.',
        'msg_info': 'Info',
        'msg_started': 'Started',
        'msg_added': 'Added',
        'msg_done': 'Done',
        'msg_no_videos': 'No videos!',
        'msg_open_editor_failed': 'Failed to open editor: {error}',
        'menu_track_editor': 'Edit Tracks...',
        'menu_faststart': 'Faststart Fix',
        'menu_denoise_submenu': 'Noise Reduction',
        'menu_denoise_multi': 'Noise Reduction ({count} items)',
        'menu_denoise_ultra_strong': 'Ultra Strong Noise Reduction',
        'menu_denoise_very_strong': 'Very Strong Noise Reduction',
        'menu_denoise_strong': 'Strong Noise Reduction',
        'menu_denoise_light': 'Light Noise Reduction',
        'menu_denoise_disable': 'Disable Noise Reduction',
        'menu_hard_rotate_submenu': 'Hard Rotate',
        'menu_hard_rotate_off': '0° (Off)',
        'menu_hard_rotate_90': 'Right 90°',
        'menu_hard_rotate_180': '180°',
        'menu_hard_rotate_270': 'Left 90°',
        'faststart_title': 'Faststart Optimization',
        'faststart_start_msg': 'Starting faststart optimization...',
        'faststart_wait_msg': 'Please wait, this operation takes a few seconds...',
        'faststart_success': 'DONE! Faststart optimization successful.',
        'faststart_error': 'ERROR! Faststart optimization failed.',
        'menu_mark_verified': '[OK] Verified - Accept',
        'menu_mark_verified_multi': '[OK] Verified - Accept {count} videos',
        'menu_all_logs': 'All Logs',
        'log_window_title': 'Log entries: {filename}',
        'log_window_video': 'Video: {filename}',
        'log_window_path': 'Path: {path}',
        'log_window_count': 'Log entry count: {count}',
        'log_window_no_entries': 'No log entries for this video.',
        'btn_refresh': 'Refresh',
        'btn_close': 'Close',
        'status_graceful_stopping': 'Stopping - completing current encoding...',
        'status_queue_filling': 'Queue filling: {current}/{total}...',
        'status_immediate_stopping_progress': 'Immediate stop... ({current}/{total})',
        'db_saving': 'Saving database...',
        'db_saving_waiting': 'Database save in progress... waiting...',
        'db_timeout': 'Database timeout',
        'db_error': 'Database error',
        'copy_error': 'Error copying non-video files: {error}',
        'msg_nvenc_disabled': 'NVENC is disabled, SVT-AV1 will be used.',
        # Console/Log messages
        'log_immediate_stop_vmaf': '[STOP] Immediate stop -> VMAF/PSNR worker interrupted',
        'log_graceful_stop_vmaf': '[STOP] Stop requested -> VMAF/PSNR worker finished (no more tasks)',
        'log_vmaf_task_received': '[NOTE] VMAF/PSNR task received: {filename}',
        'log_cpu_lock_acquiring': '[LOCK] Acquiring CPU_WORKER_LOCK...',
        'log_cpu_lock_acquired': '[OK] CPU_WORKER_LOCK acquired, starting VMAF calculation...',
        'log_status_update_vmaf': '[STATS] Status update: VMAF calculation in progress...',
        'log_vmaf_test': '[STATS] VMAF/PSNR TEST: {filename}',
        'log_calling_vmaf': '[SCAN] Calling calculate_full_vmaf...',
        'log_files_exist': 'Files exist: reference={ref}, encoded={enc}',
        'log_error_files_missing': '[ERROR] Error: Files do not exist!',
        'log_files_verified': '[OK] Files verified, calling calculate_full_vmaf...',
        'log_vmaf_returned': '[OK] calculate_full_vmaf returned: {result}',
        'log_vmaf_psnr_done': '[OK] VMAF/PSNR test done: {filename} - VMAF: {vmaf} / PSNR: {psnr}',
        'log_vmaf_done': '[OK] VMAF test done: {filename} - VMAF: {vmaf}',
        'log_psnr_done': '[OK] PSNR test done: {filename} - PSNR: {psnr}',
        'log_vmaf_calc_error': '[ERROR] VMAF/PSNR calculation error: {filename}',
        'log_vmaf_test_error': '[ERROR] VMAF test error: {error}',
        'log_graceful_stop_vmaf_done': '[STOP] Stop requested -> VMAF/PSNR worker finished',
        'log_immediate_stop_svt': '[STOP] Immediate stop -> SVT-AV1 worker interrupted',
        'log_stop_request_svt': '[STOP] Stop request -> SVT-AV1 worker interrupted',
        'log_graceful_stop_svt': '[STOP] Stop requested -> SVT-AV1 worker interrupted',
        'log_svt_worker_finished': '### SVT-AV1 WORKER FINISHED ###',
        'log_source_not_found': '[WARN] Error: Source video not found: {path}',
        'log_svt_processing': 'SVT-AV1 PROCESSING: {filename}',
        'log_full_path': 'FULL PATH: {path}',
        'log_reason': 'Reason: {reason}',
        'log_probing_source': '[SCAN] Probing source video with VirtualDub2...',
        'log_probe_failed': '[WARN] Source video probe failed! Fallback to copy.',
        'log_waiting_svt_slot': '[WAIT] Waiting for SVT-AV1 slot...',
        'log_svt_slot_acquired': '[OK] SVT-AV1 slot acquired, starting CRF search...',
        'log_svt_manual_reencode': '[INFO] SVT-AV1 manual re-encode (CRF search skipped): {filename}',
        'log_target_crf': 'Target CRF: {crf}',
        'log_svt_crf_search': '[INFO] SVT-AV1 CRF search starting: {filename}',
        'log_crf_file_check': '[SCAN] CRF search file check (full path): {path}',
        'log_immediate_stop_nvenc': '[STOP] Immediate stop -> NVENC worker interrupted',
        'log_stop_request_nvenc': '[STOP] Stop request -> NVENC worker interrupted',
        'log_graceful_stop_nvenc': '[STOP] Stop requested -> NVENC worker interrupted',
        'log_nvenc_worker_finished': '### NVENC WORKER FINISHED ###',
        'log_nvenc_processing': 'NVENC PROCESSING: {filename}',
        'log_waiting_nvenc_slot': '[WAIT] Waiting for NVENC slot...',
        'log_nvenc_slot_acquired': '[OK] NVENC slot acquired, starting CRF search...',
        'log_nvenc_manual_reencode': '[INFO] NVENC manual re-encode (CRF search skipped): {filename}',
        'log_nvenc_crf_search': '[INFO] NVENC CRF search starting: {filename}',
        'log_vmaf_stopped': '[STOP] VMAF calculation interrupted: {filename}',
        'log_scan_error': 'Scan error: {error}',
        'log_copy_av1_error': '[ERROR] Error copying .av1 file ({filename}): {error}',
        'log_critical_error': 'CRITICAL ERROR in {worker} worker: {error}',
        'log_status_revert_error': 'Error reverting status: {error}',
        'log_error': 'Error: {error}',
        'status_error_with_msg': '[ERROR] Error: {msg}',
        'status_error_copy_failed': '[ERROR] Error (copy failed)',
        'status_source_missing': '[ERROR] Source video missing',
        'log_gui_update_error': '[WARN] Error during GUI update: {error}',
        'log_file_delete_error': '[ERROR] Error deleting file: {error}',
        'summary_failed': 'Failed: {count}',
        # HTTP API messages
        'api_starting': 'Starting encoding...',
        'api_stopping': 'Stopping...',
        'api_immediate_stop': 'Immediate stop...',
        'api_loading': 'Loading videos...',
        'api_settings_updated': 'Settings updated',
        # Additional log messages
        'log_immediate_stop_vmaf_interrupted': '[STOP] Immediate stop -> VMAF calculation interrupted: {filename}',
        'log_vmaf_interrupted': '[STOP] VMAF calculation interrupted: {filename}',
        'log_immediate_stop_encoding': '[STOP] Immediate stop – encoding worker interrupted',
        'log_files_not_exist': 'Files do not exist: reference={ref}, encoded={enc}',
        'log_tree_access_error': '[ERROR] CRITICAL ERROR: Tree access failed in GUI thread: {error}',
        'log_db_update_error': '[WARN] [vmaf_worker] Database update error: {error} | video: {video}',
        'status_initializing': 'Initializing...',
        # Settings change during encoding warning
        'settings_change_during_encoding_title': 'Settings Changed',
        'settings_change_during_encoding_message': 'Encoding is in progress!\n\nThis change will only apply to videos that have not yet started processing.\n\nOK: Apply the change\nCancel: Restore previous value',
        # HTTP Server
        'http_server_enabled': 'HTTP server',
        'http_port': 'Port:',
        'http_server_title': 'HTTP server',
        'http_server_fallback_title': 'HTTP server (Fallback)',
        'http_server_error_title': 'HTTP server error',
        'http_server_started_message': 'HTTP server started!\n\nLocal access: http://127.0.0.1:{port}\nNetwork access: http://<IP>:{port}\n\nOpen it in your browser.',
        'http_server_started_fallback_message': 'HTTP server started!\n(Flask unavailable, fallback mode)\n\nLocal access: http://127.0.0.1:{port}\nNetwork access: http://{local_ip}:{port}\n\nOpen it in your browser.',
        'http_server_started_simple_message': 'HTTP server started!\n\nLocal access: http://127.0.0.1:{port}\n\nOpen it in your browser.',
        'http_server_start_error_message': 'Failed to start the HTTP server!\n\nError: {error}\n\nPort: {port}\nTry another port (for example 8080 or 8000).',
        # Invalid subtitle messages
        'unknown_reason': 'unknown reason',
        'invalid_subtitle_skipped': '[WARN] Invalid subtitle skipped for embedding: {filename} ({reason}). Will be copied as external file.',
        'msg_confirm_reencode_title': 'Confirm Re-encode',
        'msg_confirm_reencode_denoise': 'The selected video has already been encoded.\nChanging the denoise setting will delete the current encoded file and return the video to the queue.\n\nDo you want to proceed*',
        'denoise_multi_header': 'Apply noise reduction to {count} items: {denoise}',
        'denoise_multi_line_pending': '• {count} pending item(s): set only',
        'denoise_multi_line_completed': '• {count} completed item(s): the current encoded file will be DELETED and re-encoded',
        'denoise_multi_line_active': '• {count} actively encoding item(s): will be stopped and requeued with the new setting',
        'denoise_multi_footer': 'Do you want to proceed?',
        'msg_confirm_stop_and_reencode_denoise': '"{filename}" is currently being encoded!\n\nChanging the denoise setting ({denoise}) will STOP the current encoding, delete the partially completed file, and return it to the queue with the new setting.\n\nDo you want to proceed*',
        # Hybrid SMDegrain
        'hybrid_path': 'Hybrid path:',
        'select_hybrid_folder': 'Select Hybrid folder',
        'smdegrain_master_creating': '🔇 SMDegrain denoising in progress (VapourSynth)...',
        'smdegrain_master_created': '[OK] SMDegrain denoising complete',
        'smdegrain_fallback': '[WARN] SMDegrain failed, using vaguedenoiser',
        'smdegrain_cleanup': '[DEL] SMDegrain temporary files deleted',
        'smdegrain_vpy_error': '[WARN] VPY script error: {error}',
        'smdegrain_vspipe_error': '[WARN] VSPipe error: {error}',
        'confirm_exit_title': 'Confirm Exit',
        'confirm_exit_listening': 'Encoding is in progress!\n\nAre you sure you want to exit*',
        # HTTP GUI
        'http_folders': 'Folders',
        'http_encoding_settings': 'Encoding Settings',
        'http_resize': 'Resize',
        'http_save_settings': 'Save Settings',
        'http_total': 'Total',
        'http_in_progress': 'In Progress',
        'http_pending': 'Pending',
        'http_eta': 'ETA',
        'http_loading': 'LOADING...',
        'http_stopping': 'STOPPING...',
        'http_encoding': 'ENCODING',
        'http_idle': 'IDLE',
        'http_confirm_stop_immediate': 'Are you sure you want to stop immediately* Ongoing encodings will be interrupted!',
        'http_settings_saved': 'Settings saved!',
        'http_saved_with_warning': '[WARN] {warning}\n\nSettings saved!',
        'http_load_hint': 'Enter a source folder and click "Load Videos".',
        'http_auto_refresh': 'Auto refresh: 1.5 s',
        'http_loading_status': 'Loading...',
        'http_elapsed': 'Elapsed time:',
        'pending_quality_resume_title': 'Resume VMAF/PSNR calculation',
        'pending_quality_resume_message': '{count} video(s) are waiting for VMAF/PSNR calculation after restart.\n\nDo you want to start the quality check now?',
        'pending_quality_started': '{count} VMAF/PSNR calculation(s) started...',
        'mobile_orig_label': 'Original:',
        'mobile_new_label': 'Encoded:',
        'mobile_savings_label': 'Savings:',
        'http_sum_total': 'Total',
        'http_sum_pending': 'Pending',
        'http_sum_encoding': 'Encoding',
        'http_sum_completed': 'Completed',
        'http_sum_failed': 'Failed',
        'http_sum_orig': 'Original',
        'http_sum_new': 'Encoded',
        'http_sum_savings': 'Savings',
    }
}

def get_default_language():
    """Get the OS default language.
    
    Returns:
        str: 'hu' if Hungarian is detected, 'en' otherwise.
    """
    try:
        # Windows
        if platform.system() == 'Windows':
            import ctypes
            windll = ctypes.windll.kernel32
            lang_id = windll.GetUserDefaultUILanguage()
            # 0x0409 = English, 0x040E = Hungarian
            if lang_id == 0x040E:
                return 'hu'
            else:
                return 'en'
        else:
            # Linux/Mac
            lang = locale.getdefaultlocale()[0]
            if lang and 'hu' in lang.lower():
                return 'hu'
            else:
                return 'en'
    except (OSError, AttributeError, ValueError, TypeError):
        return 'en'

def format_localized_number(value, decimals=1, show_sign=False):
    """Lokalizált szám formázás: magyar = tizedesvessző, angol = tizedespont
    
    Args:
        value: A formázandó szám
        decimals: Tizedesjegyek száma
        show_sign: Ha True, akkor pozitív számoknál is megjelenik a + jel
    """
    if value is None:
        return "-"
    try:
        num_value = float(value)
        if show_sign:
            formatted = f"{num_value:+.{decimals}f}"
        else:
            formatted = f"{num_value:.{decimals}f}"
        if CURRENT_LANGUAGE == 'hu':
            formatted = formatted.replace('.', ',')
        return formatted
    except (TypeError, ValueError):
        return str(value) if value is not None else "-"

def detect_nvidia_gpu():
    """Detect NVIDIA GPU and check for NVENC support (40xx/50xx series).
    
    Checks if an NVIDIA GPU is present using nvidia-smi and verifies
    if it supports NVENC encoding (specifically targeting RTX 40xx/50xx series).
    
    Returns:
        tuple: (bool, str) - (True if supported GPU found, GPU name or None)
    """
    
    def log(msg):
        """Biztonságos log írás"""
        log_writer = _get_log_writer()
        if log_writer:
            try:
                log_writer.write(msg + "\n")
                log_writer.flush()
            except (OSError, IOError, AttributeError):
                pass
    
    log("\n=== NVIDIA GPU DETEKTÁLÁS ===")
    
    try:
        log("  nvidia-smi parancs futtatása...")
        # nvidia-smi parancs futtatása
        result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                              capture_output=True, text=True, timeout=600)
        if result.returncode == 0 and result.stdout.strip():
            gpu_name = result.stdout.strip()
            log(f"  [OK] GPU található: {gpu_name}")
            # Ellenőrizzük, hogy 40xx vagy 50xx sorozatú-e
            # RTX 40xx: "RTX 40" vagy "GeForce RTX 40" vagy "RTX 4090", "RTX 4080", stb.
            # RTX 50xx: "RTX 50" vagy "GeForce RTX 50" vagy "RTX 5090", "RTX 5080", stb.
            gpu_parts = gpu_name.split()
            last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
            if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                # Ellenőrizzük pontosabban: 40xx vagy 50xx
                parts = gpu_name.split()
                for part in parts:
                    if part.startswith('40') and len(part) >= 3:  # 4090, 4080, stb.
                        log(f"  [OK] 40xx sorozat detektálva: {gpu_name}")
                        log("  [OK] NVENC engedélyezve (40xx GPU)\n")
                        return True, gpu_name
                    if part.startswith('50') and len(part) >= 3:  # 5090, 5080, stb.
                        log(f"  [OK] 50xx sorozat detektálva: {gpu_name}")
                        log("  [OK] NVENC engedélyezve (50xx GPU)\n")
                        return True, gpu_name
            log(f"  [ERROR] GPU nem 40xx vagy 50xx sorozatú: {gpu_name}")
            log("  [ERROR] NVENC nincs engedélyezve\n")
            return False, gpu_name
        else:
            log("  [ERROR] nvidia-smi nem adott eredményt")
    except FileNotFoundError:
        log("  [ERROR] nvidia-smi nem található a PATH-ban")
    except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
        log(f"  [ERROR] nvidia-smi hiba: {e}")
    except Exception as e:
        log(f"  [ERROR] nvidia-smi váratlan hiba: {e}")
    
    # Ha nvidia-smi nem elérhető, próbáljuk Windows WMI-vel (opcionális)
    if sys.platform == 'win32':
        try:
            log("  Windows WMI próbálkozás...")
            try:
                import wmi
            except ImportError:
                log("  [ERROR] wmi modul nincs telepítve")
            else:
                log("  [OK] wmi modul elérhető")
                c = wmi.WMI()
                for gpu in c.Win32_VideoController():
                    if 'NVIDIA' in gpu.Name.upper():
                        gpu_name = gpu.Name
                        log(f"  [OK] NVIDIA GPU található: {gpu_name}")
                        # Ellenőrizzük, hogy 40xx vagy 50xx sorozatú-e
                        gpu_parts = gpu_name.split()
                        last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
                        if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                            parts = gpu_name.split()
                            for part in parts:
                                if part.startswith('40') and len(part) >= 3:
                                    log(f"  [OK] 40xx sorozat detektálva: {gpu_name}")
                                    log("  [OK] NVENC engedélyezve (40xx GPU)\n")
                                    return True, gpu_name
                                if part.startswith('50') and len(part) >= 3:
                                    log(f"  [OK] 50xx sorozat detektálva: {gpu_name}")
                                    log("  [OK] NVENC engedélyezve (50xx GPU)\n")
                                    return True, gpu_name
                        log(f"  [ERROR] GPU nem 40xx vagy 50xx sorozatú: {gpu_name}")
                        log("  [ERROR] NVENC nincs engedélyezve\n")
                        return False, gpu_name
        except Exception as e:
            log(f"  [ERROR] WMI hiba: {e}")
    
    log("  [ERROR] NVIDIA GPU nem található vagy nem 40xx/50xx sorozatú")

def find_virtualdub():
    """VirtualDub2 keresése - vdub64.exe vagy vdub2.exe"""
    def log(msg):
        """Biztonságos log írás"""
        log_writer = _get_log_writer()
        if log_writer:
            try:
                log_writer.write(msg + "\n")
                log_writer.flush()
            except (OSError, IOError, AttributeError) as e:
                import sys
                sys.stderr.write(f"Error in log_writer: {e}\n")
    
    log("\n=== VirtualDub2 keresése (vdub64.exe vagy vdub2.exe) ===")
    
    # Először próbáljuk a vdub64.exe-t
    from .core_paths_tools_logging import find_program_in_path
    log("  vdub64.exe keresése...")
    result = find_program_in_path('vdub64.exe')
    if result:
        log(f"  [OK] MEGTALÁLVA: {result}")
        return result
    
    # Ha nem találtuk, próbáljuk a vdub2.exe-t
    log("  vdub64.exe nem található, vdub2.exe keresése...")
    result = find_program_in_path('vdub2.exe')
    if result:
        log(f"  [OK] MEGTALÁLVA: {result}")
        return result
    
    log("  [ERROR] VirtualDub2 nem található (sem vdub64.exe, sem vdub2.exe)\n")
    return None

def auto_detect_programs():
    """Automatically detect external programs (FFmpeg, VirtualDub2, ab-av1).
    
    Searches for required external tools in the PATH and common locations.
    
    Returns:
        dict: Dictionary containing paths for 'ffmpeg', 'virtualdub', and 'abav1'.
    """
    from .core_paths_tools_logging import find_program_in_path
    return {
        'ffmpeg': find_program_in_path('ffmpeg.exe' if sys.platform == 'win32' else 'ffmpeg'),
        'virtualdub': find_virtualdub(),
        'abav1': find_program_in_path('ab-av1.exe' if sys.platform == 'win32' else 'ab-av1'),
    }

def t(key):
    """Translation function.
    
    Args:
        key: Translation key.
        
    Returns:
        str: Translated text or the key itself if not found.
    """
    return TRANSLATIONS.get(CURRENT_LANGUAGE, TRANSLATIONS['en']).get(key, key)

def translate_status(status_text):
    """Localize status text.
    
    Args:
        status_text: The status text to translate.
        
    Returns:
        str: Localized status text.
    """
    if not status_text:
        return status_text
    status_text = from_decorative_text(status_text)
    
    # Státusz fordítások
    status_map = {
        'hu': {
            'NVENC queue-ban vár...': 'status_nvenc_queue',
            'SVT-AV1 queue-ban vár...': 'status_svt_queue',
            '[OK] Kész': 'status_completed',
            '[OK] Kész (NVENC)': 'status_completed_nvenc',
            '[OK] Kész (SVT-AV1)': 'status_completed_svt',
            '[OK] Kész (másolva)': 'status_completed_copy',
            '[OK] Kész (már létezik)': 'status_completed_exists',
            '[ERROR] Sikertelen': 'status_failed',
            '[ERROR] Forrás videó hiányzik': 'status_source_missing',
            '[ERROR] Fájl hiányzik': 'status_file_missing',
            '[ERROR] Betöltési hiba': 'status_load_error',
        'VMAF ellenőrzésre vár...': 'status_vmaf_waiting',
        'PSNR ellenőrzésre vár...': 'status_psnr_waiting',
        'VMAF/PSNR számításra vár...': 'status_vmaf_psnr_waiting',
        'VMAF/PSNR számítás...': 'status_vmaf_calculating',
        'VMAF/PSNR számítás folyamatban...': 'status_vmaf_calculating',  # Backward compatibility
        'VMAF számítás...': 'status_vmaf_only',
        'VMAF számítás folyamatban...': 'status_vmaf_only',  # Backward compatibility
        'PSNR számítás...': 'status_psnr_only',
        'PSNR számítás folyamatban...': 'status_psnr_only',  # Backward compatibility
        '[ERROR] VMAF/PSNR számítás hiba': 'status_vmaf_error',
            '[ERROR] VMAF számítás hiba': 'status_vmaf_error',  # Backward compatibility
            'Hangsáv eltávolításra vár...': 'status_audio_edit_queue',
            'hangsáv eltávolítás...': 'status_audio_editing',
            'Hangsáv eltávolítás folyamatban...': 'status_audio_editing',  # Backward compatibility
            'Zajszűrés folyamatban...': 'status_denoising',  # Backward compatibility
            '[OK] Kész (hangsáv módosítva)': 'status_audio_edit_done',
            '[ERROR] Hangsáv eltávolítás hiba': 'status_audio_edit_failed',
            'sávok mentése...': 'status_track_saving',
            'metaadat újraolvasás...': 'status_metadata_refresh',
            'NVENC kódolás...': 'status_nvenc_encoding',
            'NVENC validálás...': 'status_nvenc_validation',
            'NVENC CRF keresés...': 'status_nvenc_crf_search',
            'SVT-AV1 kódolás...': 'status_svt_encoding',
            'SVT-AV1 validálás...': 'status_svt_validation',
            'SVT-AV1 CRF keresés...': 'status_svt_crf_search',
            '[WARN] Ellenőrizendő': 'status_needs_check',
            '[WARN] Ellenőrizendő (NVENC)': 'status_needs_check_nvenc',
            '[WARN] Ellenőrizendő (SVT)': 'status_needs_check_svt',
        },
        'en': {
            'NVENC queue waiting...': 'status_nvenc_queue',
            'SVT-AV1 queue waiting...': 'status_svt_queue',
            '[OK] Done': 'status_completed',
            '[OK] Done (NVENC)': 'status_completed_nvenc',
            '[OK] Done (SVT-AV1)': 'status_completed_svt',
            '[OK] Done (copied)': 'status_completed_copy',
            '[OK] Done (already exists)': 'status_completed_exists',
            '[ERROR] Failed': 'status_failed',
            '[ERROR] Source video missing': 'status_source_missing',
            '[ERROR] File missing': 'status_file_missing',
            '[ERROR] Load error': 'status_load_error',
        'VMAF check waiting...': 'status_vmaf_waiting',
        'PSNR check waiting...': 'status_psnr_waiting',
        'VMAF/PSNR calculation waiting...': 'status_vmaf_psnr_waiting',
        'VMAF/PSNR calculation...': 'status_vmaf_calculating',
        'VMAF/PSNR calculation in progress...': 'status_vmaf_calculating',  # Backward compatibility
        'VMAF calculation...': 'status_vmaf_only',
        'VMAF calculation in progress...': 'status_vmaf_only',  # Backward compatibility
        'PSNR calculation...': 'status_psnr_only',
        'PSNR calculation in progress...': 'status_psnr_only',  # Backward compatibility
            '[ERROR] VMAF/PSNR calculation error': 'status_vmaf_error',
            '[ERROR] VMAF calculation error': 'status_vmaf_error',  # Backward compatibility
            'Audio track removal queued...': 'status_audio_edit_queue',
            'audio track removal...': 'status_audio_editing',
            'Audio track removal in progress...': 'status_audio_editing',  # Backward compatibility
            'Denoising in progress...': 'status_denoising',  # Backward compatibility
            '[OK] Done (audio updated)': 'status_audio_edit_done',
            '[ERROR] Audio track removal error': 'status_audio_edit_failed',
            'saving tracks...': 'status_track_saving',
            're-reading metadata...': 'status_metadata_refresh',
            'NVENC encoding...': 'status_nvenc_encoding',
            'NVENC validation...': 'status_nvenc_validation',
            'NVENC CRF search...': 'status_nvenc_crf_search',
            'SVT-AV1 encoding...': 'status_svt_encoding',
            'SVT-AV1 validation...': 'status_svt_validation',
            'SVT-AV1 CRF search...': 'status_svt_crf_search',
            '[WARN] Needs Check': 'status_needs_check',
            '[WARN] Needs Check (NVENC)': 'status_needs_check_nvenc',
            '[WARN] Needs Check (SVT)': 'status_needs_check_svt',
        }
    }
    
    # Ellenőrizzük, hogy van-e fordítása
    lang_map = status_map.get(CURRENT_LANGUAGE, {})
    if status_text in lang_map:
        return t(lang_map[status_text])
    
    # Ha tartalmazza a státusz részeket, próbáljuk meg fordítani
    for orig, key in lang_map.items():
        if orig in status_text:
            return status_text.replace(orig, t(key))
    
    return status_text

def normalize_status_to_code(status_text):
    """Normalize status text to a language-independent code for database storage.
    
    Args:
        status_text: Localized status text.
        
    Returns:
        str: Internal status code or None.
    """
    if not status_text:
        return None
    status_text = from_decorative_text(status_text)
    
    # Státusz kódok nyelvfüggetlenül
    # FONTOS: A specifikusabb mintákat előbb kell ellenőrizni, mint az általánosabbakat!
    # A patterns listákon belül is a specifikusabb mintákat előbb kell tenni!
    status_patterns = {
        # Completion markers contain encoder suffixes like "(NVENC)", so
        # needs-check states must be matched before completed_* states.
        'needs_check_nvenc': ['[WARN] Ellenőrizendő (NVENC)', '[WARN] Needs Check (NVENC)'],
        'needs_check_svt': ['[WARN] Ellenőrizendő (SVT)', '[WARN] Needs Check (SVT)'],
        'needs_check': ['[WARN] Ellenőrizendő', '[WARN] Needs Check', 'Ellenőrizendő', 'Needs Check'],
        'completed_nvenc': ['[OK] Kész (NVENC)', '[OK] Done (NVENC)', '(NVENC)'],
        'completed_svt': ['[OK] Kész (SVT-AV1)', '[OK] Done (SVT-AV1)', '(SVT-AV1)'],
        'completed_copy': ['[OK] Kész (másolva)', '[OK] Done (copied)', '(másolva)', '(copied)'],
        'completed_exists': ['[OK] Kész (már létezik)', '[OK] Done (already exists)', '(már létezik)', '(already exists)'],
        'completed': ['[OK] Kész (hangsáv módosítva)', '[OK] Done (audio updated)', '[OK] Kész', '[OK] Done', 'completed'],
        'failed': ['[ERROR] Sikertelen', '[ERROR] Failed', 'Sikertelen', 'Failed', '[ERROR] Hangsáv eltávolítás hiba', '[ERROR] Audio track removal error'],
        'source_missing': ['[ERROR] Forrás videó hiányzik', '[ERROR] Source video missing', 'Forrás videó hiányzik', 'Source video missing'],
        'file_missing': ['[ERROR] Fájl hiányzik', '[ERROR] File missing', 'Fájl hiányzik', 'File missing'],
        'load_error': ['[ERROR] Betöltési hiba', '[ERROR] Load error', 'Betöltési hiba', 'Load error'],
        'nvenc_queue': ['NVENC queue-ban vár', 'NVENC queue waiting', 'NVENC queue', 'NVENC queue-ban vár...', 'NVENC queue waiting...'],
        'svt_queue': ['SVT-AV1 queue-ban vár', 'SVT-AV1 queue waiting', 'SVT-AV1 queue', 'SVT-AV1 queue-ban vár...', 'SVT-AV1 queue waiting...'],
        'vmaf_psnr_waiting': ['VMAF/PSNR számításra vár', 'VMAF/PSNR calculation waiting'],
        'vmaf_waiting': ['VMAF számításra vár', 'VMAF calculation waiting', 'VMAF ellenőrzésre vár', 'VMAF check waiting'],
        'psnr_waiting': ['PSNR számításra vár', 'PSNR calculation waiting', 'PSNR ellenőrzésre vár', 'PSNR check waiting'],
        # NOTE: Order matters! More specific patterns must come first for correct matching
        'vmaf_error': ['[ERROR] VMAF/PSNR számítás hiba', '[ERROR] VMAF/PSNR calculation error', '[ERROR] VMAF számítás hiba', '[ERROR] VMAF calculation error'],
        'vmaf_calculating': ['VMAF/PSNR számítás', 'VMAF/PSNR calculation'],
        'psnr_only': ['PSNR számítás', 'PSNR calculation'],
        'vmaf_only': ['VMAF számítás', 'VMAF calculation'],
        'audio_edit_queue': ['Hangsáv eltávolításra vár', 'Audio track removal queued'],
        'audio_editing': ['hangsáv eltávolítás', 'audio track removal'],
        'track_saving': ['sávok mentése', 'saving tracks'],
        'metadata_refresh': ['metaadat újraolvasás', 're-reading metadata'],
        'nvenc_encoding': ['NVENC kódolás...', 'NVENC encoding...', 'NVENC kódolás', 'NVENC encoding'],
        'nvenc_validation': ['NVENC validálás...', 'NVENC validation...', 'NVENC validálás', 'NVENC validation'],
        'nvenc_crf_search': ['NVENC CRF keresés', 'NVENC CRF search', 'NVENC CRF keresés (VMAF:', 'NVENC CRF search (VMAF:', 'NVENC CRF keresés (VMAF fallback:', 'NVENC CRF search (VMAF fallback:'],
        'svt_encoding': ['SVT-AV1 kódolás...', 'SVT-AV1 encoding...', 'SVT-AV1 kódolás', 'SVT-AV1 encoding'],
        'svt_validation': ['SVT-AV1 validálás...', 'SVT-AV1 validation...', 'SVT-AV1 validálás', 'SVT-AV1 validation'],
        'svt_crf_search': ['SVT-AV1 CRF keresés', 'SVT-AV1 CRF search', 'SVT-AV1 CRF keresés (VMAF:', 'SVT-AV1 CRF search (VMAF:', 'SVT-AV1 CRF keresés (VMAF fallback:', 'SVT-AV1 CRF search (VMAF fallback:'],
        'denoising': ['zajszűrés', 'denoising'],
        'rebuild_queued': ['Újjáépítésre jelölve', 'Queued for rebuild'],
        'rebuild_in_progress': ['Újjáépítés folyamatban', 'Rebuilding'],
    }
    for code, patterns in status_patterns.items():
        for pattern in patterns:
            if pattern in status_text:
                return code
    
    return None
def format_size_mb(size_bytes):
    """Format size from bytes to MB.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted size in MB (e.g., "123.4 MB") or "-" if None.
    """
    if size_bytes is None:
        return "-"
    try:
        size_mb = size_bytes / (1024 ** 2)
        size_str = format_localized_number(size_mb, decimals=1)
        return f"{size_str} MB"
    except (TypeError, ValueError):
        return "-"

def format_size_auto(size_bytes):
    """Format size from bytes to automatically selected unit (MB/GB/TB).
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted size string (e.g., "1.5 GB").
    """
    if size_bytes is None:
        return "-"
    try:
        # TB (1024^4 bytes)
        if size_bytes >= (1024 ** 4):
            size_tb = size_bytes / (1024 ** 4)
            size_str = format_localized_number(size_tb, decimals=2)
            return f"{size_str} TB"
        # GB (1024^3 bytes)
        elif size_bytes >= (1024 ** 3):
            size_gb = size_bytes / (1024 ** 3)
            size_str = format_localized_number(size_gb, decimals=2)
            return f"{size_str} GB"
        # MB (1024^2 bytes)
        elif size_bytes >= (1024 ** 2):
            size_mb = size_bytes / (1024 ** 2)
            size_str = format_localized_number(size_mb, decimals=1)
            return f"{size_str} MB"
        # KB (1024 bytes)
        elif size_bytes >= 1024:
            size_kb = size_bytes / 1024
            size_str = format_localized_number(size_kb, decimals=1)
            return f"{size_str} KB"
        else:
            return f"{int(size_bytes)} B"
    except (TypeError, ValueError):
        return "-"


def normalize_number_string(num_str):
    """Convert localized number string to language-independent (English) format for DB storage.
    
    Args:
        num_str: Localized number string (e.g., "1,5").
        
    Returns:
        str: Normalized number string (e.g., "1.5").
    """
    if not num_str or num_str == "-":
        return num_str
    try:
        # Tizedesvesszőt tizedespontra cseréljük
        normalized = str(num_str).replace(',', '.')
        # Ellenőrizzük, hogy szám-e (opcionális: validálás)
        float(normalized)
        return normalized
    except (ValueError, TypeError):
        # Ha nem szám, visszaadjuk az eredeti értéket
        return num_str

def parse_size_to_bytes(size_str):
    """Parse size string to bytes, handling KB, MB, GB, TB units.
    
    Handles both localized (comma as decimal separator) and non-localized (dot) formats.
    
    Args:
        size_str: Size string (e.g., "1,5 GB", "1.5 GB", "500 MB", "1.2 TB").
        
    Returns:
        int: Size in bytes or None if parsing fails.
    """
    import re
    
    if not size_str or size_str == "-":
        return None
    try:
        # Remove spaces and convert to uppercase for easier matching
        clean = size_str.strip().upper()
        
        # Try to extract number and unit
        # Handle localized format (comma as decimal separator)
        clean_normalized = clean.replace(',', '.')
        
        # Match patterns like "123.45 MB", "1,234.56 GB", etc.
        match = re.match(r'([\d.,]+)\s*(KB|MB|GB|TB|B)', clean_normalized)
        if match:
            number_str = match.group(1).replace(',', '')
            unit = match.group(2).upper()
            number = float(number_str)
            
            if unit == 'TB':
                return int(number * (1024 ** 4))
            elif unit == 'GB':
                return int(number * (1024 ** 3))
            elif unit == 'MB':
                return int(number * (1024 ** 2))
            elif unit == 'KB':
                return int(number * 1024)
            elif unit == 'B':
                return int(number)
        
        # Fallback: try to parse as MB (old format)
        clean = clean.replace("MB", "").replace("GB", "").replace("TB", "").replace("KB", "").replace("B", "").strip()
        clean = clean.replace(',', '.')
        value = float(clean)
        # Assume MB if no unit found
        return int(value * (1024 ** 2))
    except (ValueError, TypeError, AttributeError):
        return None


def batch_scan_directory(directory, progress_callback=None):
    """Batch scan directory for all files with size and mtime.
    
    Uses Everything SDK for ultra-fast MFT-based search when available,
    falls back to os.scandir() for efficient directory traversal.
    
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
    # Try Everything SDK first (ultra-fast MFT-based search)
    try:
        from .everything_sdk import everything_scan_directory, is_everything_available
        if is_everything_available():
            result = everything_scan_directory(directory, extensions=None, progress_callback=progress_callback)
            if result is not None:
                return result
            # If Everything returned None, fall through to os.scandir()
    except ImportError:
        pass  # everything_sdk not available, use fallback
    except Exception as e:
        print(f"[batch_scan_directory] Everything SDK error: {e}, falling back to os.scandir()")
    
    # Fallback: os.scandir() based scanning
    scan_results = {}
    # Counter for progress callback
    scan_counter = [0]
    
    def scan_dir_recursive(dir_path):
        """Recursive helper for scanning."""
        try:
            with os.scandir(dir_path) as entries:
                for entry in entries:
                    try:
                        # is_file() and is_dir() are also cached - no extra I/O!
                        if entry.is_file(follow_symlinks=False):
                            # entry.stat() reuses already cached stat info - very fast!
                            stat_info = entry.stat()
                            scan_results[Path(entry.path)] = {
                                'size': stat_info.st_size,
                                'mtime': stat_info.st_mtime
                            }
                            scan_counter[0] += 1
                            # Call progress callback every 500 files
                            if progress_callback and scan_counter[0] % 500 == 0:
                                progress_callback(scan_counter[0])
                        elif entry.is_dir(follow_symlinks=False):
                            # Recursive scan for subdirectories
                            try:
                                scan_dir_recursive(Path(entry.path))
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
            progress_callback(scan_counter[0])
            
    except (OSError, PermissionError):
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


def status_code_to_localized(code):
    """Translate status code to localized text.
    
    Args:
        code: Internal status code.
        
    Returns:
        str: Localized status text.
    """
    if not code:
        return t('status_nvenc_queue')  # Default
    
    code_map = {
        'completed': 'status_completed',
        'completed_nvenc': 'status_completed_nvenc',
        'completed_svt': 'status_completed_svt',
        'completed_copy': 'status_completed_copy',
        'completed_exists': 'status_completed_exists',
        'failed': 'status_failed',
        'source_missing': 'status_source_missing',
        'file_missing': 'status_file_missing',
        'load_error': 'status_load_error',
        'nvenc_queue': 'status_nvenc_queue',
        'svt_queue': 'status_svt_queue',
        'vmaf_waiting': 'status_vmaf_waiting',
        'psnr_waiting': 'status_psnr_waiting',
        'vmaf_psnr_waiting': 'status_vmaf_psnr_waiting',
        'vmaf_calculating': 'status_vmaf_calculating',
        'vmaf_only': 'status_vmaf_only',
        'psnr_only': 'status_psnr_only',
        'vmaf_error': 'status_vmaf_error',
        'nvenc_encoding': 'status_nvenc_encoding',
        'nvenc_validation': 'status_nvenc_validation',
        'nvenc_crf_search': 'status_nvenc_crf_search',
        'svt_encoding': 'status_svt_encoding',
        'svt_validation': 'status_svt_validation',
        'svt_crf_search': 'status_svt_crf_search',
        'needs_check': 'status_needs_check',
        'needs_check_nvenc': 'status_needs_check_nvenc',
        'needs_check_svt': 'status_needs_check_svt',
        'audio_edit_queue': 'status_audio_edit_queue',
        'audio_editing': 'status_audio_editing',
        'track_saving': 'status_track_saving',
        'metadata_refresh': 'status_metadata_refresh',
        'denoising': 'status_denoising',
        'rebuild_queued': 'status_rebuild_queued',
        'rebuild_in_progress': 'status_rebuild_in_progress',
    }
    
    return t(code_map.get(code, 'status_nvenc_queue'))

def is_status_completed(status_text):
    """Check if the status indicates completion (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is completed.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists')

def is_status_failed(status_text):
    """Check if the status indicates failure (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is failed.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('failed', 'source_missing', 'file_missing', 'vmaf_error', 'load_error')

def is_status_queue(status_text):
    """Check if the status indicates waiting in queue (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is queued.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('nvenc_queue', 'svt_queue', 'vmaf_waiting', 'psnr_waiting', 'vmaf_psnr_waiting', 'audio_edit_queue')

def is_status_rebuild(status_text):
    """Check if the status is rebuild-related (queued or in progress)."""
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('rebuild_queued', 'rebuild_in_progress')

def is_status_rebuild_queued(status_text):
    """Check if the status is specifically rebuild queued."""
    if not status_text:
        return False
    return normalize_status_to_code(status_text) == 'rebuild_queued'

def is_status_needs_check(status_text):
    """Check if the status indicates needs check (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status needs check.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('needs_check', 'needs_check_nvenc', 'needs_check_svt')

def get_completed_status_for_encoder(encoder_name):
    """Get localized 'Completed' status based on encoder name.
    
    Args:
        encoder_name: Name of the encoder (e.g., 'NVENC', 'SVT').
        
    Returns:
        str: Localized completed status text.
    """
    if "SVT" in encoder_name or "svt" in encoder_name.lower():
        return t('status_completed_svt')
    elif "NVENC" in encoder_name:
        return t('status_completed_nvenc')
    else:
        return t('status_completed')

# Nyelvkód mapping
LANGUAGE_MAP = {
    'en': 'eng', 'hu': 'hun', 'de': 'ger', 'fr': 'fre', 'es': 'spa', 'it': 'ita',
    'pt': 'por', 'ru': 'rus', 'ja': 'jpn', 'ko': 'kor', 'zh': 'chi', 'ar': 'ara',
    'pl': 'pol', 'nl': 'dut', 'sv': 'swe', 'no': 'nor', 'da': 'dan', 'fi': 'fin',
    'cs': 'cze', 'sk': 'slo', 'ro': 'rum', 'tr': 'tur', 'el': 'gre', 'he': 'heb',
    'hi': 'hin', 'th': 'tha', 'vi': 'vie', 'uk': 'ukr', 'bg': 'bul', 'hr': 'hrv', 'sr': 'srp',
    'eng': 'eng', 'hun': 'hun', 'ger': 'ger', 'deu': 'ger', 'fra': 'fre', 'fre': 'fre',
    'esp': 'spa', 'spa': 'spa', 'ita': 'ita', 'por': 'por', 'rus': 'rus', 'jpn': 'jpn',
    'kor': 'kor', 'chi': 'chi', 'zho': 'chi', 'ara': 'ara', 'pol': 'pol', 'nld': 'dut',
    'dut': 'dut', 'swe': 'swe', 'nor': 'nor', 'dan': 'dan', 'fin': 'fin', 'ces': 'cze',
    'cze': 'cze', 'slk': 'slo', 'slo': 'slo', 'ron': 'rum', 'rum': 'rum', 'tur': 'tur',
    'ell': 'gre', 'gre': 'gre', 'heb': 'heb', 'hin': 'hin', 'tha': 'tha', 'vie': 'vie',
    'ukr': 'ukr', 'bul': 'bul', 'hrv': 'hrv', 'srp': 'srp',
}
