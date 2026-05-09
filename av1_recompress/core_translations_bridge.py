import locale

TRANSLATIONS = {
    'en': {
        # Main window
        'window_title': "AV1 Batch Converter (NVENC/SVT-AV1) - by Kornél",
        'tab_files': "Files",
        'tab_console_nvenc': "Console (NVENC)",
        'tab_console_svt': "Console (SVT-AV1)",
        
        # File list columns
        'col_file': "File",
        'col_size': "Size",
        'col_status': "Status",
        'col_progress': "Progress",
        'col_cq': "CQ/CRF",
        'col_vmaf': "VMAF",
        'col_psnr': "PSNR",
        'col_new_size': "New Size",
        'col_settings': "Settings",
        
        # Buttons
        'btn_add_files': "Add Files",
        'btn_add_folder': "Add Folder",
        'btn_clear_completed': "Clear Completed",
        'btn_start': "START ENCODING",
        'btn_stop': "STOP",
        'btn_stop_graceful': "Stop (Finish current)",
        'btn_stop_immediate': "STOP IMMEDIATELY",
        
        # Settings groups
        'group_encoding': "Encoding Settings",
        'group_audio': "Audio Settings",
        'group_workers': "Worker Settings",
        'group_advanced': "Advanced Settings",
        
        # Encoding settings
        'lbl_min_vmaf': "Min VMAF:",
        'lbl_vmaf_step': "VMAF Step:",
        'lbl_max_size': "Max Size %:",
        'chk_resize': "Resize to 1080p",
        
        # Audio settings
        'chk_audio_compress': "Compress Audio (AAC 192k)",
        'chk_auto_stereo': "Auto 5.1 -> Stereo",
        'lbl_audio_method': "Method:",
        'opt_fast': "Fast (Pan)",
        'opt_dialogue': "Dialogue Focus",
        
        # Worker settings
        'lbl_nvenc_workers': "NVENC Workers:",
        'chk_nvenc_enabled': "Enable NVENC (GPU)",
        'chk_svt_enabled': "Enable SVT-AV1 (CPU)",
        'lbl_svt_preset': "SVT Preset:",
        
        # Advanced settings
        'chk_auto_vmaf': "Measure VMAF/PSNR",
        'chk_use_abav1': "Use ab-av1 for VMAF",
        'chk_hide_completed': "Hide Completed",
        'chk_autoscroll': "Auto Scroll Console",
        'lbl_language': "Language:",
        
        # Status messages
        'status_pending': "Pending",
        'status_analyzing': "Analyzing...",
        'status_encoding': "Encoding...",
        'status_completed': "Completed",
        'status_completed_nvenc': "Completed (NVENC)",
        'status_completed_svt': "Completed (SVT)",
        'status_completed_copy': "Completed (Copy)",
        'status_completed_exists': "Completed (Exists)",
        'status_failed': "Failed",
        'status_source_missing': "Source Missing",
        'status_file_missing': "File Missing",
        'status_load_error': "Load Error",
        'status_nvenc_queue': "Queued (NVENC)",
        'status_svt_queue': "Queued (SVT)",
        'status_vmaf_waiting': "Waiting for VMAF",
        'status_psnr_waiting': "Waiting for PSNR",
        'status_vmaf_psnr_waiting': "Waiting for VMAF/PSNR",
        'status_vmaf_calculating': "Calculating VMAF...",
        'status_vmaf_error': "VMAF Error",
        'status_nvenc_encoding': "Encoding (NVENC)...",
        'status_nvenc_validation': "Validating (NVENC)...",
        'status_nvenc_crf_search': "CRF Search (NVENC)...",
        'status_svt_encoding': "Encoding (SVT)...",
        'status_svt_validation': "Validating (SVT)...",
        'status_svt_crf_search': "CRF Search (SVT)...",
        'status_needs_check': "Check Needed",
        'status_needs_check_nvenc': "Check Needed (NVENC)",
        'status_needs_check_svt': "Check Needed (SVT)",
        'status_audio_edit_queue': "Queued (Audio)",
        'status_audio_editing': "Editing Audio...",
        
        # Messages
        'msg_confirm_exit': "Encoding is in progress. Are you sure you want to exit*",
        'msg_error': "Error",
        'msg_warning': "Warning",
        'msg_info': "Information",
        'msg_no_files': "No files selected!",
        'msg_load_error': "Error loading files",
        'msg_save_error': "Error saving settings",
        'msg_db_error': "Database Error",
        'msg_missing_tools': "Missing Tools",
        'msg_install_ffmpeg': "Please install FFmpeg and make sure it's in PATH.",
        
        # Tooltips
        'tip_min_vmaf': "Target VMAF score (0-100). Higher is better quality.",
        'tip_vmaf_step': "How much to adjust CQ if VMAF is too low.",
        'tip_max_size': "Maximum allowed file size as % of original.",
        'tip_resize': "Resize video to 1080p height (maintaining aspect ratio).",
        'tip_audio_compress': "Compress audio to AAC 192kbps to save space.",
        'tip_auto_stereo': "Convert 5.1 audio to Stereo (2.0).",
        'tip_nvenc_workers': "Number of parallel NVENC encoding tasks.",
        'tip_svt_preset': "SVT-AV1 preset (0-13). Lower is slower but better compression.",
        'tip_auto_vmaf': "Calculate VMAF and PSNR scores after encoding.",
        
        # Context menu
        'menu_open_source': "Open Source File",
        'menu_open_folder': "Open Source Folder",
        'menu_play_source': "Play Source (VLC)",
        'menu_play_output': "Play Output (VLC)",
        'menu_reencode_nvenc': "Re-encode (NVENC)",
        'menu_reencode_svt': "Re-encode (SVT-AV1)",
        'menu_measure_vmaf': "Measure VMAF/PSNR",
        'menu_reset_status': "Reset Status",
        'menu_remove_item': "Remove from List",
        'menu_copy_source': "Copy Source Path",
        'menu_copy_output': "Copy Output Path",
        'menu_remove_audio': "Remove Audio Track...",
        'menu_convert_audio': "Convert 5.1 to Stereo...",
    },
    'hu': {
        # Main window
        'window_title': "AV1 Batch Konvertáló (NVENC/SVT-AV1) - Készítette: Kornél",
        'tab_files': "Fájlok",
        'tab_console_nvenc': "Konzol (NVENC)",
        'tab_console_svt': "Konzol (SVT-AV1)",
        
        # File list columns
        'col_file': "Fájl",
        'col_size': "Méret",
        'col_status': "Állapot",
        'col_progress': "Haladás",
        'col_cq': "CQ/CRF",
        'col_vmaf': "VMAF",
        'col_psnr': "PSNR",
        'col_new_size': "Új méret",
        'col_settings': "Beállítások",
        
        # Buttons
        'btn_add_files': "Fájlok Hozzáadása",
        'btn_add_folder': "Mappa Hozzáadása",
        'btn_clear_completed': "Készek Törlése",
        'btn_start': "KÓDOLÁS INDÍTÁSA",
        'btn_stop': "LEÁLLÍTÁS",
        'btn_stop_graceful': "Leállítás (Befejezéssel)",
        'btn_stop_immediate': "AZONNALI LEÁLLÍTÁS",
        
        # Settings groups
        'group_encoding': "Kódolási Beállítások",
        'group_audio': "Hang Beállítások",
        'group_workers': "Worker Beállítások",
        'group_advanced': "Haladó Beállítások",
        
        # Encoding settings
        'lbl_min_vmaf': "Min VMAF:",
        'lbl_vmaf_step': "VMAF Lépés:",
        'lbl_max_size': "Max Méret %:",
        'chk_resize': "Átméretezés 1080p-re",
        
        # Audio settings
        'chk_audio_compress': "Hang Tömörítés (AAC 192k)",
        'chk_auto_stereo': "Auto 5.1 -> Sztereó",
        'lbl_audio_method': "Módszer:",
        'opt_fast': "Gyors (Pan)",
        'opt_dialogue': "Beszéd Fókusz",
        
        # Worker settings
        'lbl_nvenc_workers': "NVENC Workerek:",
        'chk_nvenc_enabled': "NVENC Engedélyezése (GPU)",
        'chk_svt_enabled': "SVT-AV1 Engedélyezése (CPU)",
        'lbl_svt_preset': "SVT Preset:",
        
        # Advanced settings
        'chk_auto_vmaf': "VMAF/PSNR Mérése",
        'chk_use_abav1': "ab-av1 használata VMAF-hez",
        'chk_hide_completed': "Készek Elrejtése",
        'chk_autoscroll': "Konzol Auto-Görgetés",
        'lbl_language': "Nyelv:",
        
        # Status messages
        'status_pending': "Várakozik",
        'status_analyzing': "Elemzés...",
        'status_encoding': "Kódolás...",
        'status_completed': "Kész",
        'status_completed_nvenc': "Kész (NVENC)",
        'status_completed_svt': "Kész (SVT)",
        'status_completed_copy': "Kész (Másolva)",
        'status_completed_exists': "Kész (Létezett)",
        'status_failed': "Hiba",
        'status_source_missing': "Forrás Hiányzik",
        'status_file_missing': "Fájl Hiányzik",
        'status_load_error': "Betöltési Hiba",
        'status_nvenc_queue': "Sorban (NVENC)",
        'status_svt_queue': "Sorban (SVT)",
        'status_vmaf_waiting': "VMAF-re vár",
        'status_psnr_waiting': "PSNR-re vár",
        'status_vmaf_psnr_waiting': "VMAF/PSNR-re vár",
        'status_vmaf_calculating': "VMAF Számítás...",
        'status_vmaf_error': "VMAF Hiba",
        'status_nvenc_encoding': "Kódolás (NVENC)...",
        'status_nvenc_validation': "Validálás (NVENC)...",
        'status_nvenc_crf_search': "CRF Keresés (NVENC)...",
        'status_svt_encoding': "Kódolás (SVT)...",
        'status_svt_validation': "Validálás (SVT)...",
        'status_svt_crf_search': "CRF Keresés (SVT)...",
        'status_needs_check': "Ellenőrizendő",
        'status_needs_check_nvenc': "Ellenőrizendő (NVENC)",
        'status_needs_check_svt': "Ellenőrizendő (SVT)",
        'status_audio_edit_queue': "Sorban (Hang)",
        'status_audio_editing': "Hang Szerkesztés...",
        
        # Messages
        'msg_confirm_exit': "A kódolás folyamatban van. Biztosan ki akarsz lépni*",
        'msg_error': "Hiba",
        'msg_warning': "Figyelmeztetés",
        'msg_info': "Információ",
        'msg_no_files': "Nincs fájl kiválasztva!",
        'msg_load_error': "Hiba a fájlok betöltésekor",
        'msg_save_error': "Hiba a beállítások mentésekor",
        'msg_db_error': "Adatbázis Hiba",
        'msg_missing_tools': "Hiányzó Eszközök",
        'msg_install_ffmpeg': "Kérlek telepítsd az FFmpeg-et és add hozzá a PATH-hoz.",
        
        # Tooltips
        'tip_min_vmaf': "Cél VMAF pontszám (0-100). A magasabb jobb minőséget jelent.",
        'tip_vmaf_step': "Mennyivel állítsa a CQ-t, ha a VMAF túl alacsony.",
        'tip_max_size': "Maximális engedélyezett fájlméret az eredeti %-ában.",
        'tip_resize': "Videó átméretezése 1080p magasságra (képarány megtartásával).",
        'tip_audio_compress': "Hang tömörítése AAC 192kbps formátumba helytakarékosságért.",
        'tip_auto_stereo': "5.1 hang konvertálása Sztereóra (2.0).",
        'tip_nvenc_workers': "Párhuzamos NVENC kódolási feladatok száma.",
        'tip_svt_preset': "SVT-AV1 preset (0-13). Az alacsonyabb lassabb, de jobb tömörítést ad.",
        'tip_auto_vmaf': "VMAF és PSNR értékek számítása kódolás után.",
        
        # Context menu
        'menu_open_source': "Forrás Fájl Megnyitása",
        'menu_open_folder': "Forrás Mappa Megnyitása",
        'menu_play_source': "Forrás Lejátszása (VLC)",
        'menu_play_output': "Kimenet Lejátszása (VLC)",
        'menu_reencode_nvenc': "Újrakódolás (NVENC)",
        'menu_reencode_svt': "Újrakódolás (SVT-AV1)",
        'menu_measure_vmaf': "VMAF/PSNR Mérése",
        'menu_reset_status': "Státusz Visszaállítása",
        'menu_remove_item': "Eltávolítás a Listából",
        'menu_copy_source': "Forrás Útvonal Másolása",
        'menu_copy_output': "Kimenet Útvonal Másolása",
        'menu_remove_audio': "Hangsáv Eltávolítása...",
        'menu_convert_audio': "5.1 -> Sztereó Konvertálás...",
    }
}

def get_default_language():
    """Get system default language code.
    
    Returns:
        str: 'hu' if system locale is Hungarian, else 'en'.
    """
    try:
        lang, _ = locale.getdefaultlocale()
        if lang and lang.startswith('hu'):
            return 'hu'
    except Exception:
        # locale.getdefaultlocale() might fail in some environments
        pass
    return 'en'

def format_localized_number(value, decimals=2):
    """Format number according to current language settings.
    
    Args:
        value: Number to format.
        decimals: Number of decimal places.
        
    Returns:
        str: Formatted number string (e.g., "1,50" for 'hu', "1.50" for 'en').
    """
    if value is None:
        return "-"
    try:
        fmt = f"{{:.{decimals}f}}"
        formatted = fmt.format(float(value))
        if CURRENT_LANGUAGE == 'hu':
            return formatted.replace('.', ',')
        return formatted
    except (ValueError, TypeError):
        return str(value)

def t(key):
    """Translate key to current language."""
    lang_dict = TRANSLATIONS.get(CURRENT_LANGUAGE, TRANSLATIONS['en'])
    return lang_dict.get(key, TRANSLATIONS['en'].get(key, key))

def translate_status(status_text):
    """Translate status text to current language.
    
    Handles both raw status codes and already localized strings.
    
    Args:
        status_text: Status text to translate.
        
    Returns:
        str: Translated status text.
    """
    if not status_text:
        return ""
        
    # If status_text is a key in TRANSLATIONS
    if status_text in TRANSLATIONS['en']:
        return t(status_text)
        
    # If status_text is already localized, try to find the key
    # This might be slow but necessary if we receive already translated text
    for lang in TRANSLATIONS:
        for key, value in TRANSLATIONS[lang].items():
            if value == status_text:
                return t(key)
                
    # Special cases (dynamic statuses)
    if "VMAF:" in status_text:
        return status_text
        
    return status_text

def normalize_status_to_code(status_text):
    """Convert localized status text to internal status code.
    
    Args:
        status_text: Localized status text.
        
    Returns:
        str: Internal status code (e.g., 'completed', 'failed').
    """
    if not status_text:
        return 'nvenc_queue'
        
    # If already a code
    if status_text in TRANSLATIONS['en']:
        # Check if it is a status code (status_ prefix)
        if status_text.startswith('status_'):
            return status_text.replace('status_', '')
            
    # Search in translations
    for lang in TRANSLATIONS:
        for key, value in TRANSLATIONS[lang].items():
            if value == status_text and key.startswith('status_'):
                return key.replace('status_', '')
                
    return 'nvenc_queue'  # Default

def format_size_mb(size_bytes):
    """Format size in bytes to MB string.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted string (e.g., "100.5 MB").
    """
    if size_bytes is None:
        return "-"
    try:
        mb = size_bytes / (1024 * 1024)
        return f"{format_localized_number(mb, 1)} MB"
    except (TypeError, ValueError):
        return "-"

def format_size_auto(size_bytes):
    """Format size in bytes to appropriate unit (B, KB, MB, GB, TB).
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted string with unit.
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
        # Replace decimal comma with dot
        normalized = str(num_str).replace(',', '.')
        # Check if it is a number (optional: validation)
        float(normalized)
        return normalized
    except (ValueError, TypeError):
        # If not a number, return original value
        return num_str

def parse_size_to_bytes(size_str):
    """Parse size string to bytes, handling both localized (comma) and non-localized (dot) formats.
    
    Args:
        size_str: Size string (e.g., "1,5 GB" or "1.5 GB").
        
    Returns:
        int: Size in bytes or None if parsing fails.
    """
    try:
        if not size_str or size_str == "-":
            return None
        clean = size_str.replace("MB", "").replace("mb", "").strip()
        # Handle localized format (comma as decimal separator)
        clean = clean.replace(',', '.')
        value = float(clean)
        return int(value * (1024 ** 2))
    except (ValueError, TypeError):
        return None

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

# Language code mapping
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
