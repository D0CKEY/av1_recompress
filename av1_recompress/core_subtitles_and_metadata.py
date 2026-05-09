import os
import re
import shutil
import subprocess
from pathlib import Path

from .core_preamble_and_imports import SUBTITLE_EXTENSIONS, FFMPEG_PATH
from .core_paths_tools_logging import get_startup_info

# Language map for normalization
LANGUAGE_MAP = {
    'hu': 'hu', 'hun': 'hu', 'hungarian': 'hu',
    'en': 'en', 'eng': 'en', 'english': 'en',
    'de': 'de', 'deu': 'de', 'ger': 'de', 'german': 'de',
    'fr': 'fr', 'fra': 'fr', 'fre': 'fr', 'french': 'fr',
    'es': 'es', 'spa': 'es', 'spanish': 'es',
    'it': 'it', 'ita': 'it', 'italian': 'it',
    'ru': 'ru', 'rus': 'ru', 'russian': 'ru',
    'ja': 'ja', 'jpn': 'ja', 'japanese': 'ja',
    'zh': 'zh', 'zho': 'zh', 'chi': 'zh', 'chinese': 'zh',
    'ko': 'ko', 'kor': 'ko', 'korean': 'ko',
    'pt': 'pt', 'por': 'pt', 'portuguese': 'pt',
    'nl': 'nl', 'nld': 'nl', 'dut': 'nl', 'dutch': 'nl',
    'pl': 'pl', 'pol': 'pl', 'polish': 'pl',
    'sv': 'sv', 'swe': 'sv', 'swedish': 'sv',
    'da': 'da', 'dan': 'da', 'danish': 'da',
    'fi': 'fi', 'fin': 'fi', 'finnish': 'fi',
    'no': 'no', 'nor': 'no', 'norwegian': 'no',
    'tr': 'tr', 'tur': 'tr', 'turkish': 'tr',
    'cs': 'cs', 'ces': 'cs', 'cze': 'cs', 'czech': 'cs',
    'sk': 'sk', 'slk': 'sk', 'slo': 'sk', 'slovak': 'sk',
    'ro': 'ro', 'ron': 'ro', 'rum': 'ro', 'romanian': 'ro',
    'bg': 'bg', 'bul': 'bg', 'bulgarian': 'bg',
    'hr': 'hr', 'hrv': 'hr', 'croatian': 'hr',
    'sr': 'sr', 'srp': 'sr', 'serbian': 'sr',
    'sl': 'sl', 'slv': 'sl', 'slovenian': 'sl',
    'uk': 'uk', 'ukr': 'uk', 'ukrainian': 'uk',
    'el': 'el', 'ell': 'el', 'greek': 'el',
    'he': 'he', 'heb': 'he', 'hebrew': 'he',
    'ar': 'ar', 'ara': 'ar', 'arabic': 'ar',
    'hi': 'hi', 'hin': 'hi', 'hindi': 'hi',
    'th': 'th', 'tha': 'th', 'thai': 'th',
    'vi': 'vi', 'vie': 'vi', 'vietnamese': 'vi',
    'id': 'id', 'ind': 'id', 'indonesian': 'id',
}

def extract_language_from_filename(filename):
    """Extract language code from filename using common patterns.
    
    Args:
        filename: Filename string.
        
    Returns:
        tuple: (base_name, language_code) or (filename, None) if not found.
    """
    basename = os.path.basename(filename)
    name_parts = basename.rsplit('.', 1)
    name_without_ext = name_parts[0] if len(name_parts) > 1 else basename
    
    patterns = [
        (r'^(.+?)[-]([a-z]{2}-[A-Z]{2})$', '-'),
        (r'^(.+?)[.]([a-z]{2}-[A-Z]{2})$', '.'),
        (r'^(.+?)[-]([A-Za-z]{2,})$', '-'),
        (r'^(.+?)[_]([A-Za-z]{2,})$', '_'),
        (r'^(.+?)[.]([A-Za-z]{2,3})$', '.'),
        (r'^(.+?)\s+([A-Za-z]{2,})\s*$', ' '),
    ]
    
    for pattern, separator in patterns:
        match = re.match(pattern, name_without_ext, re.IGNORECASE)
        if match:
            base_name = match.group(1).strip()
            lang_part = match.group(2).strip()
            lang_normalized = lang_part.lower()
            if '-' in lang_normalized:
                lang_normalized = lang_normalized.split('-')[0]
            if lang_normalized in LANGUAGE_MAP:
                return (base_name, lang_part)
    return (name_without_ext, None)

def normalize_language_code(lang_string):
    """Normalize language string to ISO 639-1 code if possible.

    Args:
        lang_string: Language string (e.g., 'eng', 'hu', 'en-US').

    Returns:
        str: Normalized language code (e.g., 'en') or 'und' if unknown.
    """
    if not lang_string:
        return 'und'
    lang_clean = lang_string.strip().lower()
    if '-' in lang_clean:
        lang_clean = lang_clean.split('-')[0]
    if lang_clean in LANGUAGE_MAP:
        return LANGUAGE_MAP[lang_clean]
    return 'und'


def detect_subtitle_encoding(file_path, language_hint=None):
    """Detect the character encoding of a subtitle file.

    Uses BOM detection, UTF-8 validation, language-based hints, and heuristic
    byte pattern analysis to determine the most likely encoding. Supports a
    wide range of encodings commonly found in subtitle files worldwide.

    Args:
        file_path: Path to the subtitle file.
        language_hint: Optional ISO 639-1/2/3 language code (e.g., 'hun', 'hu', 'hungarian')
                      Used to improve encoding detection accuracy.

    Returns:
        str: FFmpeg-compatible encoding name (e.g., 'UTF-8', 'CP1250', 'CP1251').
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return 'UTF-8'  # Default fallback
    
    # === LANGUAGE HINT MAPPING (45 languages) ===
    LANGUAGE_TO_ENCODING = {
        # Central European (CP1250)
        'hun': 'CP1250', 'hu': 'CP1250', 'hungarian': 'CP1250',
        'ces': 'CP1250', 'cs': 'CP1250', 'cze': 'CP1250', 'czech': 'CP1250',
        'pol': 'CP1250', 'pl': 'CP1250', 'polish': 'CP1250',
        'slk': 'CP1250', 'sk': 'CP1250', 'slo': 'CP1250', 'slovak': 'CP1250',
        'ron': 'CP1250', 'ro': 'CP1250', 'rum': 'CP1250', 'romanian': 'CP1250',
        'hrv': 'CP1250', 'hr': 'CP1250', 'croatian': 'CP1250',
        'slv': 'CP1250', 'sl': 'CP1250', 'slovenian': 'CP1250',
        
        # Cyrillic (CP1251)
        'rus': 'CP1251', 'ru': 'CP1251', 'russian': 'CP1251',
        'ukr': 'CP1251', 'uk': 'CP1251', 'ukrainian': 'CP1251',
        'bul': 'CP1251', 'bg': 'CP1251', 'bulgarian': 'CP1251',
        'srp': 'CP1251', 'sr': 'CP1251', 'serbian': 'CP1251',
        'bel': 'CP1251', 'be': 'CP1251', 'belarusian': 'CP1251',
        'mkd': 'CP1251', 'mk': 'CP1251', 'macedonian': 'CP1251',
        
        # Western European (CP1252 / ISO-8859-1)
        'fra': 'CP1252', 'fr': 'CP1252', 'fre': 'CP1252', 'french': 'CP1252',
        'deu': 'CP1252', 'de': 'CP1252', 'ger': 'CP1252', 'german': 'CP1252',
        'ita': 'CP1252', 'it': 'CP1252', 'italian': 'CP1252',
        'spa': 'CP1252', 'es': 'CP1252', 'spanish': 'CP1252',
        'por': 'CP1252', 'pt': 'CP1252', 'portuguese': 'CP1252',
        'nld': 'CP1252', 'nl': 'CP1252', 'dut': 'CP1252', 'dutch': 'CP1252',
        'swe': 'CP1252', 'sv': 'CP1252', 'swedish': 'CP1252',
        'dan': 'CP1252', 'da': 'CP1252', 'danish': 'CP1252',
        'nor': 'CP1252', 'no': 'CP1252', 'norwegian': 'CP1252',
        'fin': 'CP1252', 'fi': 'CP1252', 'finnish': 'CP1252',
        
        # Greek (CP1253)
        'ell': 'CP1253', 'el': 'CP1253', 'gre': 'CP1253', 'greek': 'CP1253',
        
        # Turkish (CP1254)
        'tur': 'CP1254', 'tr': 'CP1254', 'turkish': 'CP1254',
        
        # Hebrew (CP1255)
        'heb': 'CP1255', 'he': 'CP1255', 'hebrew': 'CP1255',
        
        # Arabic (CP1256)
        'ara': 'CP1256', 'ar': 'CP1256', 'arabic': 'CP1256',
        
        # Baltic (CP1257)
        'lit': 'CP1257', 'lt': 'CP1257', 'lithuanian': 'CP1257',
        'lav': 'CP1257', 'lv': 'CP1257', 'latvian': 'CP1257',
        'est': 'CP1257', 'et': 'CP1257', 'estonian': 'CP1257',
        
        # Thai (CP874)
        'tha': 'CP874', 'th': 'CP874', 'thai': 'CP874',
        
        # Vietnamese (CP1258)
        'vie': 'CP1258', 'vi': 'CP1258', 'vietnamese': 'CP1258',
    }
    
    # Normalize language hint
    suggested_encoding = None
    if language_hint:
        lang_normalized = str(language_hint).lower().strip()
        suggested_encoding = LANGUAGE_TO_ENCODING.get(lang_normalized)

    try:
        with open(file_path, 'rb') as f:
            raw_bytes = f.read(16384)  # Read first 16KB for better detection

        # === PHASE 1: BOM Detection (100% reliable) ===
        if raw_bytes.startswith(b'\xef\xbb\xbf'):
            return 'UTF-8'  # UTF-8 BOM
        if raw_bytes.startswith(b'\xff\xfe\x00\x00'):
            return 'UTF-32LE'  # UTF-32 Little Endian BOM
        if raw_bytes.startswith(b'\x00\x00\xfe\xff'):
            return 'UTF-32BE'  # UTF-32 Big Endian BOM
        if raw_bytes.startswith(b'\xff\xfe'):
            return 'UTF-16LE'  # UTF-16 Little Endian BOM
        if raw_bytes.startswith(b'\xfe\xff'):
            return 'UTF-16BE'  # UTF-16 Big Endian BOM

        # === PHASE 2: UTF-8 Validation (strict) ===
        try:
            raw_bytes.decode('utf-8')
            return 'UTF-8'  # Valid UTF-8 without BOM
        except UnicodeDecodeError:
            pass
        
        # === PHASE 2.5: LANGUAGE HINT VALIDATION (NEW!) ===
        # If user provided language code, try suggested encoding first
        if suggested_encoding:
            try:
                decoded = raw_bytes.decode(suggested_encoding)
                # Validate: check if it looks like valid text (not gibberish)
                # Simple heuristic: count printable chars (letters, digits, punctuation, spaces)
                if len(decoded) > 100:  # Reasonable sample size
                    printable_count = sum(1 for c in decoded if c.isprintable() or c in '\n\r\t')
                    # If 95%+ printable, it's very likely correct encoding
                    if printable_count / len(decoded) > 0.95:
                        return suggested_encoding  # High confidence based on language hint!
            except (UnicodeDecodeError, LookupError):
                pass  # Suggested encoding failed, continue with heuristics

        # === PHASE 3: Heuristic Detection by Byte Patterns ===
        # Count high-byte occurrences to detect encoding family

        # Cyrillic detection (Russian, Ukrainian, Bulgarian, Serbian)
        # CP1251 uses 0xC0-0xFF for Cyrillic letters
        cyrillic_cp1251_count = sum(1 for b in raw_bytes if 0xC0 <= b <= 0xFF)

        # Central European detection (Hungarian, Czech, Polish, Slovak, Romanian)
        # CP1250 uses specific bytes for Central European accented letters.
        central_european_bytes = {0x8C, 0x8D, 0x8E, 0x8F, 0x9C, 0x9D, 0x9E, 0x9F,
                                  0xA5, 0xB9, 0xBC, 0xBE, 0xD0, 0xD1, 0xD2, 0xD3,
                                  0xD8, 0xDD, 0xDE, 0xE0, 0xF0, 0xF1, 0xF8, 0xFD, 0xFE}
        cp1250_specific_count = sum(1 for b in raw_bytes if b in central_european_bytes)

        # Greek detection (CP1253)
        greek_bytes = {0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA,
                       0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA}
        greek_count = sum(1 for b in raw_bytes if b in greek_bytes)

        # Hebrew detection (CP1255)
        hebrew_bytes = set(range(0xE0, 0xFB))
        hebrew_count = sum(1 for b in raw_bytes if b in hebrew_bytes)

        # Arabic detection (CP1256)
        arabic_bytes = set(range(0xC1, 0xFB))
        arabic_count = sum(1 for b in raw_bytes if b in arabic_bytes)

        # Turkish detection (CP1254) - specific Turkish chars
        turkish_bytes = {0xD0, 0xDD, 0xDE, 0xF0, 0xFD, 0xFE, 0x8C, 0x9C, 0x9F}
        turkish_count = sum(1 for b in raw_bytes if b in turkish_bytes)

        # Thai detection (CP874)
        thai_bytes = set(range(0xA1, 0xFB))
        thai_count = sum(1 for b in raw_bytes if b in thai_bytes)

        # Vietnamese detection (CP1258)
        vietnamese_bytes = {0xC0, 0xC1, 0xC2, 0xC3, 0xC8, 0xC9, 0xCA, 0xCC, 0xCD,
                            0xD2, 0xD3, 0xD4, 0xD5, 0xD9, 0xDA, 0xDD}
        vietnamese_count = sum(1 for b in raw_bytes if b in vietnamese_bytes)

        # Baltic detection (CP1257) - Lithuanian, Latvian, Estonian
        baltic_bytes = {0xA8, 0xAA, 0xAF, 0xB8, 0xBA, 0xBF, 0xC6, 0xC7, 0xCA,
                        0xCE, 0xD0, 0xD1, 0xD2, 0xD3, 0xD8, 0xE6, 0xE7, 0xEA}
        baltic_count = sum(1 for b in raw_bytes if b in baltic_bytes)

        # === PHASE 4: Scoring and Selection ===
        sample_size = len(raw_bytes)
        threshold = sample_size * 0.01  # 1% threshold for detection

        # Prioritized detection based on scores
        scores = [
            (cyrillic_cp1251_count, 'CP1251', 'Cyrillic'),
            (cp1250_specific_count * 3, 'CP1250', 'Central European'),  # Boost for specific chars
            (greek_count, 'CP1253', 'Greek'),
            (hebrew_count, 'CP1255', 'Hebrew'),
            (arabic_count, 'CP1256', 'Arabic'),
            (turkish_count * 2, 'CP1254', 'Turkish'),
            (thai_count, 'CP874', 'Thai'),
            (vietnamese_count, 'CP1258', 'Vietnamese'),
            (baltic_count, 'CP1257', 'Baltic'),
        ]

        # Find best match above threshold
        best_score, best_encoding, _ = max(scores, key=lambda x: x[0])

        if best_score > threshold:
            return best_encoding

        # === PHASE 5: Fallback Validation ===
        # Try common encodings in order of global popularity
        FALLBACK_ENCODINGS = [
            # Central/Eastern European (most common for Hungarian)
            ('cp1250', 'CP1250'),       # Windows Central European
            ('iso-8859-2', 'ISO-8859-2'),  # Latin-2

            # Cyrillic
            ('cp1251', 'CP1251'),       # Windows Cyrillic
            ('koi8-r', 'KOI8-R'),       # Russian KOI8
            ('iso-8859-5', 'ISO-8859-5'),  # ISO Cyrillic

            # Western European
            ('cp1252', 'CP1252'),       # Windows Western
            ('iso-8859-1', 'ISO-8859-1'),  # Latin-1
            ('iso-8859-15', 'ISO-8859-15'),  # Latin-9 (with Euro)

            # Greek
            ('cp1253', 'CP1253'),       # Windows Greek
            ('iso-8859-7', 'ISO-8859-7'),  # ISO Greek

            # Turkish
            ('cp1254', 'CP1254'),       # Windows Turkish
            ('iso-8859-9', 'ISO-8859-9'),  # Latin-5

            # Hebrew
            ('cp1255', 'CP1255'),       # Windows Hebrew
            ('iso-8859-8', 'ISO-8859-8'),  # ISO Hebrew

            # Arabic
            ('cp1256', 'CP1256'),       # Windows Arabic
            ('iso-8859-6', 'ISO-8859-6'),  # ISO Arabic

            # Baltic
            ('cp1257', 'CP1257'),       # Windows Baltic
            ('iso-8859-13', 'ISO-8859-13'),  # Latin-7

            # Vietnamese
            ('cp1258', 'CP1258'),       # Windows Vietnamese

            # Thai
            ('cp874', 'CP874'),         # Windows Thai
            ('iso-8859-11', 'TIS-620'),  # Thai TIS

            # Asian (CJK)
            ('shift_jis', 'SHIFT_JIS'),  # Japanese
            ('euc-jp', 'EUC-JP'),       # Japanese
            ('gb2312', 'GB2312'),       # Chinese Simplified
            ('gbk', 'GBK'),             # Chinese Simplified Extended
            ('big5', 'BIG5'),           # Chinese Traditional
            ('euc-kr', 'EUC-KR'),       # Korean
        ]

        best_fallback = None
        best_printable = 0.0
        for python_enc, ffmpeg_enc in FALLBACK_ENCODINGS:
            try:
                decoded = raw_bytes.decode(python_enc)
                if len(decoded) > 0:
                    ratio = sum(1 for c in decoded if c.isprintable() or c in '\n\r\t') / len(decoded)
                    if ratio > best_printable:
                        best_printable = ratio
                        best_fallback = ffmpeg_enc
            except (UnicodeDecodeError, LookupError):
                continue
        if best_fallback:
            return best_fallback
        return 'CP1250'

    except Exception:
        return 'UTF-8'  # Safe default on any error


def find_subtitle_files(video_path):
    """Find subtitle files associated with a video.
    
    Finds all subtitle files (.srt, .ass, .ssa, .vtt, .sub) that match the video name,
    optionally with a language code.
    
    Args:
        video_path: Path to the video file (Path).
        
    Returns:
        list: List of (subtitle_path, language_code) tuples.
    """
    video_stem = video_path.stem
    video_dir = video_path.parent
    subtitle_files = []
    found_paths = set()
    
    for file_path in video_dir.iterdir():
        if not file_path.is_file() or file_path.suffix.lower() not in SUBTITLE_EXTENSIONS:
            continue
        base_name, lang_part = extract_language_from_filename(file_path.name)
        if video_stem.strip().lower() == base_name.strip().lower():
            if file_path not in found_paths:
                subtitle_files.append((file_path, lang_part))
                found_paths.add(file_path)
    return subtitle_files

SUBTITLE_VALIDATION_SAMPLE_BYTES = 200_000
SRT_BLOCK_PATTERN = re.compile(r'^\s*\d+\s*\r*\n\s*\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}', re.MULTILINE)
VTT_HEADER_PATTERN = re.compile(r'^\ufeff*WEBVTT', re.IGNORECASE)
VTT_TIMECODE_PATTERN = re.compile(r'\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}')
ASS_EVENTS_PATTERN = re.compile(r'\[Events\]', re.IGNORECASE)
ASS_DIALOGUE_PATTERN = re.compile(r'^\s*Dialogue:', re.IGNORECASE | re.MULTILINE)
SUB_MICRODVD_PATTERN = re.compile(r'\{\d+\}\{\d+\}')

def _read_subtitle_preview(file_path, limit=SUBTITLE_VALIDATION_SAMPLE_BYTES):
    """Read a small preview of the subtitle file for validation.
    
    Args:
        file_path: Path to the subtitle file.
        limit: Number of bytes to read.
        
    Returns:
        str: Decoded text content or empty string on error.
    """
    try:
        with file_path.open('rb') as handle:
            data = handle.read(limit)
    except (OSError, IOError):
        return ""
    if not data:
        return ""
    text = data.decode('utf-8', errors='ignore')
    if not text.strip():
        text = data.decode('latin-1', errors='ignore')
    return text.replace('\x00', '')

def _validate_subtitle_with_ffmpeg(file_path):
    """Validate subtitle file using FFmpeg to ensure it can be read.
    
    This is a more thorough check than pattern matching, as FFmpeg will
    actually try to parse the entire file.
    
    Args:
        file_path: Path to the subtitle file.
        
    Returns:
        tuple: (bool, str) - (True if valid, error message or empty string).
    """
    try:
        # Use FFmpeg to test if the subtitle file can be read
        # Command: ffmpeg -i "file" -c:s copy -f null NUL
        # This will catch encoding issues, corrupted files, etc.
        import platform
        null_output = 'NUL' if platform.system() == 'Windows' else '/dev/null'
        
        cmd = [
            FFMPEG_PATH,
            '-i', str(file_path),
            '-c:s', 'copy',
            '-f', 'null',
            null_output,
            '-loglevel', 'error'
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            startupinfo=get_startup_info()
        )
        # Check stderr for actual error messages
        if result.stderr:
            error_lower = result.stderr.lower()
            # Common FFmpeg subtitle errors
            if 'invalid data' in error_lower or 'error opening input' in error_lower:
                # Extract more specific error message if available
                error_lines = result.stderr.strip().split('\n')
                error_msg = error_lines[-1] if error_lines else "FFmpeg cannot read subtitle file"
                return False, f"FFmpeg error: {error_msg}"
        # If return code is non-zero and we have stderr, it's likely an error
        if result.returncode != 0 and result.stderr:
            error_lower = result.stderr.lower()
            if 'invalid' in error_lower or 'error' in error_lower:
                return False, "FFmpeg validation failed"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Subtitle validation timeout"
    except FileNotFoundError:
        # FFmpeg not found - skip FFmpeg validation, rely on pattern matching
        return True, ""
    except Exception as e:
        # Other errors - be conservative and allow the file
        # (might be a temporary issue)
        return True, ""

def is_valid_subtitle_file(file_path):
    """Validate subtitle file content and format.
    
    Checks file size, readability, and format-specific headers/patterns
    (SRT, VTT, ASS/SSA, SUB). Also validates with FFmpeg if possible.
    
    Args:
        file_path: Path to the subtitle file.
        
    Returns:
        tuple: (bool, str) - (True if valid, error message or empty string).
    """
    suffix = file_path.suffix.lower()
    try:
        stat_info = file_path.stat()
    except (OSError, ValueError):
        return False, "Subtitle not readable"
    if stat_info.st_size < 10:
        return False, "Subtitle file too small (<10 bytes)"
    text_preview = _read_subtitle_preview(file_path)
    if not text_preview or not text_preview.strip():
        return False, "Subtitle empty or unreadable"
    if suffix == '.srt':
        if SRT_BLOCK_PATTERN.search(text_preview):
            # Pattern matches - now validate with FFmpeg for thoroughness
            ffmpeg_valid, ffmpeg_error = _validate_subtitle_with_ffmpeg(file_path)
            if not ffmpeg_valid:
                return False, ffmpeg_error or "FFmpeg validation failed"
            return True, ""
        return False, "Missing SRT timing"
    if suffix == '.vtt':
        first_line = text_preview.splitlines()[0] if text_preview.splitlines() else ""
        if VTT_HEADER_PATTERN.search(first_line) or VTT_TIMECODE_PATTERN.search(text_preview):
            ffmpeg_valid, ffmpeg_error = _validate_subtitle_with_ffmpeg(file_path)
            if not ffmpeg_valid:
                return False, ffmpeg_error or "FFmpeg validation failed"
            return True, ""
        return False, "WEBVTT header/timing missing"
    if suffix in ('.ass', '.ssa'):
        if ASS_EVENTS_PATTERN.search(text_preview) and ASS_DIALOGUE_PATTERN.search(text_preview):
            ffmpeg_valid, ffmpeg_error = _validate_subtitle_with_ffmpeg(file_path)
            if not ffmpeg_valid:
                return False, ffmpeg_error or "FFmpeg validation failed"
            return True, ""
        return False, "ASS/SSA Events section missing"
    if suffix == '.sub':
        if SUB_MICRODVD_PATTERN.search(text_preview):
            ffmpeg_valid, ffmpeg_error = _validate_subtitle_with_ffmpeg(file_path)
            if not ffmpeg_valid:
                return False, ffmpeg_error or "FFmpeg validation failed"
            return True, ""
        return False, "SUB timing missing"
    return True, ""

def split_valid_invalid_subtitles(subtitle_files):
    """Split subtitle files into valid and invalid groups.
    
    Args:
        subtitle_files: List of (subtitle_path, language_code) tuples.
        
    Returns:
        tuple: (valid, invalid) where:
            - valid: List of valid subtitles [(path, language)]
            - invalid: List of invalid subtitles [(path, language, reason)]
    """
    valid = []
    invalid = []
    for sub_path, lang_part in subtitle_files:
        is_valid, reason = is_valid_subtitle_file(sub_path)
        if is_valid:
            valid.append((sub_path, lang_part))
        else:
            invalid.append((sub_path, lang_part, reason or "Unknown format"))
    return valid, invalid


def copy_external_subtitles(source_video_path, dest_video_path):
    """Copy all external subtitles associated with the source video to the destination.
    
    Args:
        source_video_path: Path to the source video file.
        dest_video_path: Path to the destination video file.
        
    Returns:
        int: Number of subtitle files copied.
    """
    try:
        source_path = Path(source_video_path)
        dest_path = Path(dest_video_path)
        
        if not source_path.exists():
            return 0
            
        subtitle_files = find_subtitle_files(source_path)
        copied_count = 0
        
        for sub_path, lang_part in subtitle_files:
            try:
                # Construct destination subtitle filename
                # Matches the destination video filename stem
                dest_sub_name = dest_path.stem
                if lang_part:
                    dest_sub_name += f".{lang_part}"
                dest_sub_name += sub_path.suffix
                
                dest_sub_path = dest_path.parent / dest_sub_name
                
                # Create parent directory if needed
                dest_sub_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Copy if not exists or different size
                if not dest_sub_path.exists() or dest_sub_path.stat().st_size != sub_path.stat().st_size:
                    shutil.copy2(sub_path, dest_sub_path)
                    copied_count += 1
            except Exception as e:
                print(f"Warning: Failed to copy subtitle {sub_path.name}: {e}")
                
        return copied_count
    except Exception as e:
        print(f"Warning: Error in copy_external_subtitles: {e}")
        return 0
