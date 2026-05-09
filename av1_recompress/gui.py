# Auto-generated aggregator for gui fragments
from pathlib import Path

_gui_dir = Path(__file__).resolve().parent
_gui_parts = [
    'gui_preamble_and_imports.py',
    'gui_app_state_and_paths.py',
    'gui_widgets_layout.py',
    'gui_language_and_labels.py',
    'gui_tree_setup.py',
    'gui_buttons_actions.py',
    'gui_db_and_state_load.py',
    'gui_context_menus.py',
    'gui_video_loading.py',
    'gui_encoding_control.py',
    'gui_nvenc_worker.py',
    'gui_svt_worker.py',
    'gui_vmaf_worker.py',
    'gui_audio_worker.py',
    'gui_queue_management.py',
    'gui_events_and_progress.py',
    'gui_misc_helpers.py',
]

_gui_header = ""
_gui_combined = [_gui_header]
for _name in _gui_parts:
    _content = (_gui_dir / _name).read_text(encoding='utf-8')
    if _content.startswith("﻿"):
        _content = _content.lstrip("﻿")
    _gui_combined.append(_content)

exec("\n".join(_gui_combined), globals())

del _name, _gui_parts, _gui_dir, _content, _gui_combined, _gui_header
