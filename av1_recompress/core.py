# Auto-generated aggregator for core fragments
from pathlib import Path

_core_dir = Path(__file__).resolve().parent
_core_parts = [
    'core_preamble_and_imports.py',
    'core_paths_tools_logging.py',
    'core_translations_bridge.py',
    'core_probe_and_scan.py',
    'core_subtitles_and_metadata.py',
    'core_metrics_and_validation.py',
    'core_audio_video_ops.py',
    'core_workers_and_flows.py',
    'core_cli_and_entrypoints.py',
]

_core_combined = []
for _name in _core_parts:
    _content = (_core_dir / _name).read_text(encoding='utf-8')
    if _content.startswith("﻿"):
        _content = _content.lstrip("﻿")
    _core_combined.append(_content)

exec("\n".join(_core_combined), globals())

del _name, _core_parts, _core_dir, _content, _core_combined
