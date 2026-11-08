"""Rebuild the dissertation from saved tables, without raw-book access or fitting."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
os.environ['FYP_ROOT'] = str(ROOT)
sys.path.insert(0, str(ROOT))

from FYP.reporting.processed_extension import prepare as prepare_processed
from FYP.reporting.robustness_extension import prepare as prepare_robustness
from FYP.reporting.allocation_extension import prepare as prepare_allocation
from FYP.reporting.sensitivity_extension import prepare as prepare_sensitivity
from FYP.reporting.report import build


def main():
    for directory, prepare in [('processed_review_v1', prepare_processed),
                               ('robustness_review_v1', prepare_robustness),
                               ('allocation_review_v1', prepare_allocation),
                               ('sensitivity_review_v1', prepare_sensitivity)]:
        extension = ROOT / 'artifacts/extensions' / directory
        if not (extension / 'complete.json').exists():
            continue
        # Original experimental outputs must remain intact before regenerating
        # presentation tables. Report-derived files are deliberately regenerated.
        recorded = json.loads((extension / 'complete.json').read_text())
        for name, expected in recorded['files_sha256'].items():
            if name.startswith('tables/report_') or name in ('report_tables.json', 'table_widths.json'):
                continue
            if directory in ('robustness_review_v1', 'allocation_review_v1') and (name.startswith('figures/') or name == 'figure_index.csv'):
                continue
            path = extension / name
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError('Changed extension evidence: ' + name)
        prepare(ROOT)
        paths = [p for p in extension.rglob('*') if p.is_file() and p.name != 'complete.json']
        manifest = dict(files_sha256={p.relative_to(extension).as_posix():
                        hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})
        (extension / 'complete.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(build())


if __name__ == '__main__':
    main()
