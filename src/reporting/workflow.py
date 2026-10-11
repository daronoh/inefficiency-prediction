"""Portable notebook run locations; fresh runs never overwrite completed results."""
from datetime import datetime, timezone
import json
from pathlib import Path
from src.config import ScreeningSpec
from src.paths import PROJECT_ROOT, PROCESSED_DIR, FEATURES, LABELS_DIR, RESULTS_DIR


def timestamp():
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')


def dataset_paths():
    pointer = PROCESSED_DIR / 'active_dataset.json'
    if not pointer.exists():
        return FEATURES, LABELS_DIR
    relative = json.loads(pointer.read_text())
    paths = [(PROCESSED_DIR / relative[k]).resolve() for k in ['features', 'labels']]
    if any(not p.is_relative_to(PROCESSED_DIR.resolve()) for p in paths):
        raise ValueError('Dataset pointer must stay within processed data')
    return tuple(paths)


def select_dataset(features, labels):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    value = {k: str(Path(p).resolve().relative_to(PROCESSED_DIR.resolve()))
             for k, p in [('features', features), ('labels', labels)]}
    (PROCESSED_DIR / 'active_dataset.json').write_text(json.dumps(value, indent=2)+'\n')


def dataset_spec(features):
    return ScreeningSpec(**json.loads((Path(features).parent / 'run.json').read_text())['contract'])


def new_model_run():
    if not RESULTS_DIR.exists():
        return RESULTS_DIR
    return PROJECT_ROOT / 'results' / 'runs' / timestamp()


def select_model_run(run_dir):
    root = PROJECT_ROOT / 'results'
    relative = Path(run_dir).resolve().relative_to(root.resolve())
    (root / 'latest.json').write_text(json.dumps({'run': relative.as_posix()}, indent=2)+'\n')


def active_model_run():
    root = PROJECT_ROOT / 'results'
    pointer = root / 'latest.json'
    result = (root / json.loads(pointer.read_text())['run']).resolve() if pointer.exists() else RESULTS_DIR
    if not result.resolve().is_relative_to(root.resolve()):
        raise ValueError('Model run must stay within results')
    return result


def new_execution_run(run_dir):
    root = Path(run_dir) / 'execution'
    return root if not root.exists() else Path(run_dir) / f'execution_{timestamp()}'


def select_execution_run(run_dir, execution_dir):
    relative = Path(execution_dir).resolve().relative_to(Path(run_dir).resolve())
    (Path(run_dir) / 'execution_pointer.json').write_text(json.dumps({'folder': relative.as_posix()}, indent=2)+'\n')


def execution_path(run_dir):
    root = Path(run_dir)
    pointer = root / 'execution_pointer.json'
    result = (root / json.loads(pointer.read_text())['folder']).resolve() if pointer.exists() else root / 'execution'
    if not result.resolve().is_relative_to(root.resolve()):
        raise ValueError('Execution run must stay within model run')
    return result


def timing_path(run_dir):
    root = execution_path(run_dir)
    pointer = root / 'timing_pointer.json'
    result = (root / json.loads(pointer.read_text())['folder']).resolve() if pointer.exists() else root / 'timing'
    if not result.resolve().is_relative_to(root.resolve()):
        raise ValueError('Timing run must stay within execution run')
    return result


def new_timing_run(run_dir):
    root = execution_path(run_dir)
    return root / 'timing' if not (root / 'timing').exists() else root / f'timing_{timestamp()}'


def select_timing_run(run_dir, folder):
    root = execution_path(run_dir)
    relative = Path(folder).resolve().relative_to(root.resolve())
    (root / 'timing_pointer.json').write_text(json.dumps({'folder': relative.as_posix()}, indent=2)+'\n')


def capacity_path(run_dir):
    root = execution_path(run_dir)
    pointer = root / 'capacity_pointer.json'
    result = (root / json.loads(pointer.read_text())['folder']).resolve() if pointer.exists() else root / 'capacity'
    if not result.resolve().is_relative_to(root.resolve()):
        raise ValueError('Capacity run must stay within execution run')
    return result


def new_capacity_run(run_dir):
    root = execution_path(run_dir)
    return root / 'capacity' if not (root / 'capacity').exists() else root / f'capacity_{timestamp()}'


def select_capacity_run(run_dir, folder):
    root = execution_path(run_dir)
    relative = Path(folder).resolve().relative_to(root.resolve())
    (root / 'capacity_pointer.json').write_text(json.dumps({'folder': relative.as_posix()}, indent=2)+'\n')


def profitability_dataset():
    pointer = PROCESSED_DIR / 'profitability_latest.json'
    result = (PROCESSED_DIR / json.loads(pointer.read_text())['folder']).resolve() if pointer.exists() else PROCESSED_DIR / 'profitability'
    if not result.resolve().is_relative_to(PROCESSED_DIR.resolve()):
        raise ValueError('Profitability dataset must stay within processed data')
    return result


def select_profitability_dataset(folder):
    relative = Path(folder).resolve().relative_to(PROCESSED_DIR.resolve())
    (PROCESSED_DIR / 'profitability_latest.json').write_text(json.dumps({'folder':relative.as_posix()},indent=2)+'\n')


def new_profitability_dataset():
    root = PROCESSED_DIR / 'profitability'
    return root if not root.exists() else PROCESSED_DIR / 'profitability_runs' / timestamp()


def profitability_run():
    root = PROJECT_ROOT / 'results'
    pointer = root / 'profitability_latest.json'
    result = (root / json.loads(pointer.read_text())['run']).resolve() if pointer.exists() else root / 'profitability'
    if not result.resolve().is_relative_to(root.resolve()):
        raise ValueError('Profitability run must stay within results')
    return result


def select_profitability_run(folder):
    root = PROJECT_ROOT / 'results'
    relative = Path(folder).resolve().relative_to(root.resolve())
    (root / 'profitability_latest.json').write_text(json.dumps({'run':relative.as_posix()},indent=2)+'\n')


def new_profitability_run():
    root = PROJECT_ROOT / 'results' / 'profitability'
    return root if not root.exists() else PROJECT_ROOT / 'results' / 'runs' / ('profitability_'+timestamp())
