"""Alignment causality and run-location isolation."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from src.data.dataset import align_books
from src.reporting import workflow
from fixtures import BASE, _frame


class DatasetTests(unittest.TestCase):
    def test_alignment_uses_only_prior_books_inside_recorded_extent(self):
        frame = _frame([(0, [(.4, 10)], [(.3, 10)]),
                        (10, [(.6, 20)], [(.5, 20)])])
        times = [BASE + pd.Timedelta(seconds=s) for s in [-1, 5, 10, 11]]
        aligned = align_books({'a': frame}, times)
        self.assertEqual(aligned.observed.tolist(), [False, True, True, False])
        self.assertAlmostEqual(aligned.iloc[1].best_ask, .4)
        self.assertEqual(aligned.iloc[1].age_s, 5.)
        self.assertAlmostEqual(aligned.iloc[2].best_ask, .6)

    def test_alignment_rejects_final_test_times(self):
        frame = _frame([(0, [(.4, 10)], [(.3, 10)])])
        with self.assertRaisesRegex(ValueError, 'development'):
            align_books({'a': frame}, [pd.Timestamp('2026-01-08', tz='UTC')])

    def test_dataset_pointer_cannot_escape_processed_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'active_dataset.json').write_text(json.dumps(
                {'features': '../outside.parquet', 'labels': 'labels'}))
            with patch.object(workflow, 'PROCESSED_DIR', root):
                with self.assertRaisesRegex(ValueError, 'within processed'):
                    workflow.dataset_paths()

    def test_execution_pointer_cannot_escape_model_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'execution_pointer.json').write_text(json.dumps({'folder': '../other'}))
            with self.assertRaisesRegex(ValueError, 'within model run'):
                workflow.execution_path(root)
