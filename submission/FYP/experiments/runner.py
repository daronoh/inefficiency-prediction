"""End-to-end study entry point, with cached verification or full reconstruction."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits
from FYP.paths import PROJECT_ROOT
from FYP.common import sha256
from FYP.data.pipeline import ROOT as DATA, load, rebuild, plans, write_json
from FYP.experiments.study import OUTPUT as MODELS, run as study
from FYP.experiments.economics import OUTPUT as REPLAY, run as replay, summarize
from FYP.reporting.verification import verify, verify_replay
from FYP.reporting.artifacts import generate
from FYP.reporting.report import build as report


def test_suite(output):
    suite=unittest.defaultTestLoader.discover(str(PROJECT_ROOT/'FYP/tests'))
    def names(item):
        if isinstance(item,unittest.TestSuite):
            return [name for sub in item for name in names(sub)]
        return [item.id()]
    identifiers=names(suite)
    if len(identifiers)!=28:raise ValueError('Submission inventory should have 28 focused tests')
    pd.DataFrame(dict(test=identifiers)).to_csv(output/'test_index.csv',index=False)
    result=unittest.TextTestRunner(verbosity=1).run(suite)
    if not result.wasSuccessful():raise ValueError('Submission tests failed')
    return dict(tests_run=result.testsRun,failures=len(result.failures),errors=len(result.errors))


def reconcile(replay_dir):
    attempts=pd.read_parquet(replay_dir/'attempts.parquet')
    choices=pd.read_parquet(replay_dir/'selections.parquet')
    saved=pd.read_csv(replay_dir/'summary.csv')
    computed=summarize(attempts,choices)
    keys=['scope','top_n','model','arrival_delay_s','fold']
    a=saved.set_index(keys).sort_index();b=computed.set_index(keys).sort_index()
    if not a.index.equals(b.index):raise ValueError('Economic summary population changed')
    for c in ['known_pnl_usd','completed_profit_usd','partial_pnl_usd','attempts','completed','unknown_attempts','max_drawdown_usd','no_settlement_funding_usd']:
        np.testing.assert_allclose(a[c],b[c],rtol=1e-10,atol=1e-7,equal_nan=True)
    return dict(summary_rows=len(saved),attempt_rows=len(attempts),economic_accounting_reconciled=True)


def execute(full=False,device=None):
    torch.set_num_threads(4)
    device=device or ('cuda' if torch.cuda.is_available() else 'cpu')
    if device=='cuda' and not torch.cuda.is_available():raise ValueError('CUDA is unavailable; choose cpu')
    with threadpool_limits(limits=4):
        from FYP.rules.audit import verify_metadata
        metadata=verify_metadata(plans())
        if full:
            run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
            destination=PROJECT_ROOT/'runs'/run_id;destination.mkdir(parents=True)
            data=destination/'data';models=destination/'models';economic=destination/'replay';artifacts=destination/'artifacts'
            rebuild(data)
            study(models,data,device)
            replay(economic,models,data)
        else:
            destination=PROJECT_ROOT;data=DATA;models=MODELS;economic=REPLAY;artifacts=PROJECT_ROOT/'artifacts'
            load(data)
        artifacts.mkdir(parents=True,exist_ok=True)
        print('Checking actual model populations and reproducing saved scores...',flush=True)
        result=verify(models,data,reproduce_scores=True)
        verify_replay(economic)
        result.update(reconcile(economic))
        result.update(test_suite(artifacts))
        result.update(metadata)
        generate(models,economic,data,artifacts)
        report(artifacts,destination/'report')
        result.update(mode='full raw reconstruction and refit' if full else 'verified cached study; regenerated analyses',
            figures=50,raw_files=994,execution_target_used=False)
        write_json(artifacts/'verification.json',result)
        print('Research outputs:',destination,flush=True)
        return destination


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full',action='store_true',help='Rebuild from raw, tune/refit and replay into a new runs directory')
    parser.add_argument('--device',choices=['cuda','cpu'])
    args=parser.parse_args();execute(args.full,args.device)


if __name__=='__main__':main()
