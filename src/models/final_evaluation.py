"""Frozen final inference, event selections and prediction metrics; no fitting."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from src.common import sha256
from src.data.final_evaluation import verify as verify_data
from src.models.evaluation import ranked, safe_ap
from src.models.features import family
from src.models.final_preparation import score_bundle
from src.paths import PROJECT_ROOT
from src.reporting.final_protocol import verify as verify_protocol


def ratio(a,b):
    return float(a/b) if b else np.nan


def canonicalize_forest_alias(prediction):
    """The same saved forest is one policy, despite parallel sum roundoff."""
    forest = prediction.loc[prediction.model.eq('fixed_book_random_forest')].sort_values(['event_slug','time'])
    alias = prediction.loc[prediction.model.eq('n3_book_random_forest')].sort_values(['event_slug','time'])
    pd.testing.assert_frame_equal(forest[['event_slug','time']].reset_index(drop=True),alias[['event_slug','time']].reset_index(drop=True))
    np.testing.assert_allclose(forest.score,alias.score,rtol=1e-12,atol=1e-14)
    prediction = prediction.copy()
    prediction.loc[alias.index,'score'] = forest.score.to_numpy()
    return prediction


def scope_pools(scores):
    """Scope membership is determined by frozen event keys, never outcomes."""
    yield 'frozen_supported_universe',scores,(3,5)
    yield 'development_events_only',scores.loc[scores.development_event],(3,5)
    for slug in sorted(scores.event_slug.unique()):
        yield 'leave_out:'+slug,scores.loc[scores.event_slug.ne(slug)],(3,)


def scored_references(prediction, seeds):
    pool = prediction.loc[prediction.model.eq('quoted_gap_rule')].sort_values(['time','event_slug']).reset_index(drop=True)
    pieces = [prediction]
    for i,seed in enumerate(seeds):
        pieces.append(pool.assign(model=f'random_{i:02d}',score=np.random.default_rng(seed).random(len(pool))))
    pieces.append(pool.assign(model='monitor_all',score=0.))
    return pd.concat(pieces,ignore_index=True)


def selections(scores):
    rows = []
    for scope,pool,budgets in scope_pools(scores):
        for n in budgets:
            for name,part in pool.groupby('model'):
                chosen = part if name == 'monitor_all' else ranked(part,part.score,n).drop(columns='_score')
                rows.append(chosen.assign(scope=scope,top_n=n))
    return pd.concat(rows,ignore_index=True)


def measure(pool, chosen, n):
    count = pool.groupby('time').size()
    chance = pool.time.map(count).map(lambda size:min(n,size)/size)
    known = pool.label_known.astype(bool)
    selected_known = chosen.label_known.astype(bool)
    positive = pool.target.eq(1)
    captured = chosen.target.eq(1)
    new = known & ~pool.opportunity_at_scan & pool.future_positive
    selected_new = selected_known & ~chosen.opportunity_at_scan & chosen.future_positive
    total_value = float(pool.value_target.sum())
    captured_value = float(chosen.value_target.sum())
    random_positive = float((positive*chance).sum())
    random_value = float((pool.value_target.fillna(0)*chance).sum())
    competitive = pool.time.map(count).gt(n)
    comp_selected = chosen.time.map(count).gt(n)
    return dict(top_n=n,windows=pool.time.nunique(),competitive_windows=int(count.gt(n).sum()),
        candidates=len(pool),selected=len(chosen),known_candidates=int(known.sum()),known_selected=int(selected_known.sum()),
        selected_unknown=int((~selected_known).sum()),selected_unknown_fraction=ratio((~selected_known).sum(),len(chosen)),
        positive_windows=int(positive.sum()),captured_positive_windows=int(captured.sum()),
        recall=ratio(captured.sum(),positive.sum()),precision_known=ratio(captured.sum(),selected_known.sum()),
        competitive_recall=ratio(chosen.loc[comp_selected,'target'].eq(1).sum(),pool.loc[competitive,'target'].eq(1).sum()),
        total_peak_value=total_value,captured_peak_value=captured_value,peak_value_recall=ratio(captured_value,total_value),
        known_new_without_scan=int(new.sum()),captured_new_without_scan=int(selected_new.sum()),
        new_without_scan_recall=ratio(selected_new.sum(),new.sum()),
        average_precision=safe_ap(pool.loc[known,'target'],pool.loc[known,'score']),
        expected_random_captured=random_positive,expected_random_peak_value=random_value,
        expected_random_recall=ratio(random_positive,positive.sum()),expected_random_value_recall=ratio(random_value,total_value),
        capture_lift_vs_random=ratio(captured.sum(),random_positive),peak_value_lift_vs_random=ratio(captured_value,random_value),
        all_token_snapshots=float(pool.x_active_tokens.sum()),selected_token_snapshots=float(chosen.x_active_tokens.sum()))


def evaluate(output, data):
    output,data = Path(output),Path(data)
    data_design = verify_data(data)
    protocol = verify_protocol(PROJECT_ROOT/'results/final_protocol',PROJECT_ROOT/'results/final_coverage')
    root = PROJECT_ROOT/'results/final_preparation'
    for name,digest in protocol['integrity']['final_model_files_sha256'].items():
        if sha256(root/name) != digest:
            raise ValueError('Frozen final model changed')
    features = pd.read_parquet(data/'features.parquet')
    if features.time.lt(pd.Timestamp(protocol['period']['start'])).any():
        raise ValueError('Final inference cannot score training observations')
    bundle = json.loads((root/'final_refit_bundle.json').read_text())
    # No label-bearing table is passed to any fitted model.
    prediction = score_bundle(root,bundle,features)
    frame = pd.read_parquet(data/'examples.parquet')
    prediction = prediction.merge(frame,on=['event_slug','time'],validate='many_to_one')
    prediction['family'] = prediction.event_slug.map(family)
    prediction = canonicalize_forest_alias(prediction)
    scores = scored_references(prediction,protocol['references']['random']['seeds'])
    chosen = selections(scores)
    rows,groups = [],[]
    for scope,pool,budgets in scope_pools(scores):
        for n in budgets:
            for model,part in pool.groupby('model'):
                selected = chosen.loc[chosen.scope.eq(scope)&chosen.top_n.eq(n)&chosen.model.eq(model)]
                counts = part.groupby('time').size()
                for subset,times in [('full',set(part.time)),('competitive',set(counts.loc[counts>n].index))]:
                    p,s = part.loc[part.time.isin(times)],selected.loc[selected.time.isin(times)]
                    rows.append(dict(scope=scope,model=model,subset=subset,**measure(p,s,n)))
                if scope in ('frozen_supported_universe','development_events_only'):
                    for kind in ('event_slug','family'):
                        for group,g in part.groupby(kind):
                            s = selected.loc[selected[kind].eq(group)]
                            groups.append(dict(scope=scope,top_n=n,model=model,group_type=kind,group=group,
                                positive_windows=int(g.target.eq(1).sum()),captured_positive_windows=int(s.target.eq(1).sum()),
                                total_peak_value=float(g.value_target.sum()),captured_peak_value=float(s.value_target.sum())))
    for scope in ('frozen_supported_universe','development_events_only'):
        primary = chosen.loc[chosen.scope.eq(scope)&chosen.top_n.eq(5)]
        base = primary.loc[primary.model.eq('monitor_all'),['event_slug','time']].sort_values(['time','event_slug']).reset_index(drop=True)
        for _,part in primary.groupby('model'):
            pd.testing.assert_frame_equal(base,part[['event_slug','time']].sort_values(['time','event_slug']).reset_index(drop=True))
    output.mkdir(parents=True,exist_ok=False)
    prediction.to_parquet(output/'predictions.parquet',index=False)
    scores.to_parquet(output/'reference_scores.parquet',index=False)
    chosen.to_parquet(output/'selections.parquet',index=False)
    pd.DataFrame(rows).to_csv(output/'metrics.csv',index=False)
    pd.DataFrame(groups).to_csv(output/'group_capture.csv',index=False)
    design = dict(dataset_completion_sha256=sha256(data/'complete.json'),
        protocol_sha256=sha256(PROJECT_ROOT/'results/final_protocol/protocol.json'),
        prepared_completion_sha256=sha256(root/'complete.json'),
        source_sha256={**data_design['source_sha256'],'src/models/final_evaluation.py':sha256(Path(__file__))},
        models_retrained=False,final_outcomes_opened=True,labels_passed_to_models=False,
        random_seeds=protocol['references']['random']['seeds'],forest_alias_verified=True,n5_selection_equality_verified=True)
    (output/'design.json').write_text(json.dumps(design,indent=2)+'\n')
    files = {p.name:sha256(p) for p in output.iterdir() if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,models_retrained=False,
        final_outcomes_opened=True),indent=2)+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    torch.set_num_threads(4)
    with threadpool_limits(limits=4):
        evaluate(args.output,args.data)
