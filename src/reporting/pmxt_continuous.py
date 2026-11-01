"""Paired evidence for continuous PMXT initialization, without fitting models."""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path

import pandas as pd

from src.common import sha256
from src.paths import PROJECT_ROOT
from src.data.pmxt_archive import isolated, write_json
from src.data.pmxt_continuous import ROOT, ISOLATED, CONTINUOUS
from src.data.pmxt_diagnostics import AuditBook, REASONS
from src.data.pmxt_books import milliseconds, price_key
from src.data.pmxt_source import source_batches
from src.data.pmxt_validation import read_prefix


class ObservedBook(AuditBook):
    """Record analysis-window contradictions; preserve every v3 state change."""
    def __init__(self,capture,start,end):
        super().__init__(capture)
        self.__post_init__()
        self.analysis_start,self.analysis_end=milliseconds(start),milliseconds(end)
        self.analysis_issues=Counter()
        self.analysis_examples=[]
        self.first_issue_after_seed=None

    def _issue(self,source,reason,side=None,*,drift=False):
        if self.baseline is not None and self.first_issue_after_seed is None:
            self.first_issue_after_seed=source
        if self.analysis_start<=source<self.analysis_end:
            self.analysis_issues[reason]+=1
            if reason=='best_price_contradiction' and sum(r['side']==side for r in self.analysis_examples)<3:
                col='best_ask' if side=='SELL' else 'best_bid'
                self.analysis_examples.append(dict(source_ms=source,side=side,
                    reconstructed_possible_best=[v/10000 for v in sorted(self.possible_best(side))],
                    reported_best=[v/10000 for v in sorted({price_key(r[col]) for r in self.current_rows if not pd.isna(r.get(col))})],
                    snapshot_anchor_ms=self.baseline,already_faulty=self.faults[side]))
        return super()._issue(source,reason,side,drift=drift)


def initial_and_analysis_evidence(frame,assets,capture,start,end):
    books={t:ObservedBook(milliseconds(capture),start,end) for t in assets}
    first_scan=None
    for (stamp,token),rows in source_batches(frame.loc[(frame.timestamp>=capture)&(frame.timestamp<end)]):
        if first_scan is None and stamp>=start:
            first_scan=token_start_rows(books)
        books[token].apply_batch(stamp,rows)
    if first_scan is None:
        first_scan=token_start_rows(books)
    rows=[]
    for row in first_scan:
        book=books[row['asset_id']]
        rows.append(dict(row,analysis_best_price_contradiction_side_groups=book.analysis_issues['best_price_contradiction'],
            first_issue_after_seed_ms=book.first_issue_after_seed))
    return pd.DataFrame(rows),{t:b.analysis_examples for t,b in books.items() if b.analysis_examples}


def token_start_rows(books):
    return [dict(asset_id=t,initialized=b.baseline is not None,latest_snapshot_source_ms=b.baseline,
        causal_ask_available=b.available('SELL'),causal_ask_exact=b.exact('SELL'),
        causal_bid_available=b.available('BUY'),ask_fault_reason=b.fault_reason['SELL'],
        bid_fault_reason=b.fault_reason['BUY']) for t,b in books.items()]


def independent_ask_check(folder,token,anchor_ms,decision_ms):
    """Plain Decimal/dictionary replay, independent of BoundedBook logic.

    A diagnostic trace only: the nearby trade is reported, not used to repair
    depth or alter labels. Ambiguous same-time quantities are rejected here.
    """
    anchor=pd.Timestamp(anchor_ms,unit='ms',tz='UTC')
    decision=pd.Timestamp(decision_ms,unit='ms',tz='UTC')
    frames=[]
    for path in (Path(folder)/'raw').glob('*.parquet'):
        metadata=json.loads(path.with_suffix('.json').read_text())
        if sha256(path)!=metadata['sha256']:
            raise ValueError('Raw file changed before independent trace')
        frames.append(pd.read_parquet(path,filters=[('asset_id','==',token)]))
    frame=pd.concat(frames,ignore_index=True)
    snapshots=frame.loc[frame.timestamp.eq(anchor)&frame.event_type.eq('book')]
    decoded=[{Decimal(str(p)):Decimal(str(q)) for p,q in json.loads(raw) if Decimal(str(q))>0} for raw in snapshots.asks]
    if not decoded or any(book!=decoded[0] for book in decoded):
        raise ValueError('Independent trace needs one unambiguous anchor snapshot')
    asks=dict(decoded[0])
    original_best=min(asks)
    original_quantity=asks[original_best]
    same_time=frame.loc[frame.timestamp.eq(anchor)&frame.event_type.eq('price_change')&frame.side.eq('SELL')]
    for row in same_time.itertuples():
        if asks.get(Decimal(str(row.price)),Decimal(0))!=Decimal(str(row.size)):
            raise ValueError('Independent trace refuses ambiguous snapshot/update ordering')
    work=frame.loc[frame.timestamp.gt(anchor)&frame.timestamp.le(decision)]
    if work.event_type.eq('book').any():
        raise ValueError('Expected the latest snapshot before the decision')
    changes=work.loc[work.event_type.eq('price_change')&work.side.eq('SELL')]
    for (stamp,price),group in changes.groupby(['timestamp','price'],sort=True):
        sizes={Decimal(str(v)) for v in group['size']}
        if len(sizes)!=1:
            raise ValueError('Independent trace refuses unresolved same-time quantities')
        size=next(iter(sizes))
        price=Decimal(str(price))
        if size:asks[price]=size
        else:asks.pop(price,None)
    observed=frame.loc[frame.timestamp.eq(decision)&frame.event_type.eq('price_change')]
    reported=sorted({str(Decimal(str(v))) for v in observed.best_ask.dropna()})
    trades=frame.loc[frame.event_type.eq('last_trade_price')&frame.side.eq('BUY')&
        frame.timestamp.ge(decision)&frame.timestamp.le(decision+pd.Timedelta(seconds=10))&
        frame.price.map(lambda p:False if pd.isna(p) else Decimal(str(p))==original_best)]
    nearby=[dict(source_time=str(r.timestamp),delay_ms=(r.timestamp-decision).total_seconds()*1000,
        price=str(r.price),size=str(r.size)) for r in trades.sort_values('timestamp').itertuples()]
    return dict(asset_id=token,anchor_source_time=str(anchor),decision_source_time=str(decision),
        snapshot_best_ask=str(original_best),snapshot_best_ask_quantity=str(original_quantity),
        independently_reconstructed_best_ask=str(min(asks)),remaining_quantity=str(asks[min(asks)]),
        reported_best_asks=reported,intervening_ask_updates=len(changes),
        updates_at_original_best=int(changes.price.map(lambda p:Decimal(str(p))==original_best).sum()),
        decision_rows=[{c:str(row[c]) for c in ['timestamp','event_type','side','price','size','best_bid','best_ask']} for row in observed.to_dict('records')],
        nearby_buy_trades=nearby,
        note='Independent price-level-only trace. Trade handling, incomplete capture and timestamp semantics remain hypotheses; no depth repair or label change.')


def compare(root=ROOT):
    root=isolated(root)
    design_path=root/'study_design.json'
    design=json.loads(design_path.read_text())
    output=isolated(PROJECT_ROOT/'results/pmxt_pilot'/root.name,'results')
    output.mkdir(parents=True,exist_ok=True)
    overview=[]
    hour_tables=[]
    token_tables=[]
    reasons=[]
    examples={}
    provenance={str(design_path.relative_to(PROJECT_ROOT)):sha256(design_path)}
    for filename in ['seed_search.json','download_ledger.json']:
        provenance[str((root/filename).relative_to(PROJECT_ROOT))]=sha256(root/filename)
    for scenario,name in [('isolated',ISOLATED),('continuous',CONTINUOUS)]:
        folder=root/name
        plan=json.loads((folder/'plan.json').read_text())
        run=PROJECT_ROOT/'results/pmxt_pilot'/name/'source_validation_v3'
        summary_path=run/'summary.json'
        summary=json.loads(summary_path.read_text())
        for p,digest in design['rules_sha256'].items():
            if sha256(PROJECT_ROOT/p)!=digest:
                raise ValueError('Frozen reconstruction changed')
        for label,digest in summary['derived_sha256'].items():
            if sha256(folder/'source_validation_v3'/label)!=digest:
                raise ValueError('Derived replay changed')
        for label,digest in summary['results_sha256'].items():
            if sha256(run/label)!=digest:
                raise ValueError('Replay result changed')
        provenance[str(summary_path.relative_to(PROJECT_ROOT))]=sha256(summary_path)
        audit_path=run.parent/'unknown_audit_v1/summary.json'
        audit=json.loads(audit_path.read_text())
        if audit['replay_summary_sha256']!=sha256(summary_path):
            raise ValueError('Stale uncertainty audit')
        provenance[str(audit_path.relative_to(PROJECT_ROOT))]=sha256(audit_path)
        frame,inputs=read_prefix(folder,plan)
        if inputs!=summary['inputs']:
            raise ValueError('Raw replay context changed')
        capture=pd.Timestamp(plan['warmup_start'])
        start,end=pd.Timestamp(design['start']),pd.Timestamp(design['analysis_end'])
        tokens,trace=initial_and_analysis_evidence(frame,design['assets'],capture,start,end)
        tokens['scenario']=scenario
        token_tables.append(tokens)
        examples[scenario]=trace
        overview.append(dict(scenario=scenario,capture_start=capture,
            prefix_hours=(start-capture).total_seconds()/3600,receipt_hours=len(inputs),
            initialized_tokens_at_first_scan=int(tokens.initialized.sum()),
            causal_ask_available_tokens_at_first_scan=int(tokens.causal_ask_available.sum()),
            event_hours=summary['event_hours'],positive_hours=summary['occurrence_positive_hours'],
            negative_hours=summary['occurrence_negative_hours'],unknown_hours=summary['occurrence_unknown_hours'],
            full_ask_time_fraction=summary['full_ask_time_fraction'],full_book_time_fraction=summary['full_book_time_fraction'],
            determined_time_fraction=summary['occurrence_determined_time_fraction'],
            exact_value_hours=summary['exact_value_hours'],
            max_supported_gross_usd=summary['maximum_observed_value_lower_usd'],
            analysis_contradiction_side_groups=int(tokens.analysis_best_price_contradiction_side_groups.sum()),
            missing_seed_unknown_seconds=audit['primary_reason_seconds']['missing_initial_snapshot']))
        hours=pd.read_csv(run/'event_hours.csv',dtype={'event_id':str})
        hours['scenario']=scenario
        hour_tables.append(hours)
        for reason in REASONS:
            reasons.append(dict(scenario=scenario,reason=reason,
                primary_unknown_seconds=audit['primary_reason_seconds'][reason],
                overlapping_unknown_seconds=audit['overlapping_reason_seconds'][reason]))
    baseline,continuous=hour_tables
    identity=['event_id','hour','package_count','token_count']
    if baseline[identity].to_dict('records')!=continuous[identity].to_dict('records'):
        raise ValueError('Paired comparison changes baskets or analysis endpoints')
    overview=pd.DataFrame(overview)
    all_hours=pd.concat(hour_tables,ignore_index=True)
    tokens=pd.concat(token_tables,ignore_index=True)
    for name,table in [('overview',overview),('event_hours',all_hours),('token_start',tokens),('unknown_reasons',pd.DataFrame(reasons))]:
        table.to_csv(output/f'{name}.csv',index=False)
    baseline[['event_id','hour','label_occurrence','label_peak_value_usd']].merge(
        continuous[['event_id','hour','label_occurrence','label_peak_value_usd']],
        on=['event_id','hour'],suffixes=('_isolated','_continuous'),validate='one_to_one').to_csv(output/'paired_labels.csv',index=False)
    write_json(output/'contradiction_examples.json',examples)
    eligible=[dict(example,asset_id=t) for t,items in examples['continuous'].items() for example in items
        if example['side']=='SELL' and not example['already_faulty']]
    if eligible:
        trace=min(eligible,key=lambda r:(r['source_ms'],r['asset_id']))
        independent=independent_ask_check(root/CONTINUOUS,trace['asset_id'],trace['snapshot_anchor_ms'],trace['source_ms'])
        write_json(output/'independent_ask_check.json',independent)
    for path in (root/'forensic/raw').glob('*'):
        if path.is_file():
            provenance[str(path.relative_to(PROJECT_ROOT))]=sha256(path)
    summary=dict(scope=design['scope'],package_ids=design['package_ids'],tokens=len(design['assets']),
        paired_event_hours=len(baseline),historical_parameters_verified=False,models_trained=False,
        new_final_test_accessed=False,net_pnl_estimated=False,
        evidence_sha256=provenance,
        implementation_sha256={p:sha256(PROJECT_ROOT/p) for p in ['src/data/pmxt_continuous.py','src/reporting/pmxt_continuous.py']},
        results_sha256={p.name:sha256(p) for p in output.iterdir() if p.is_file() and p.name!='summary.json'},
        caveats=['Source-clock and snapshot semantics remain assumptions. Finite receipt padding is not proof of complete capture.',
            'Analysis contradictions count repeated side-groups, not independent losses. Different initial state can change this diagnostic.',
            'A single ladder pair does not certify its entire parent event. The subset was deliberately selected for missing initial books.',
            'Observed displayed margins are conditional gross values, not fees-verified executable profit.'])
    write_json(output/'summary.json',summary)
    print(overview.to_string(index=False))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT)
    args=parser.parse_args()
    compare(args.root)
