"""Freeze final evaluation choices after a timestamp-only coverage audit."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.common import sha256
from src.data.final_coverage import verify as verify_coverage
from src.paths import PROJECT_ROOT


def freeze(audit, output):
    audit, output = Path(audit), Path(output)
    summary = verify_coverage(audit)
    root = PROJECT_ROOT/'results/final_preparation'
    complete = json.loads((root/'complete.json').read_text())
    if complete['final_lockbox_opened'] or complete['execution_outcomes_used']:
        raise ValueError('Expected models fitted before final-outcome access')
    for name, digest in complete['files_sha256'].items():
        if sha256(root/name) != digest:
            raise ValueError(f'Prepared artifact changed: {name}')
    bundle = json.loads((root/'final_refit_bundle.json').read_text())
    if pd.Timestamp(bundle['trained_before']) != pd.Timestamp(summary['final_start']):
        raise ValueError('Model fitting and final evaluation boundary differ')
    prepared = json.loads((root/'design.json').read_text())
    for name, digest in prepared['source_sha256'].items():
        if sha256(PROJECT_ROOT/name) != digest:
            raise ValueError(f'Prepared source changed: {name}')
    output.mkdir(parents=True, exist_ok=False)
    events = pd.read_csv(audit/'event_inventory.csv')
    pool = pd.read_csv(audit/'hourly_pool.csv', parse_dates=['time'])
    primary = pool.loc[pool.scope.eq('frozen_supported_universe')]
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True, layout='constrained')
    for ax, scope in zip(axes, ('frozen_supported_universe', 'development_events_only')):
        part = pool.loc[pool.scope.eq(scope)]
        ax.step(part.time, part.eligible_events, where='post', label='Events with ranking recordings')
        ax.step(part.time, part.extent_bracketed_events, where='post', label='All member extents span the next window')
        ax.axhline(3, color='black', linestyle='--', alpha=.55, label='Primary monitoring budget N=3')
        ax.set_ylabel('Event count'); ax.set_ylim(-.2, 5.5)
        ax.set_title('Original frozen supported universe' if scope == 'frozen_supported_universe' else 'Development events only (secondary)')
        ax.legend(loc='upper right', fontsize=8)
    axes[-1].set_xlabel('Source time (UTC)')
    fig.suptitle('Final-period timestamp audit: recording evidence only')
    fig.savefig(output/'coverage.png', dpi=140); plt.close(fig)
    model_files = {name:digest for name,digest in complete['files_sha256'].items()
                   if name.startswith('final_models/') or name == 'final_refit_bundle.json'}
    protocol = dict(version='final_evaluation_protocol_v1', status='frozen_before_final_outcomes',
        final_outcomes_opened=False, coverage_timestamps_inspected=True,
        period=dict(start=summary['final_start'], end_exclusive=summary['final_end_exclusive'],
                    endpoint_rule=summary['endpoint_rule'], scheduled_hours=summary['scheduled_hours']),
        cadence_minutes=60, horizon_minutes=60, primary_top_n=3, secondary_top_n=5,
        primary_scope='frozen_supported_universe', secondary_scope='development_events_only',
        universe=dict(events=events.event_slug.tolist(),
            primary='All 51 original structurally supported events: 46 negRisk and five audited ladders; no discovery or return-based additions.',
            previously_unseen_events=events.loc[~events.development_event,'event_slug'].tolist(),
            ladders='Keep original audited windows. No final-period ladder entries are supported. Do not extend rule validity by extending a loader date.',
            negrisk='Frozen exhaustive membership. Original Jan8 end was the administrative development cutoff; recording support is audited separately. No future resolved outcomes become predictors.'),
        opportunity=dict(primary_model='n3_tabm_two_stage', primary_metric='peak_value_recall@3',
            primary_comparator='quoted_gap_rule', occurrence_reference='fixed_book_logistic',
            secondary_metrics=['opportunity recall','known precision','average precision','new-onset capture absent at scan',
                               'competitive-hour capture','selected unknown fraction','token monitoring burden'],
            target='Largest qualifying displayed dollar margin of one basket at one observed state during the monitoring window; not summed tradable profit.',
            qualification='Same uncapped integer-depth quote qualification as frozen development labels; minimum five shares or exchange minimum, positive gross and worst-limit margins.',
            unknowns='Rank before labels. Unknown rows consume slots; no dropping them or converting to zero. Report known denominators, selected unknowns and witnessed-in-unknown diagnostics separately.',
            zero_denominators='Undefined capture/precision/AP remain missing with counts; no forced zero or claim of superiority.'),
        policies=bundle['policies'],
        references=dict(quoted_gap='x_best_quoted_net_usd, missing values receive -1e12',
            random=dict(realizations=20, seeds=list(range(20261001,20261021)),
                        selection='Uniform sampling without replacement per scan; predeclared seeded streams; independent fixed policies replayed with their own persistent ledgers.',
                        opportunity='Also report exact expected capture using min(N,M)/M over the entire eligible pool.'),
            monitor_all='Select every eligible event as a resource reference. It is not an execution-PnL upper bound.',
            identical_policies='The two final forest names share one fitted artifact. Preserve both labels and verify identical predictions; do not count them as independent evidence.'),
        ordering='Descending score; ascending event_slug breaks ties, independently per UTC scan. Select min(N,M); no learned abstention.',
        observation_mask='Match development: required tokens seen at scan, active package, and scan inside event-level recording extent. Last-per-token endpoints never filter ranked rows. The offline event extent mask cannot be used as a live deactivation signal.',
        feature_history='Use backward-as-of snapshots and immediately previous scheduled snapshot only. Include Jan7 23:00 feature context for Jan8 00:00, without fitting. Never fill from future states.',
        fitting='Use saved full-development bundle; no tuning, refitting, seed selection, feature selection or calibration using final outcomes.',
        execution=prepared['execution'], latency_sensitivity_s=prepared['latency_sensitivity_s'],
        economic=dict(primary_arrival_s=.1, capital_cap_usd=None, share_cap=None,
            random_failure_probability=0, unwind_delay_s=1., fees='Frozen zero-exchange-fee cohort scenario; additional attempt costs zero.',
            initial_state='Empty inventory and liquidity ledger at final start, shared persistently across final hours for each independent policy/scenario. Do not add in-sample final-model development trading.',
            terminal_state='Entries only within final windows; load recorded exit context through end + 1.1 seconds. Later context may resolve existing attempts only. Unsupported exits remain unknown; no forced profitable liquidation.',
            metrics=['known net PnL','completed basket margin','partial fill PnL','unknown attempts and committed cash',
                     'completion rate','drawdown','no-settlement funding scenario','opportunity-to-trade conversion'],
            accounting='Same persistent quote reservations and one-second partial unwind as development. Completed guaranteed margin is booked at completion, not cash settlement. Unknown PnL is counted and disclosed.',
            uncertainty='Displayed depth, information latency, competition, queue position and settlement funding remain limitations; no live-profitability claim.'),
        diagnostics=dict(full_period='Always report entire fixed interval, including empty and noncompetitive hours.',
            competitive='Report subset with M>N separately, using full eligible pool before labels; do not replace full-period result.',
            concentration='Event and family contributions; leave each event out and rerank as a symmetric secondary sensitivity at primary budget/latency, without retraining.',
            uncertainty='Paired event/family contributions and exploratory family bootstrap, seed 2026, 10000 draws where at least two families and nonzero denominators exist. State small cluster counts; no independent-hour tests or unadjusted multiple-comparison significance claims.',
            n5='Secondary resource reference: all 632 audited hours have M<=5, so every policy selects the same pool at N=5. Verify equality rather than claim ranking skill.'),
        integrity=dict(audit_completion_sha256=sha256(audit/'complete.json'),
            prepared_completion_sha256=sha256(root/'complete.json'), final_model_files_sha256=model_files,
            prepared_source_sha256=prepared['source_sha256'],
            raw_inputs='Before label/economic evaluation verify whole raw-file hashes against frozen development inventories where present; freeze additional supported input files explicitly. Timestamp-only audit is not full price-file verification.'),
        changes='Any change after final outcome access creates an explicitly exploratory extension and cannot replace this fixed comparison.',
        evaluation_runner='Not executed or implemented by this audit. Implement as a separate explicit final-only path; preserve development access guards.',
        selection='TabM is the predeclared value-model candidate, fixed logistic the occurrence reference. Compare all saved policies; no final-test-driven model retuning.')
    (output/'protocol.json').write_text(json.dumps(protocol, indent=2)+'\n', encoding='utf-8')
    files = {p.name:sha256(p) for p in output.iterdir() if p.is_file()}
    (output/'complete.json').write_text(json.dumps(dict(files_sha256=files,
        source_sha256=sha256(Path(__file__)), audit_completion_sha256=sha256(audit/'complete.json'),
        final_outcomes_opened=False), indent=2)+'\n', encoding='utf-8')
    return protocol


def verify(output, audit):
    output, audit = Path(output), Path(audit)
    verify_coverage(audit)
    complete = json.loads((output/'complete.json').read_text())
    if complete['final_outcomes_opened'] or sha256(Path(__file__)) != complete['source_sha256']:
        raise ValueError('Expected frozen protocol without final-outcome access')
    if sha256(audit/'complete.json') != complete['audit_completion_sha256']:
        raise ValueError('Recording audit changed')
    for name, digest in complete['files_sha256'].items():
        if sha256(output/name) != digest:
            raise ValueError(f'Frozen protocol artifact changed: {name}')
    return json.loads((output/'protocol.json').read_text())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', type=Path, default=PROJECT_ROOT/'results/final_coverage')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    freeze(args.audit, args.output)
