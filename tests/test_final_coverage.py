import unittest
from unittest.mock import patch
from pathlib import Path

import pandas as pd

from src.data.final_coverage import endpoint, event_availability, pool_summary, timestamp_history


START = pd.Timestamp('2026-01-08', tz='UTC')
HOUR = pd.Timedelta(hours=1)


class FinalCoverageTests(unittest.TestCase):
    def plan(self, end=START+3*HOUR):
        return dict(event_slug='example',stratum='negrisk',packages=[(['a','b'],START,end)])

    def test_endpoint_uses_full_recorded_tail_without_prices_or_labels(self):
        self.assertEqual(endpoint(pd.Series([START+HOUR,START+3*HOUR+pd.Timedelta(minutes=29)]),START,'60min'),START+3*HOUR)
        with self.assertRaises(ValueError):
            endpoint(pd.Series([START+pd.Timedelta(minutes=59)]),START,'60min')

    def test_expired_member_still_ranks_but_lacks_full_extent(self):
        extents={'a':dict(first=START-HOUR,last=START+3*HOUR),
                 'b':dict(first=START-HOUR,last=START-pd.Timedelta(minutes=1))}
        rows=event_availability(self.plan(),extents,pd.DatetimeIndex([START]),HOUR,set())
        self.assertTrue(rows.recording_eligible.iloc[0])
        self.assertFalse(rows.all_member_extent_bracketed.iloc[0])
        self.assertFalse(rows.development_event.iloc[0])
        self.assertEqual(rows.members_ended_before_scan.iloc[0],1)

    def test_no_future_seed_and_half_open_contract_boundary(self):
        extents={'a':dict(first=START,last=START+3*HOUR),
                 'b':dict(first=START+pd.Timedelta(minutes=1),last=START+3*HOUR)}
        windows=pd.date_range(START,periods=3,freq='h')
        rows=event_availability(self.plan(START+2*HOUR),extents,windows,HOUR,{'example'})
        self.assertEqual(len(rows),2)
        self.assertEqual(rows.recording_eligible.tolist(),[False,True])
        self.assertTrue(rows.all_member_extent_bracketed.iloc[1])

    def test_budget_counts_unknown_extent_members_and_keeps_empty_hours(self):
        extents={'a':dict(first=START-HOUR,last=START+3*HOUR),
                 'b':dict(first=START-HOUR,last=START-HOUR)}
        plans=[dict(self.plan(),event_slug=str(i)) for i in range(4)]
        rows=pd.concat([event_availability(p,extents,pd.DatetimeIndex([START]),HOUR,{'0'}) for p in plans])
        summary=pool_summary(rows,pd.DatetimeIndex([START,START+HOUR]))
        all_events=summary.loc[summary.scope.eq('frozen_supported_universe')]
        self.assertEqual(all_events.eligible_events.tolist(),[4,0])
        self.assertEqual(all_events.extent_bracketed_events.tolist(),[0,0])
        self.assertEqual(all_events.selective_at_3.tolist(),[True,False])
        self.assertEqual(all_events.monitor_all_at_3.tolist(),[False,False])

    def test_timestamp_projection_does_not_request_orderbook_columns(self):
        frame=pd.DataFrame({'snapshot_time':[START+HOUR,START,START]})
        with patch('src.data.final_coverage.pd.read_parquet',return_value=frame) as reader:
            times=timestamp_history(Path('synthetic.parquet'))
        reader.assert_called_once_with(Path('synthetic.parquet'),columns=['snapshot_time'])
        self.assertEqual(times.tolist(),[START,START+HOUR])


if __name__ == '__main__':
    unittest.main()
