import json
import unittest

from src.data.pmxt_reconciliation import DiagnosticBook


def snapshot(asks):
    return dict(event_type='book',bids=json.dumps([['0.3','20']]),asks=json.dumps(asks))


def change(side,price,size,best_ask):
    return dict(event_type='price_change',side=side,price=price,size=size,best_bid='0.3',best_ask=best_ask)


class ReconciliationTests(unittest.TestCase):
    def test_trade_does_not_repair_but_explicit_depth_update_does(self):
        book=DiagnosticBook()
        book.apply(0,[snapshot([['0.4','10'],['0.5','20']])])
        book.apply(10,[change('BUY','0.3','21','0.5')])
        book.apply(29,[dict(event_type='last_trade_price',side='BUY',price='0.4',size='10')])
        self.assertFalse(book.compatible('SELL'))
        book.apply(50,[change('SELL','0.4','0','0.5')])
        self.assertTrue(book.compatible('SELL'))
        self.assertEqual(book.episodes[0]['resolution'],'level_update_original_quote')
        self.assertEqual(book.episodes[0]['duration_ms'],40)

    def test_quote_return_is_not_depth_repair(self):
        book=DiagnosticBook()
        book.apply(0,[snapshot([['0.4','10'],['0.5','20']])])
        book.apply(10,[change('BUY','0.3','21','0.5')])
        book.apply(20,[change('BUY','0.3','22','0.4')])
        self.assertEqual(book.episodes[0]['resolution'],'quote_revision_without_depth_repair')

    def test_checkpoint_checks_untouched_levels_before_snapshot_replacement(self):
        book=DiagnosticBook()
        book.apply(0,[snapshot([['0.4','10'],['0.5','20']])])
        book.apply(20,[change('SELL','0.4','11','0.4'),snapshot([['0.4','11'],['0.5','5']])])
        asks=[r for r in book.checkpoints if r['side']=='SELL'][0]
        self.assertTrue(asks['same_time_level_updates'])
        self.assertTrue(asks['top_price_and_quantity_match'])
        self.assertFalse(asks['untouched_depth_match'])
        self.assertEqual(asks['untouched_level_differences'],1)

    def test_conflicting_depth_stays_ambiguous_until_snapshot(self):
        book=DiagnosticBook()
        book.apply(0,[snapshot([['0.4','10']])])
        book.apply(10,[change('SELL','0.4','5','0.4'),change('SELL','0.4','7','0.4')])
        self.assertFalse(book.compatible('SELL'))
        book.apply(11,[change('SELL','0.4','6','0.4')])
        self.assertFalse(book.compatible('SELL'))
        book.apply(20,[snapshot([['0.4','6']])])
        self.assertTrue(book.compatible('SELL'))


if __name__ == '__main__':
    unittest.main()
