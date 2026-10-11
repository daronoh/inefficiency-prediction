# Profitability-oriented development results

The study ranks events from scheduled snapshots to allocate a five-event monitoring budget. The primary objective is expected simulated net P&L during the next monitoring window. This is a development comparison; the final test from 8 January 2026 remains unopened.

26,835 eligible event-windows from 48 events; 21,827 known targets and 5,008 unknown. Known hours contain 437 positive, 21,373 zero and 17 negative net outcomes. 454 hours contain attempted trades.

The best observed development result within each main model family is shown below. These winners are selected after comparing variants and do not constitute final-test model selection.

| approach | model | known_pnl_usd |
| --- | --- | --- |
| Occurrence classifier | combined_random_forest | 5942.7078 |
| Direct P&L regression | combined_reg_ensemble | 5842.3872 |
| Two-stage P&L | combined_two_stage_random_forest | 5903.5544 |
| Reference | quoted_gap_rule | 6045.2515 |
| Reference | monitor_all | 5951.6045 |

## Target and execution policy

Targets are rebuilt for every event-window independently of model selections, using 100 ms arrival, uncapped integer-depth sizing, a one-second partial-basket unwind/hold, zero historical exchange fees for the included market types and no additional imposed failure probability. Incomplete coverage or an unpriceable attempted outcome leaves the target missing. Complete no-trade hours have a zero target. Prices and quantities are fixed before arrival. The book features that depend on basket sizing, their lags and the quoted-gap rule are also rebuilt without caps.

Standalone hourly labels start with an empty reservation ledger. Evaluation below replays each policy chronologically with reservations retained across windows and folds. Summing selected labels is only a diagnostic and is not the strategy result. Unknown hours still compete for monitoring slots.

## Models and chronological evaluation

Opportunity classifiers (logistic regression, random forest and boosted trees) are refitted to the new execution-derived occurrence labels. Direct regression uses ridge regression, random forest and boosted trees to predict signed dollars. Two-stage models estimate P(attempt) and conditional net P&L, then multiply them. The conditional model includes zero and losing trading hours. Gates are unweighted classifiers; out-of-sample reliability is reported rather than assuming calibrated probabilities. Classifier ensembles average percentile ranks; economic ensembles average dollar predictions. Every family uses book-only and book-plus-metadata inputs. Hyperparameters are fixed without a search.

| fold | training_rows | trading_rows | loss_hours | train_last_time | test_first_time |
| --- | --- | --- | --- | --- | --- |
| nov_early | 4859 | 69 | 1 | 2025-10-31 22:00:00+00:00 | 2025-11-01 00:00:00+00:00 |
| nov_late | 11191 | 234 | 7 | 2025-11-21 22:00:00+00:00 | 2025-11-22 00:00:00+00:00 |
| dec_late | 17690 | 405 | 11 | 2025-12-14 22:00:00+00:00 | 2025-12-15 00:00:00+00:00 |

Training uses earlier known labels and purges the complete monitoring horizon plus 1.1 seconds. Preprocessing is fitted only on training rows; conditional preprocessing uses only earlier trading hours.

## Primary results: five events per scan

Dollar figures below come from persistent replay. Recall is capture of hours containing at least one positive attempt; it is secondary to net P&L. MAE/RMSE measure standalone dollar predictions and apply only to regression and two-stage models. Unknown execution counts qualify every known-P&L subtotal.

Book features:

| model | known_pnl_usd | completed_profit_usd | partial_pnl_usd | attempts | unknown_attempts | max_drawdown_usd | largest_reserved_cash_usd | cumulative_purchase_cash_usd | recall | precision_known | mae_usd | rmse_usd | selected_unknown |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| logistic | 5902.6282 | 6317.3745 | -414.7462 | 479 | 0 | 206.1506 | 72920.8660 | 1428952.7289 | 0.9734 | 0.0478 | nan | nan | 374.0000 |
| random_forest | 5942.4238 | 6357.1700 | -414.7462 | 510 | 0 | 205.9126 | 72920.8660 | 1461914.9334 | 0.9521 | 0.0531 | nan | nan | 1297.0000 |
| boosted_trees | 5219.1920 | 5633.9382 | -414.7462 | 510 | 0 | 205.9126 | 72920.8660 | 1395164.1652 | 0.9441 | 0.0532 | nan | nan | 1367.0000 |
| rank_ensemble | 5942.0156 | 6356.7618 | -414.7462 | 513 | 0 | 205.9126 | 72920.8660 | 1459862.3416 | 0.9654 | 0.0528 | nan | nan | 1165.0000 |
| reg_ridge | 5604.6607 | 5886.0490 | -281.3882 | 431 | 0 | 142.0794 | 72920.8660 | 1344749.7566 | 0.9149 | 0.0441 | 1.1670 | 8.4003 | 235.0000 |
| reg_random_forest | 5643.0423 | 6057.2885 | -414.2462 | 493 | 0 | 205.9126 | 52909.7930 | 1358412.9149 | 0.8910 | 0.0512 | 0.5144 | 8.3538 | 1492.0000 |
| reg_boosted_trees | 5332.9090 | 5549.3457 | -216.4367 | 391 | 0 | 133.9942 | 52909.7930 | 1233655.9378 | 0.6489 | 0.0389 | 0.6133 | 8.4568 | 1755.0000 |
| reg_ensemble | 5655.8757 | 6068.5412 | -412.6654 | 453 | 0 | 264.9606 | 52909.7930 | 1333544.5434 | 0.8697 | 0.0460 | 0.7225 | 8.3164 | 918.0000 |
| two_stage_ridge | 5843.2454 | 6255.2747 | -412.0292 | 444 | 0 | 207.1606 | 72920.8660 | 1416752.4507 | 0.8218 | 0.0418 | 0.5576 | 8.1508 | 649.0000 |
| two_stage_random_forest | 5891.4619 | 6306.2081 | -414.7462 | 498 | 0 | 205.9126 | 72920.8660 | 1444794.8953 | 0.9149 | 0.0526 | 0.4695 | 8.2460 | 1489.0000 |
| two_stage_boosted_trees | 5795.9206 | 6190.3692 | -394.4486 | 478 | 0 | 205.9126 | 72920.8660 | 1430201.0414 | 0.7979 | 0.0457 | 0.4366 | 8.2454 | 1476.0000 |
| two_stage_ensemble | 5888.8684 | 6303.6146 | -414.7462 | 500 | 0 | 205.9126 | 72920.8660 | 1452058.4888 | 0.8590 | 0.0475 | 0.4760 | 8.1775 | 1228.0000 |
| quoted_gap_rule | 6045.2515 | 6328.7206 | -283.4690 | 491 | 0 | 142.0794 | 72920.8660 | 1456722.1038 | 0.9176 | 0.0529 | nan | nan | 1518.0000 |
| depth_rule | 5741.7491 | 6095.0791 | -353.3300 | 338 | 0 | 206.1506 | 72920.8660 | 1387063.0375 | 0.6277 | 0.0393 | nan | nan | 2034.0000 |
| freshness_rule | 5838.2194 | 6252.9656 | -414.7462 | 440 | 0 | 206.1506 | 72920.8660 | 1387893.1378 | 0.8883 | 0.0422 | nan | nan | 113.0000 |
| monitor_all | 5951.6045 | 6366.3507 | -414.7462 | 521 | 0 | 205.9126 | 72920.8660 | 1462765.7527 | nan | nan | nan | nan | nan |

Book features and metadata:

| model | known_pnl_usd | completed_profit_usd | partial_pnl_usd | attempts | unknown_attempts | max_drawdown_usd | largest_reserved_cash_usd | cumulative_purchase_cash_usd | recall | precision_known | mae_usd | rmse_usd | selected_unknown |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| combined_logistic | 5925.4577 | 6340.2039 | -414.7462 | 477 | 0 | 205.9126 | 72920.8660 | 1433432.8995 | 0.9309 | 0.0504 | nan | nan | 1086.0000 |
| combined_random_forest | 5942.7078 | 6357.4540 | -414.7462 | 513 | 0 | 205.9126 | 72920.8660 | 1462056.6494 | 0.9574 | 0.0543 | nan | nan | 1409.0000 |
| combined_boosted_trees | 5929.1010 | 6343.8472 | -414.7462 | 510 | 0 | 205.9126 | 72920.8660 | 1461332.2562 | 0.9415 | 0.0534 | nan | nan | 1403.0000 |
| combined_rank_ensemble | 5939.9475 | 6354.6938 | -414.7462 | 505 | 0 | 205.9126 | 72920.8660 | 1456341.4096 | 0.9495 | 0.0533 | nan | nan | 1336.0000 |
| combined_reg_ridge | 5586.1025 | 5867.4908 | -281.3882 | 432 | 0 | 142.0794 | 72920.8660 | 1341157.3148 | 0.9122 | 0.0444 | 1.2239 | 8.4116 | 315.0000 |
| combined_reg_random_forest | 5793.7612 | 6208.0074 | -414.2462 | 488 | 0 | 205.9126 | 72920.8660 | 1429611.1960 | 0.8910 | 0.0515 | 0.5221 | 8.3548 | 1534.0000 |
| combined_reg_boosted_trees | 5271.9959 | 5537.1032 | -265.1073 | 402 | 0 | 133.9942 | 52909.7930 | 1238369.8422 | 0.6569 | 0.0399 | 0.6230 | 8.4614 | 1837.0000 |
| combined_reg_ensemble | 5842.3872 | 6255.0526 | -412.6654 | 458 | 0 | 205.9126 | 72920.8660 | 1391622.0320 | 0.8777 | 0.0480 | 0.7470 | 8.3194 | 1160.0000 |
| combined_two_stage_ridge | 5826.6760 | 6238.7052 | -412.0292 | 424 | 0 | 207.1606 | 72920.8660 | 1408855.0202 | 0.7872 | 0.0400 | 0.5445 | 8.1419 | 628.0000 |
| combined_two_stage_random_forest | 5903.5544 | 6318.3006 | -414.7462 | 496 | 0 | 205.9126 | 72920.8660 | 1444570.8027 | 0.9069 | 0.0522 | 0.4741 | 8.2474 | 1496.0000 |
| combined_two_stage_boosted_trees | 5780.2742 | 6174.3238 | -394.0496 | 468 | 0 | 205.9126 | 72920.8660 | 1432575.0299 | 0.7952 | 0.0439 | 0.4467 | 8.2463 | 1224.0000 |
| combined_two_stage_ensemble | 5876.9315 | 6291.6777 | -414.7462 | 477 | 0 | 206.1506 | 72920.8660 | 1449420.4256 | 0.8723 | 0.0452 | 0.4778 | 8.1766 | 778.0000 |
| quoted_gap_rule | 6045.2515 | 6328.7206 | -283.4690 | 491 | 0 | 142.0794 | 72920.8660 | 1456722.1038 | 0.9176 | 0.0529 | nan | nan | 1518.0000 |
| depth_rule | 5741.7491 | 6095.0791 | -353.3300 | 338 | 0 | 206.1506 | 72920.8660 | 1387063.0375 | 0.6277 | 0.0393 | nan | nan | 2034.0000 |
| freshness_rule | 5838.2194 | 6252.9656 | -414.7462 | 440 | 0 | 206.1506 | 72920.8660 | 1387893.1378 | 0.8883 | 0.0422 | nan | nan | 113.0000 |
| monitor_all | 5951.6045 | 6366.3507 | -414.7462 | 521 | 0 | 205.9126 | 72920.8660 | 1462765.7527 | nan | nan | nan | nan | nan |

Twenty seeded random policies earn mean $1,814.91, minimum $827.26, and maximum $2,422.14. Monitor-all is a reference policy, not a guaranteed profit upper bound.

An always-zero dollar prediction has MAE $0.3786 and RMSE $8.5012; the many zero hours make prediction-error metrics insufficient on their own for judging monitoring choices.

## Fold stability

| model | fold | known_pnl_usd | partial_pnl_usd | unknown_attempts |
| --- | --- | --- | --- | --- |
| logistic | nov_early | 4498.1487 | -290.4770 | 0 |
| logistic | nov_late | 1072.5509 | -23.9284 | 0 |
| logistic | dec_late | 331.9287 | -100.3408 | 0 |
| quoted_gap_rule | nov_early | 4622.7116 | -159.1998 | 0 |
| quoted_gap_rule | nov_late | 1090.6112 | -23.9284 | 0 |
| quoted_gap_rule | dec_late | 331.9287 | -100.3408 | 0 |
| reg_boosted_trees | nov_early | 3976.6543 | -147.3276 | 0 |
| reg_boosted_trees | nov_late | 975.7272 | -23.9284 | 0 |
| reg_boosted_trees | dec_late | 380.5275 | -45.1807 | 0 |
| two_stage_boosted_trees | nov_early | 4483.4075 | -290.4770 | 0 |
| two_stage_boosted_trees | nov_late | 1047.7304 | -3.6308 | 0 |
| two_stage_boosted_trees | dec_late | 264.7827 | -100.3408 | 0 |
| monitor_all | nov_early | 4508.5482 | -290.4770 | 0 |
| monitor_all | nov_late | 1111.1277 | -23.9284 | 0 |
| monitor_all | dec_late | 331.9287 | -100.3408 | 0 |

## Concentration and transfer

The following sensitivity removes the December Fed-decision event from the monitoring pool and reranks the remaining events to reallocate the freed slots. It uses the same fitted models. This is a post-hoc concentration check, not retraining without that event.

| model | known_pnl_usd | partial_pnl_usd | unknown_attempts |
| --- | --- | --- | --- |
| logistic | 697.5775 | -130.8976 | 0 |
| random_forest | 725.9560 | -130.8976 | 0 |
| boosted_trees | 723.0398 | -130.8976 | 0 |
| rank_ensemble | 724.1890 | -130.8976 | 0 |
| reg_ridge | 678.2565 | -128.8168 | 0 |
| reg_random_forest | 687.3977 | -130.3976 | 0 |
| reg_boosted_trees | 659.3944 | -74.6675 | 0 |
| reg_ensemble | 678.7621 | -128.8168 | 0 |
| two_stage_ridge | 620.7912 | -128.1806 | 0 |
| two_stage_random_forest | 717.1640 | -130.8976 | 0 |
| two_stage_boosted_trees | 626.5934 | -110.6000 | 0 |
| two_stage_ensemble | 667.3340 | -130.8976 | 0 |
| quoted_gap_rule | 726.5680 | -130.8976 | 0 |
| depth_rule | 538.1329 | -69.9813 | 0 |
| freshness_rule | 629.6294 | -130.8976 | 0 |
| monitor_all | 729.1503 | -130.8976 | 0 |

The separate family-holdout models remove the evaluated broad topic from training while preserving chronology. These checks use boosted classifiers, direct regression and two-stage models in both feature views.

| model | known_pnl_usd | partial_pnl_usd | unknown_attempts |
| --- | --- | --- | --- |
| family_holdout_boosted_trees | 5233.3030 | -272.9770 | 0 |
| family_holdout_combined_boosted_trees | 5235.8145 | -272.9770 | 0 |
| family_holdout_combined_reg_boosted_trees | 5962.0553 | -345.6794 | 0 |
| family_holdout_combined_two_stage_boosted_trees | 3893.1905 | -213.8586 | 0 |
| family_holdout_reg_boosted_trees | 5945.2958 | -384.5827 | 0 |
| family_holdout_two_stage_boosted_trees | 5620.9271 | -251.1294 | 0 |

Event contributions for the book-only two-stage boosted model:

| event_slug | known_pnl_usd | attempts | unknown_attempts |
| --- | --- | --- | --- |
| fed-decision-in-december | 5187.9743 | 203 | 0 |
| fed-decision-in-january | 427.2223 | 92 | 0 |
| honduras-presidential-election | 109.5198 | 18 | 0 |
| what-price-will-ethereum-hit-in-november-2025 | 55.2204 | 41 | 0 |
| what-price-will-bitcoin-hit-in-november-2025 | 36.4240 | 26 | 0 |
| avatar-fire-and-ash-opening-weekend-box-office | 22.4392 | 44 | 0 |
| largest-company-end-of-november | 5.6658 | 10 | 0 |
| gensyn-public-sale-total-commitments | 4.0720 | 5 | 0 |
| which-party-holds-the-most-seats-after-argentina-deputies-election | 2.1215 | 4 | 0 |
| will-the-government-shutdown-end-by-763 | 1.6082 | 11 | 0 |
| which-company-has-the-best-ai-model-end-of-january | 1.2320 | 3 | 0 |
| monad-market-cap-fdv-one-day-after-launch | 0.5285 | 3 | 0 |
| boxing-jake-paul-vs-anthony-joshua-third-option-included | -58.1073 | 18 | 0 |

## Uncertainty and features

Paired intervals resample broad topic families using each policy's actual replay returns. They are exploratory 95% percentile intervals, with few independent families and no multiple-comparison correction. An interval crossing zero does not establish an advantage over the quoted-gap rule.

| model | pnl_difference_usd | lower95 | upper95 |
| --- | --- | --- | --- |
| boosted_trees | -826.0596 | -2491.6561 | 12.5429 |
| logistic | -142.6233 | -362.2967 | 16.9241 |
| random_forest | -102.8278 | -339.1139 | 29.5161 |
| rank_ensemble | -103.2360 | -339.0014 | 31.1235 |
| reg_boosted_trees | -712.3426 | -2214.5874 | 92.2028 |
| reg_ensemble | -389.3758 | -1103.1060 | 15.2896 |
| reg_random_forest | -402.2093 | -1210.6999 | 5.3786 |
| reg_ridge | -440.5908 | -1266.1494 | 38.7605 |
| two_stage_boosted_trees | -249.3309 | -758.3107 | 26.4276 |
| two_stage_ensemble | -156.3832 | -451.6961 | 12.0321 |
| two_stage_random_forest | -153.7896 | -459.3301 | 2.1072 |
| two_stage_ridge | -202.0061 | -491.2437 | 8.4096 |

Book-only boosted feature-removal models are refitted. Their actual replay P&L is:

| model | known_pnl_usd | partial_pnl_usd | unknown_attempts |
| --- | --- | --- | --- |
| reg_without_freshness_quality | 5193.3008 | -76.5226 | 0 |
| reg_without_history | 5067.4715 | -185.0917 | 0 |
| reg_without_quoted_gap | 3295.4466 | -216.4367 | 0 |
| two_stage_without_freshness_quality | 5903.7693 | -407.6867 | 0 |
| two_stage_without_history | 5815.4615 | -392.3286 | 0 |
| two_stage_without_quoted_gap | 5808.0020 | -399.4547 | 0 |

Grouped and individual within-scan permutation diagnostics appear in notebook 02. Their score is the change in selected standalone net-P&L labels, not a rerun of the persistent simulator and not a causal effect. Gate reliability bins and Brier scores are also retained in diagnostics/.

## Interpretation limits

100 ms is an assumed arrival delay, not a measured historical latency. Displayed-depth fills assume the recorded quotes are attainable. There is no quote-age cutoff, and carry-forward assumes complete capture of changes within recorded extents. Funding, settlement cash release, operating costs and a shared portfolio cash budget are not simulated. Maximum order cash and cumulative purchase cash are exposure and turnover diagnostics, not an investment-return denominator. Completed baskets are valued at guaranteed payout, so the P&L curve is not a settled cash balance. Frozen retrospective universe selection and concentrated returns limit generalisation.

The previous capped and fixed-ranking execution studies remain in results/baseline/ as historical references. Their targets and quoted-gap scores differ, so dollar changes between studies cannot be attributed solely to changing the learning objective. All methods in the new tables share the same rebuilt targets, snapshot features, eligibility, folds and execution policy.

See [dataset documentation](../dataset.md) and [simulator documentation](../simulator.md).
