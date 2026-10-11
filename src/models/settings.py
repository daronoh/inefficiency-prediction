"""Predeclared model comparison settings and chronological development folds."""
SEED = 20261001
FOLDS = [('nov_early', '2025-11-01', '2025-11-22'),
         ('nov_late', '2025-11-22', '2025-12-15'),
         ('dec_late', '2025-12-15', '2026-01-08')]
MODEL_NAMES = ['logistic', 'random_forest', 'boosted_trees']
FEATURE_SETS = ('book', 'metadata', 'combined')


def method_name(model, feature_set='book', heldout=False):
    name = model if feature_set == 'book' else f'{feature_set}_{model}'
    return f'family_holdout_{name}' if heldout else name


def ensemble_name(feature_set='book', heldout=False):
    if heldout:
        return 'family_holdout_ensemble' if feature_set == 'book' else f'family_holdout_{feature_set}_ensemble'
    return method_name('rank_ensemble', feature_set)


CLASSIFIER_METHODS = [method_name(model, feature_set, heldout)
                      for heldout in (False, True) for feature_set in FEATURE_SETS for model in MODEL_NAMES]
BUDGETS = (1, 3, 5, 10)

