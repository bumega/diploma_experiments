from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler

import nirs_core as core
from datasets import make_dataset_B_real
from evaluation import _strict_unary_on_reduced
from reducers import EnsembleReducer, HSICSelector, PCAReducer, PLSReducer


SEED = 41
Q = 5
RESULTS_ROOT = Path("quick_breal_ablation")


SHORT_COMMON = {
    "d_hidden": 32,
    "n_hidden_layers": 2,
    "num_epochs": 8,
    "batch_size": 2048,
    "num_models": 4,
    "num_attempts": 20,
    "seed": SEED,
    "output": True,
    "device": "cuda",
    "use_gpu_shap": True,
    "max_models_per_batch": 500,
    "max_shap_models_per_batch": 128,
    "checkpoint_every": 5,
    "checkpoint_n_grid": 19,
    "split_mode": "split_60_20_20",
}

UNARY_PARAMS = {
    "d_hidden": 32,
    "n_hidden_layers": 2,
    "num_epochs": 8,
    "batch_size": 2048,
    "checkpoint_every": 5,
    "checkpoint_n_grid": 19,
    "cG": 0.25,
    "seed": SEED,
    "device": "cuda",
}


def _make_first_fold(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    tr, te = next(iter(skf.split(X, y_bin)))
    return tr, te


def _evaluate_method(
    *,
    method_name: str,
    reducer_ctor,
    reducer_kwargs: dict,
    X: np.ndarray,
    y: np.ndarray,
    tr: np.ndarray,
    te: np.ndarray,
) -> tuple[list[dict], dict]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)

    Xtr, Xte = X[tr], X[te]
    ytr_bin, yte_bin = y_bin[tr], y_bin[te]
    ytr_orig, yte_orig = y_orig[tr], y_orig[te]

    scaler = StandardScaler()
    Xtr_s = scaler.fit_transform(Xtr)
    Xte_s = scaler.transform(Xte)

    models = core.get_models(seed=SEED + 1)

    try:
        red = reducer_ctor(**reducer_kwargs)
    except TypeError:
        if "n_select" in reducer_kwargs:
            kwargs2 = dict(reducer_kwargs)
            q = kwargs2.pop("n_select")
            red = reducer_ctor(q=q, **kwargs2)
        else:
            raise
    Ztr = red.fit_transform(Xtr_s, ytr_orig)
    Zte = red.transform(Xte_s)

    cls_rows: list[dict] = []
    for mname, base_model in models.items():
        m1 = core._clone_estimator(base_model)
        m1.fit(Ztr, ytr_bin)
        p1 = core._get_score_vector(m1, Zte)
        cls_rows.append(
            {
                "method": method_name,
                "q": Q,
                "model": mname,
                "AUC": float(roc_auc_score(yte_bin, p1)),
                "PR_AUC": float(average_precision_score(yte_bin, p1)),
            }
        )

    unary_eval = _strict_unary_on_reduced(
        Ztr=np.asarray(Ztr, dtype=np.float32),
        ytr_orig=np.asarray(ytr_orig, dtype=np.int64),
        Zte=np.asarray(Zte, dtype=np.float32),
        yte_orig=np.asarray(yte_orig, dtype=np.int64),
        unary_params=UNARY_PARAMS,
    )
    unary = unary_eval["test_metrics"]
    unary_row = {
        "method": method_name,
        "q": Q,
        "S_test": float(unary["S"]),
        "F12": float(unary["F12"]),
        "G12": float(unary["G12"]),
        "coverage": float(unary["coverage"]),
        "conflict_rate": float(unary["conflict_rate"]),
        "beta_pos": float(unary_eval["beta_pos"]),
        "beta_neg": float(unary_eval["beta_neg"]),
        "best_epoch": int(unary_eval["best_epoch"]),
        "cG": float(unary_eval["cG"]),
        "selected_indices": ",".join(map(str, np.asarray(getattr(red, "selected_indices_", []), dtype=int).tolist())),
    }
    return cls_rows, unary_row


def main() -> None:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)

    X, y = make_dataset_B_real(seed=SEED)
    tr, te = _make_first_fold(X, y)

    methods = [
        ("PCA", lambda n_select: PCAReducer(n_select), {"n_select": Q}),
        ("PLS", lambda n_select: PLSReducer(n_select), {"n_select": Q}),
        ("HSIC_Lasso", lambda n_select: HSICSelector(n_select), {"n_select": Q}),
        (
            "Ensemble_baseline_short",
            EnsembleReducer,
            {
                **SHORT_COMMON,
                "n_select": Q,
                "num_coords": 4,
                "num_samples": 4,
                "cG": 0.25,
            },
        ),
        (
            "Ensemble_coords5_samples10_cg025",
            EnsembleReducer,
            {
                **SHORT_COMMON,
                "n_select": Q,
                "num_coords": 5,
                "num_samples": 10,
                "cG": 0.25,
            },
        ),
        (
            "Ensemble_coords5_samples10_cg050",
            EnsembleReducer,
            {
                **SHORT_COMMON,
                "n_select": Q,
                "num_coords": 5,
                "num_samples": 10,
                "cG": 0.50,
            },
        ),
        (
            "Ensemble_coords5_samples10_cg010",
            EnsembleReducer,
            {
                **SHORT_COMMON,
                "n_select": Q,
                "num_coords": 5,
                "num_samples": 10,
                "cG": 0.10,
            },
        ),
    ]

    cls_parts: list[dict] = []
    unary_parts: list[dict] = []

    for method_name, ctor, kwargs in methods:
        print(f"[run] {method_name}")
        cls_rows, unary_row = _evaluate_method(
            method_name=method_name,
            reducer_ctor=ctor,
            reducer_kwargs=kwargs,
            X=X,
            y=y,
            tr=tr,
            te=te,
        )
        cls_parts.extend(cls_rows)
        unary_parts.append(unary_row)

    df_cls = pd.DataFrame(cls_parts)
    df_un = pd.DataFrame(unary_parts)

    summary_cls = (
        df_cls.groupby("method")
        .agg(
            auc_mean=("AUC", "mean"),
            pr_mean=("PR_AUC", "mean"),
            auc_best=("AUC", "max"),
            pr_best=("PR_AUC", "max"),
        )
        .reset_index()
        .sort_values(["auc_mean", "pr_mean"], ascending=False)
    )

    df_cls.to_csv(RESULTS_ROOT / "classical_by_model.csv", index=False)
    df_un.to_csv(RESULTS_ROOT / "unary_summary.csv", index=False)
    summary_cls.to_csv(RESULTS_ROOT / "classical_summary.csv", index=False)

    with open(RESULTS_ROOT / "meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "seed": SEED,
                "q": Q,
                "train_size": int(len(tr)),
                "test_size": int(len(te)),
                "methods": [name for name, _, _ in methods],
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print("CLASSICAL_SUMMARY")
    print(summary_cls.to_csv(index=False))
    print("UNARY_SUMMARY")
    print(df_un.to_csv(index=False))


if __name__ == "__main__":
    main()
