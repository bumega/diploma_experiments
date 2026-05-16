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
RESULTS_ROOT = Path("quick_breal_search_ablation")


def _make_first_fold(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    tr, te = next(iter(skf.split(X, y_bin)))
    return tr, te


def _evaluate_method(
    *,
    method_name: str,
    reducer_factory,
    unary_params: dict,
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
    red = reducer_factory()
    Ztr = red.fit_transform(Xtr_s, ytr_orig)
    Zte = red.transform(Xte_s)

    cls_rows: list[dict] = []
    for mname, base_model in models.items():
        model = core._clone_estimator(base_model)
        model.fit(Ztr, ytr_bin)
        score = core._get_score_vector(model, Zte)
        cls_rows.append(
            {
                "method": method_name,
                "q": Q,
                "model": mname,
                "AUC": float(roc_auc_score(yte_bin, score)),
                "PR_AUC": float(average_precision_score(yte_bin, score)),
            }
        )

    unary_eval = _strict_unary_on_reduced(
        Ztr=np.asarray(Ztr, dtype=np.float32),
        ytr_orig=np.asarray(ytr_orig, dtype=np.int64),
        Zte=np.asarray(Zte, dtype=np.float32),
        yte_orig=np.asarray(yte_orig, dtype=np.int64),
        unary_params=unary_params,
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


def _ensemble_factory(
    *,
    d_hidden: int,
    n_hidden_layers: int,
    num_epochs: int,
    batch_size: int,
    num_models: int,
    num_attempts: int,
    num_coords: int,
    num_samples: int,
    cG: float,
) -> tuple:
    def build() -> EnsembleReducer:
        return EnsembleReducer(
            d_hidden=d_hidden,
            n_hidden_layers=n_hidden_layers,
            num_epochs=num_epochs,
            batch_size=batch_size,
            n_select=Q,
            num_models=num_models,
            num_attempts=num_attempts,
            num_coords=num_coords,
            num_samples=num_samples,
            seed=SEED,
            output=True,
            device="cuda",
            use_gpu_shap=True,
            max_models_per_batch=500,
            max_shap_models_per_batch=128,
            checkpoint_every=5,
            checkpoint_n_grid=19,
            split_mode="split_60_20_20",
            cG=cG,
        )

    unary_params = {
        "d_hidden": d_hidden,
        "n_hidden_layers": n_hidden_layers,
        "num_epochs": max(8, min(num_epochs, 20)),
        "batch_size": batch_size,
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "cG": cG,
        "seed": SEED,
        "device": "cuda",
    }
    return build, unary_params


def main() -> None:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)

    X, y = make_dataset_B_real(seed=SEED)
    tr, te = _make_first_fold(X, y)

    methods: list[tuple[str, object, dict]] = [
        (
            "PCA",
            lambda: PCAReducer(Q),
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 8,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": "cuda",
            },
        ),
        (
            "PLS",
            lambda: PLSReducer(Q),
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 8,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": "cuda",
            },
        ),
        (
            "HSIC_Lasso",
            lambda: HSICSelector(Q),
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 8,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": "cuda",
            },
        ),
    ]

    ensemble_specs = [
        ("Ens_base_4x20_cg025", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=4, num_attempts=20, num_coords=4, num_samples=4, cG=0.25)),
        ("Ens_base_4x20_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=4, num_attempts=20, num_coords=4, num_samples=4, cG=0.50)),
        ("Ens_coords5_samples10_cg025", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=4, num_attempts=20, num_coords=5, num_samples=10, cG=0.25)),
        ("Ens_coords5_samples10_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=4, num_attempts=20, num_coords=5, num_samples=10, cG=0.50)),
        ("Ens_coords6_samples12_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=4, num_attempts=20, num_coords=6, num_samples=12, cG=0.50)),
        ("Ens_models8_attempts40_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=8, num_attempts=40, num_coords=5, num_samples=10, cG=0.50)),
        ("Ens_models12_attempts80_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=8, batch_size=2048, num_models=12, num_attempts=80, num_coords=5, num_samples=10, cG=0.50)),
        ("Ens_epochs20_models8_cg050", dict(d_hidden=32, n_hidden_layers=2, num_epochs=20, batch_size=2048, num_models=8, num_attempts=40, num_coords=5, num_samples=10, cG=0.50)),
        ("Ens_hidden64_epochs20_cg050", dict(d_hidden=64, n_hidden_layers=2, num_epochs=20, batch_size=2048, num_models=8, num_attempts=40, num_coords=5, num_samples=10, cG=0.50)),
        ("Ens_hidden16_epochs20_cg050", dict(d_hidden=16, n_hidden_layers=2, num_epochs=20, batch_size=2048, num_models=8, num_attempts=40, num_coords=5, num_samples=10, cG=0.50)),
    ]

    for name, spec in ensemble_specs:
        factory, unary_params = _ensemble_factory(**spec)
        methods.append((name, factory, unary_params))

    cls_parts: list[dict] = []
    unary_parts: list[dict] = []

    for method_name, factory, unary_params in methods:
        print(f"[run] {method_name}")
        cls_rows, unary_row = _evaluate_method(
            method_name=method_name,
            reducer_factory=factory,
            unary_params=unary_params,
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
