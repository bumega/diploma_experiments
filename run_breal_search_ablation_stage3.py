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
QS = [5, 10, 15]
FOLD_IDS = [1]
RESULTS_ROOT = Path("quick_breal_search_ablation_stage3")


def _make_folds(X: np.ndarray, y: np.ndarray) -> list[tuple[np.ndarray, np.ndarray]]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    return list(skf.split(X, y_bin))


def _resolve_q_param(spec: dict, key: str, q: int):
    by_q = spec.get(f"{key}_by_q")
    if by_q is not None:
        return by_q[q]
    return spec[key]


def _evaluate_method(
    *,
    method_name: str,
    q: int,
    fold_id: int,
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

    models = core.get_models(seed=SEED + fold_id)
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
                "q": q,
                "fold_id": fold_id,
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
        "q": q,
        "fold_id": fold_id,
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


def _ensemble_factory(q: int, spec: dict) -> tuple:
    d_hidden = int(_resolve_q_param(spec, "d_hidden", q))
    n_hidden_layers = int(_resolve_q_param(spec, "n_hidden_layers", q))
    num_epochs = int(_resolve_q_param(spec, "num_epochs", q))
    batch_size = int(_resolve_q_param(spec, "batch_size", q))
    num_models = int(_resolve_q_param(spec, "num_models", q))
    num_attempts = int(_resolve_q_param(spec, "num_attempts", q))
    num_coords = int(_resolve_q_param(spec, "num_coords", q))
    num_samples = int(_resolve_q_param(spec, "num_samples", q))
    cG = float(_resolve_q_param(spec, "cG", q))
    max_models_per_batch = int(spec.get("max_models_per_batch", 500))
    max_shap_models_per_batch = int(spec.get("max_shap_models_per_batch", 128))

    def build() -> EnsembleReducer:
        return EnsembleReducer(
            d_hidden=d_hidden,
            n_hidden_layers=n_hidden_layers,
            num_epochs=num_epochs,
            batch_size=batch_size,
            n_select=q,
            num_models=num_models,
            num_attempts=num_attempts,
            num_coords=num_coords,
            num_samples=num_samples,
            seed=SEED,
            output=True,
            device="cuda",
            use_gpu_shap=True,
            max_models_per_batch=max_models_per_batch,
            max_shap_models_per_batch=max_shap_models_per_batch,
            checkpoint_every=5,
            checkpoint_n_grid=19,
            split_mode="split_60_20_20",
            cG=cG,
        )

    unary_params = {
        "d_hidden": d_hidden,
        "n_hidden_layers": n_hidden_layers,
        "num_epochs": max(12, min(num_epochs, 30)),
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
    folds = _make_folds(X, y)

    ensemble_specs = [
        (
            "Ens_currbest_cg050",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "num_models": 16,
                "num_attempts": 160,
                "num_coords": 7,
                "num_samples": 14,
                "cG": 0.50,
            },
        ),
        (
            "Ens_lowcg_qaware",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "num_models": 16,
                "num_attempts": 160,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 10, 10: 8, 15: 8},
                "cG_by_q": {5: 0.50, 10: 0.25, 15: 0.10},
            },
        ),
        (
            "Ens_h64_m24_a240_ep30_cg025",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 30,
                "batch_size": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 8, 10: 8, 15: 8},
                "cG": 0.25,
            },
        ),
        (
            "Ens_h64_m24_a240_ep30_cg010",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 30,
                "batch_size": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 8, 10: 8, 15: 8},
                "cG": 0.10,
            },
        ),
        (
            "Ens_h96_m24_a320_qwide_cg025",
            {
                "d_hidden": 96,
                "n_hidden_layers": 2,
                "num_epochs": 30,
                "batch_size": 2048,
                "num_models": 24,
                "num_attempts": 320,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 10, 10: 10, 15: 10},
                "cG": 0.25,
            },
        ),
        (
            "Ens_h96_m24_a320_qwide_cg010",
            {
                "d_hidden": 96,
                "n_hidden_layers": 2,
                "num_epochs": 30,
                "batch_size": 2048,
                "num_models": 24,
                "num_attempts": 320,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 10, 10: 10, 15: 10},
                "cG": 0.10,
            },
        ),
        (
            "Ens_h64_m32_a500_bstyle_cg025",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 40,
                "batch_size": 2048,
                "num_models": 32,
                "num_attempts": 500,
                "num_coords_by_q": {5: 7, 10: 8, 15: 10},
                "num_samples": 4,
                "cG": 0.25,
            },
        ),
        (
            "Ens_h96_m32_a500_qwide_cg025",
            {
                "d_hidden": 96,
                "n_hidden_layers": 2,
                "num_epochs": 40,
                "batch_size": 2048,
                "num_models": 32,
                "num_attempts": 500,
                "num_coords_by_q": {5: 7, 10: 10, 15: 12},
                "num_samples_by_q": {5: 6, 10: 6, 15: 6},
                "cG": 0.25,
            },
        ),
    ]

    cls_parts: list[dict] = []
    unary_parts: list[dict] = []

    for q in QS:
        print(f"[q={q}]")
        methods: list[tuple[str, object, dict]] = [
            (
                "PCA",
                lambda q=q: PCAReducer(q),
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
                lambda q=q: PLSReducer(q),
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
                lambda q=q: HSICSelector(q),
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

        for name, spec in ensemble_specs:
            factory, unary_params = _ensemble_factory(q, spec)
            methods.append((name, factory, unary_params))

        for fold_id in FOLD_IDS:
            tr, te = folds[fold_id - 1]
            print(f"[fold={fold_id}]")
            for method_name, factory, unary_params in methods:
                print(f"[run] q={q} fold={fold_id} method={method_name}")
                cls_rows, unary_row = _evaluate_method(
                    method_name=method_name,
                    q=q,
                    fold_id=fold_id,
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
        df_cls.groupby(["method", "q"])
        .agg(
            auc_mean=("AUC", "mean"),
            pr_mean=("PR_AUC", "mean"),
            auc_best=("AUC", "max"),
            pr_best=("PR_AUC", "max"),
        )
        .reset_index()
        .sort_values(["q", "auc_mean", "pr_mean"], ascending=[True, False, False])
    )

    df_cls.to_csv(RESULTS_ROOT / "classical_by_model.csv", index=False)
    df_un.to_csv(RESULTS_ROOT / "unary_summary.csv", index=False)
    summary_cls.to_csv(RESULTS_ROOT / "classical_summary.csv", index=False)

    with open(RESULTS_ROOT / "meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "seed": SEED,
                "qs": QS,
                "fold_ids": FOLD_IDS,
                "methods": sorted(df_cls["method"].unique().tolist()),
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
