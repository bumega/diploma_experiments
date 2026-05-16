from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.preprocessing import StandardScaler

import nirs_core as core
from datasets import make_dataset_rdt_waterspout
from evaluation import _strict_unary_on_reduced
from rdt_dataset import repair_training_matrix
from reducers import EnsembleReducer, HSICSelector, PCAReducer, PLSReducer


SEED = 41
Q = 20
FOLD_IDS = [1]
FEATURE_SET = "clean_85"
GROUP_BY = "date_folder"
RESULTS_ROOT = Path(os.environ.get("RDT_RESULTS_ROOT", "quick_rdt_search_q20"))
METHOD_FILTER = {
    item.strip() for item in os.environ.get("RDT_METHOD_FILTER", "").split(",") if item.strip()
}


def _make_median_imputer() -> SimpleImputer:
    try:
        return SimpleImputer(strategy="median", keep_empty_features=True)
    except TypeError:
        return SimpleImputer(strategy="median")


def _detect_device_str() -> str:
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def _make_folds(X: np.ndarray, y: np.ndarray, groups: np.ndarray) -> list[tuple[np.ndarray, np.ndarray]]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)
    groups_arr = np.asarray(groups).ravel()
    unique_groups = np.unique(groups_arr)
    pos_groups = np.unique(groups_arr[y_bin == 1])
    neg_groups = np.unique(groups_arr[y_bin == 0])
    n_splits = min(5, len(unique_groups), len(pos_groups), len(neg_groups))
    if n_splits < 2:
        raise ValueError("RDT needs at least two positive and negative groups for StratifiedGroupKFold.")
    sgkf = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=42)
    return list(sgkf.split(X, y_bin, groups_arr))


def _evaluate_method(
    *,
    method_name: str,
    fold_id: int,
    reducer_factory,
    unary_params: dict[str, Any],
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    tr: np.ndarray,
    te: np.ndarray,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)

    Xtr, Xte = X[tr], X[te]
    ytr_bin, yte_bin = y_bin[tr], y_bin[te]
    ytr_orig, yte_orig = y_orig[tr], y_orig[te]

    imputer = _make_median_imputer()
    Xtr = imputer.fit_transform(Xtr)
    Xte = imputer.transform(Xte)
    Xtr = repair_training_matrix(Xtr, feature_names)
    Xte = repair_training_matrix(Xte, feature_names)

    scaler = StandardScaler()
    Xtr_s = scaler.fit_transform(Xtr)
    Xte_s = scaler.transform(Xte)

    reducer = reducer_factory()
    Ztr = reducer.fit_transform(Xtr_s, ytr_orig)
    Zte = reducer.transform(Xte_s)

    cls_rows: list[dict[str, Any]] = []
    for model_name, base_model in core.get_models(seed=SEED + fold_id).items():
        model = core._clone_estimator(base_model)
        model.fit(Ztr, ytr_bin)
        score = core._get_score_vector(model, Zte)
        cls_rows.append(
            {
                "method": method_name,
                "q": Q,
                "fold_id": fold_id,
                "model": model_name,
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
        "selected_indices": ",".join(map(str, np.asarray(getattr(reducer, "selected_indices_", []), dtype=int).tolist())),
    }
    return cls_rows, unary_row


def _ensemble_factory(device_str: str, spec: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    resolved_batch_size = int(spec.get("batch_size", spec.get("batch_size_neg", 2048)))

    def build() -> EnsembleReducer:
        return EnsembleReducer(
            d_hidden=int(spec["d_hidden"]),
            n_hidden_layers=int(spec["n_hidden_layers"]),
            num_epochs=int(spec["num_epochs"]),
            batch_size=resolved_batch_size,
            n_select=Q,
            num_models=int(spec["num_models"]),
            num_attempts=int(spec["num_attempts"]),
            num_coords=int(spec["num_coords"]),
            num_samples=int(spec["num_samples"]),
            seed=SEED,
            output=True,
            device=device_str,
            use_gpu_shap=device_str.startswith("cuda"),
            max_models_per_batch=int(spec.get("max_models_per_batch", 4 if device_str.startswith("cuda") else 16)),
            max_shap_models_per_batch=int(spec.get("max_shap_models_per_batch", 4 if device_str.startswith("cuda") else 8)),
            checkpoint_every=5,
            checkpoint_n_grid=19,
            split_mode=str(spec["split_mode"]),
            cv_n_splits=int(spec.get("cv_n_splits", 5)),
            cG=float(spec["cG"]),
            batch_size_pos=spec.get("batch_size_pos"),
            batch_size_neg=spec.get("batch_size_neg"),
        )

    unary_params = {
        "d_hidden": int(spec["d_hidden"]),
        "n_hidden_layers": int(spec["n_hidden_layers"]),
        "num_epochs": int(spec["num_epochs"]),
        "batch_size": resolved_batch_size,
        "batch_size_pos": spec.get("batch_size_pos"),
        "batch_size_neg": spec.get("batch_size_neg"),
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "cG": float(spec["cG"]),
        "seed": SEED,
        "device": device_str,
    }
    return build, unary_params


def main() -> None:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    device_str = _detect_device_str()

    X, y, groups, feature_names = make_dataset_rdt_waterspout(
        seed=SEED,
        feature_set=FEATURE_SET,
        return_groups=True,
        return_feature_names=True,
        group_by=GROUP_BY,
    )
    folds = _make_folds(X, y, groups)

    meta_rows = []
    for fold_id, (tr, te) in enumerate(folds, start=1):
        ytr = np.asarray(y)[tr]
        yte = np.asarray(y)[te]
        meta_rows.append(
            {
                "fold_id": fold_id,
                "train_rows": int(len(tr)),
                "test_rows": int(len(te)),
                "train_pos": int((ytr == 1).sum()),
                "test_pos": int((yte == 1).sum()),
                "train_groups": int(len(np.unique(np.asarray(groups)[tr]))),
                "test_groups": int(len(np.unique(np.asarray(groups)[te]))),
            }
        )
    pd.DataFrame(meta_rows).to_csv(RESULTS_ROOT / "fold_meta.csv", index=False)
    pd.DataFrame({"feature": feature_names}).to_csv(RESULTS_ROOT / "feature_names.csv", index=False)

    ensemble_specs: list[tuple[str, dict[str, Any]]] = [
        (
            "Ens_holdout_h64_m24_a240_c12_s8_ep20_cg025",
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords": 12,
                "num_samples": 8,
                "split_mode": "split_60_20_20",
                "cG": 0.25,
            },
        ),
        (
            "Ens_full_h32_m24_a240_c8_s1_ep50_cg010_bp8_bn2048",
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 50,
                "batch_size": 2048,
                "batch_size_pos": 8,
                "batch_size_neg": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords": 8,
                "num_samples": 1,
                "split_mode": "full_data",
                "cG": 0.10,
            },
        ),
        (
            "Ens_full_h32_m24_a240_c8_s2_ep50_cg010_bp8_bn2048",
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 50,
                "batch_size": 2048,
                "batch_size_pos": 8,
                "batch_size_neg": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords": 8,
                "num_samples": 2,
                "split_mode": "full_data",
                "cG": 0.10,
            },
        ),
        (
            "Ens_full_h32_m24_a240_c8_s4_ep50_cg010_bp8_bn2048",
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 50,
                "batch_size": 2048,
                "batch_size_pos": 8,
                "batch_size_neg": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords": 8,
                "num_samples": 4,
                "split_mode": "full_data",
                "cG": 0.10,
            },
        ),
        (
            "Ens_cv5_h32_m24_a240_c8_s2_ep50_cg010_bp8_bn2048",
            {
                "d_hidden": 32,
                "n_hidden_layers": 2,
                "num_epochs": 50,
                "batch_size": 2048,
                "batch_size_pos": 8,
                "batch_size_neg": 2048,
                "num_models": 24,
                "num_attempts": 240,
                "num_coords": 8,
                "num_samples": 2,
                "split_mode": "cv_5fold",
                "cv_n_splits": 5,
                "cG": 0.10,
            },
        ),
    ]

    methods: list[tuple[str, Any, dict[str, Any]]] = [
        (
            "PCA",
            lambda: PCAReducer(Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": device_str,
            },
        ),
        (
            "PLS",
            lambda: PLSReducer(Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": device_str,
            },
        ),
        (
            "HSIC_Lasso",
            lambda: HSICSelector(Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": SEED,
                "device": device_str,
            },
        ),
    ]

    config_specs: dict[str, dict[str, Any]] = {}
    for name, spec in ensemble_specs:
        factory, unary_params = _ensemble_factory(device_str, spec)
        methods.append((name, factory, unary_params))
        config_specs[name] = spec

    if METHOD_FILTER:
        methods = [triple for triple in methods if triple[0] in METHOD_FILTER]
        print(f"[method-filter] selected={sorted(METHOD_FILTER)}")

    cls_raw_path = RESULTS_ROOT / "classical_raw.csv"
    unary_raw_path = RESULTS_ROOT / "unary_raw.csv"
    cls_parts: list[dict[str, Any]] = pd.read_csv(cls_raw_path).to_dict("records") if cls_raw_path.exists() else []
    unary_parts: list[dict[str, Any]] = pd.read_csv(unary_raw_path).to_dict("records") if unary_raw_path.exists() else []

    def _persist_progress() -> None:
        df_cls_local = pd.DataFrame(cls_parts)
        df_un_local = pd.DataFrame(unary_parts)
        df_cls_local.to_csv(cls_raw_path, index=False)
        df_un_local.to_csv(unary_raw_path, index=False)

        if not df_cls_local.empty:
            cls_summary_local = (
                df_cls_local.groupby(["method", "q"])
                .agg(
                    AUC_mean=("AUC", "mean"),
                    AUC_std=("AUC", "std"),
                    PR_mean=("PR_AUC", "mean"),
                    PR_std=("PR_AUC", "std"),
                    n=("AUC", "size"),
                )
                .reset_index()
                .sort_values(["AUC_mean", "PR_mean"], ascending=False)
            )
            cls_by_model_local = (
                df_cls_local.groupby(["method", "q", "model"])
                .agg(
                    AUC_mean=("AUC", "mean"),
                    AUC_std=("AUC", "std"),
                    PR_mean=("PR_AUC", "mean"),
                    PR_std=("PR_AUC", "std"),
                    n=("AUC", "size"),
                )
                .reset_index()
                .sort_values(["AUC_mean", "PR_mean"], ascending=False)
            )
            cls_summary_local.to_csv(RESULTS_ROOT / "classical_summary.csv", index=False)
            cls_by_model_local.to_csv(RESULTS_ROOT / "classical_by_model.csv", index=False)

        if not df_un_local.empty:
            un_summary_local = (
                df_un_local.groupby(["method", "q"])
                .agg(
                    S_test=("S_test", "mean"),
                    F12=("F12", "mean"),
                    G12=("G12", "mean"),
                    coverage=("coverage", "mean"),
                    conflict_rate=("conflict_rate", "mean"),
                    n=("S_test", "size"),
                )
                .reset_index()
                .sort_values(["S_test", "F12"], ascending=False)
            )
            un_summary_local.to_csv(RESULTS_ROOT / "unary_summary.csv", index=False)

    for fold_id in FOLD_IDS:
        tr, te = folds[fold_id - 1]
        print(f"[fold={fold_id}] train={len(tr)} test={len(te)} train_pos={(np.asarray(y)[tr] == 1).sum()} test_pos={(np.asarray(y)[te] == 1).sum()}")
        for method_name, reducer_factory, unary_params in methods:
            already_cls = any(
                row.get("method") == method_name and int(row.get("q", -1)) == Q and int(row.get("fold_id", -1)) == fold_id
                for row in cls_parts
            )
            already_un = any(
                row.get("method") == method_name and int(row.get("q", -1)) == Q and int(row.get("fold_id", -1)) == fold_id
                for row in unary_parts
            )
            if already_cls and already_un:
                print(f"[skip-existing] method={method_name} q={Q} fold={fold_id}")
                continue

            print(f"[run] method={method_name} q={Q} fold={fold_id}")
            cls_rows, unary_row = _evaluate_method(
                method_name=method_name,
                fold_id=fold_id,
                reducer_factory=reducer_factory,
                unary_params=unary_params,
                X=X,
                y=y,
                feature_names=list(feature_names),
                tr=tr,
                te=te,
            )
            cls_parts.extend(cls_rows)
            unary_parts.append(unary_row)
            _persist_progress()

    df_cls = pd.DataFrame(cls_parts)
    df_un = pd.DataFrame(unary_parts)

    df_cls.to_csv(RESULTS_ROOT / "classical_raw.csv", index=False)
    df_un.to_csv(RESULTS_ROOT / "unary_raw.csv", index=False)

    cls_summary = (
        df_cls.groupby(["method", "q"])
        .agg(
            AUC_mean=("AUC", "mean"),
            AUC_std=("AUC", "std"),
            PR_mean=("PR_AUC", "mean"),
            PR_std=("PR_AUC", "std"),
            n=("AUC", "size"),
        )
        .reset_index()
        .sort_values(["AUC_mean", "PR_mean"], ascending=False)
    )
    cls_by_model = (
        df_cls.groupby(["method", "q", "model"])
        .agg(
            AUC_mean=("AUC", "mean"),
            AUC_std=("AUC", "std"),
            PR_mean=("PR_AUC", "mean"),
            PR_std=("PR_AUC", "std"),
            n=("AUC", "size"),
        )
        .reset_index()
        .sort_values(["AUC_mean", "PR_mean"], ascending=False)
    )
    un_summary = (
        df_un.groupby(["method", "q"])
        .agg(
            S_test=("S_test", "mean"),
            F12=("F12", "mean"),
            G12=("G12", "mean"),
            coverage=("coverage", "mean"),
            conflict_rate=("conflict_rate", "mean"),
            n=("S_test", "size"),
        )
        .reset_index()
        .sort_values(["S_test", "F12"], ascending=False)
    )

    cls_summary.to_csv(RESULTS_ROOT / "classical_summary.csv", index=False)
    cls_by_model.to_csv(RESULTS_ROOT / "classical_by_model.csv", index=False)
    un_summary.to_csv(RESULTS_ROOT / "unary_summary.csv", index=False)

    with open(RESULTS_ROOT / "ensemble_specs.json", "w", encoding="utf-8") as f:
        json.dump(config_specs, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
