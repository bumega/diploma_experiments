from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import torch

import nirs_core as core
from datasets import make_dataset_A, make_dataset_A_real, make_dataset_B, make_dataset_B_real, make_dataset_rdt_waterspout
from evaluation import (
    evaluate_reduction,
    run_gaussiannb_on_saved_folds,
    run_kde_bayes_on_saved_folds,
    run_knn_on_saved_folds,
    run_mlp_on_saved_folds,
    run_qda_on_saved_folds,
    run_shrinkage_lda_on_saved_folds,
    run_svm_on_saved_folds,
)
from reducers import EnsembleReducer, HSICSelector, MLPReducer, PCAReducer, PLSReducer, UMAPReducer
from reporting import summarize_results_dir
from unary import run_unary_on_saved_folds_strict


_DEFAULT_QGRIDS: dict[str, list[int]] = {
    "A": [2, 3, 5],
    "A_real": [2, 3, 5],
    "B": [5, 10, 15],
    "B_real": [5, 10, 15],
    "RDT": [5, 10, 15, 20, 30],
}

_DATASET_LABELS: dict[str, str] = {
    "A": "DS-A",
    "A_real": "DS-A-real",
    "B": "DS-B",
    "B_real": "DS-B-real",
    "RDT": "RDT-waterspout",
}

_DATASET_MAKERS: dict[str, Any] = {
    "A": make_dataset_A,
    "A_real": make_dataset_A_real,
    "B": make_dataset_B,
    "B_real": make_dataset_B_real,
    "RDT": lambda seed=42: make_dataset_rdt_waterspout(seed=seed, return_groups=True, return_feature_names=True),
}

_DATASET_ALIASES: dict[str, str] = {
    "A": "A",
    "run_A": "A",
    "dataset_a": "A",
    "A_real": "A_real",
    "run_A_real": "A_real",
    "dataset_a_real": "A_real",
    "B": "B",
    "run_B": "B",
    "dataset_b": "B",
    "B_real": "B_real",
    "run_B_real": "B_real",
    "dataset_b_real": "B_real",
    "RDT": "RDT",
    "run_RDT": "RDT",
    "dataset_rdt": "RDT",
    "rdt": "RDT",
}


def _detect_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def _normalize_dataset_key(name: str) -> str:
    key = _DATASET_ALIASES.get(str(name), str(name))
    if key not in _DEFAULT_QGRIDS:
        raise ValueError(f"Unknown dataset key: {name!r}. Expected one of {sorted(_DEFAULT_QGRIDS)}")
    return key


def _merge_params(base: dict[str, Any], override: dict[str, Any] | None = None, *, c_G: float | None = None) -> dict[str, Any]:
    merged = dict(base)
    if override:
        merged.update(override)
    if c_G is not None:
        merged["cG"] = float(c_G)
    return merged


def _resolve_qgrid(dataset_key: str, qgrids: dict[str, list[int]] | None = None) -> list[int]:
    if qgrids and dataset_key in qgrids:
        return list(qgrids[dataset_key])
    return list(_DEFAULT_QGRIDS[dataset_key])


def _find_delta_auc_columns(df_cls: pd.DataFrame) -> tuple[str, str]:
    delta_auc_col = next((col for col in df_cls.columns if "AUC" in col and "PR" not in col and col not in {"AUC", "AUC_base"}), None)
    delta_pr_col = next((col for col in df_cls.columns if "PR_AUC" in col and col not in {"PR_AUC", "PR_AUC_base"}), None)
    if delta_auc_col is None or delta_pr_col is None:
        raise KeyError("Could not identify delta AUC columns in classical results dataframe")
    return delta_auc_col, delta_pr_col


def _aggregate_classical(df_cls: pd.DataFrame, group_keys: Sequence[str] | None = None) -> pd.DataFrame:
    if df_cls.empty:
        return pd.DataFrame()
    keys = list(group_keys or ["dataset", "method", "q", "model"])
    delta_auc_col, delta_pr_col = _find_delta_auc_columns(df_cls)
    for col in ["AUC", "PR_AUC", delta_auc_col, delta_pr_col, "t_reducer", "t_model", "t_model_base"]:
        if col in df_cls.columns:
            df_cls[col] = pd.to_numeric(df_cls[col], errors="coerce")
    return (
        df_cls.groupby(keys, dropna=False)
        .agg(
            AUC_mean=("AUC", "mean"),
            AUC_std=("AUC", "std"),
            PR_AUC_mean=("PR_AUC", "mean"),
            PR_AUC_std=("PR_AUC", "std"),
            dAUC_mean=(delta_auc_col, "mean"),
            dAUC_std=(delta_auc_col, "std"),
            dPR_mean=(delta_pr_col, "mean"),
            dPR_std=(delta_pr_col, "std"),
            t_red_med=("t_reducer", "median"),
            t_mod_med=("t_model", "median"),
        )
        .reset_index()
        .sort_values(keys[:-1] + ["dAUC_mean"], ascending=[True] * max(0, len(keys) - 1) + [False])
    )


def _aggregate_unary(df_un: pd.DataFrame, group_keys: Sequence[str] | None = None) -> pd.DataFrame:
    if df_un.empty:
        return pd.DataFrame()
    keys = list(group_keys or ["dataset", "method", "q", "model"])
    return (
        df_un.groupby(keys, dropna=False)
        .agg(
            F12_mean=("F12", "mean"),
            F12_std=("F12", "std"),
            G12_mean=("G12", "mean"),
            G12_std=("G12", "std"),
            S_test_mean=("S_test", "mean"),
            S_test_std=("S_test", "std"),
            coverage_mean=("coverage", "mean"),
            coverage_std=("coverage", "std"),
            conflict_rate_mean=("conflict_rate", "mean"),
            conflict_rate_std=("conflict_rate", "std"),
            reject_rate_mean=("reject_rate", "mean"),
            reject_rate_std=("reject_rate", "std"),
            selective_accuracy_mean=("selective_accuracy", "mean"),
            selective_accuracy_std=("selective_accuracy", "std"),
            selective_f1_mean=("selective_f1", "mean"),
            selective_f1_std=("selective_f1", "std"),
            conservative_accuracy_mean=("conservative_accuracy", "mean"),
            conservative_accuracy_std=("conservative_accuracy", "std"),
        )
        .reset_index()
    )


def _run_dataset_suite(
    *,
    configs: list[tuple[str, Any, list[int]]],
    methods: list[tuple[str, Any]],
    results_root: str | Path,
    include_unary: bool,
    unary_params: dict | None,
    reducer_extra_kwargs: dict[str, dict[str, Any]] | None = None,
    model_seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    results_root = Path(results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    device = _detect_device()
    reducer_extra_kwargs = reducer_extra_kwargs or {}

    base_unary_params = {
        "d_hidden": 32,
        "n_hidden_layers": 2,
        "num_epochs": 100,
        "batch_size": 512 if device.type == "cuda" else 128,
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "cG": 0.25,
        "device": str(device),
    }
    if unary_params is not None:
        base_unary_params.update(unary_params)

    rows_cls_all: list[dict[str, Any]] = []
    rows_un_all: list[dict[str, Any]] = []

    for dname, maker, qgrid in configs:
        made = maker()
        groups = None
        feature_names = None
        if isinstance(made, tuple) and len(made) == 4:
            X, y, groups, feature_names = made
        elif isinstance(made, tuple) and len(made) == 3:
            X, y, groups = made
        else:
            X, y = made
        dataset_dir = results_root / dname
        dataset_dir.mkdir(parents=True, exist_ok=True)
        for name, ctor in methods:
            for q in qgrid:
                method_dir = dataset_dir / f"{name}_q{q}"
                method_dir.mkdir(parents=True, exist_ok=True)
                reducer_kwargs = dict(reducer_extra_kwargs.get(name, {}))
                cls_rows, un_rows = evaluate_reduction(
                    X=X,
                    y=y,
                    reducer_name=name,
                    reducer_ctor=ctor,
                    q=q,
                    dataset_name=dname,
                    reducer_kwargs=reducer_kwargs,
                    compute_unary_on_reduced=include_unary,
                    unary_params=base_unary_params,
                    results_dir=method_dir,
                    model_seed=model_seed,
                    groups=groups,
                    feature_names=feature_names,
                )
                rows_cls_all.extend(cls_rows)
                rows_un_all.extend(un_rows)

    df_cls = pd.DataFrame(rows_cls_all)
    df_un = pd.DataFrame(rows_un_all)
    agg_cls = _aggregate_classical(df_cls.copy())
    agg_un = _aggregate_unary(df_un.copy())

    df_cls.to_csv(results_root / "all_classical_results.csv", index=False)
    df_un.to_csv(results_root / "all_unary_results.csv", index=False)
    agg_cls.to_csv(results_root / "summary_classical.csv", index=False)
    agg_un.to_csv(results_root / "summary_unary.csv", index=False)

    with open(results_root / "results_bundle.pkl", "wb") as f:
        pickle.dump({"df_cls": df_cls, "df_un": df_un, "agg_cls": agg_cls, "agg_un": agg_un}, f)

    return df_cls, df_un, agg_cls, agg_un


def _build_dataset_suite_spec(
    *,
    dataset_key: str,
    device: torch.device,
    dataset_seed: int,
    reducer_seed: int,
    c_G: float | None = None,
    qgrids: dict[str, list[int]] | None = None,
    mlp_reducer_params: dict[str, Any] | None = None,
    rase_params: dict[str, Any] | None = None,
) -> tuple[list[tuple[str, Any, list[int]]], list[tuple[str, Any]], dict[str, dict[str, Any]]]:
    dataset_key = _normalize_dataset_key(dataset_key)
    qgrid = _resolve_qgrid(dataset_key, qgrids)
    dataset_label = _DATASET_LABELS[dataset_key]
    dataset_maker = _DATASET_MAKERS[dataset_key]

    mlp_defaults: dict[str, dict[str, Any]] = {
        "A": {"d_hidden": 32, "n_hidden_layers": 2, "num_epochs": 100, "batch_size": 128, "seed": reducer_seed, "output": False},
        "A_real": {"d_hidden": 64, "n_hidden_layers": 2, "num_epochs": 40, "batch_size": 256, "seed": reducer_seed, "output": False},
    }
    rase_defaults: dict[str, dict[str, Any]] = {
        "B": {"d_hidden": 32, "n_hidden_layers": 2, "num_epochs": 40, "batch_size": 512, "num_models": 96, "num_attempts": 500, "num_coords": 4, "num_samples": 4, "seed": reducer_seed, "output": True},
        "B_real": {"d_hidden": 32, "n_hidden_layers": 2, "num_epochs": 8, "batch_size": 1024, "num_models": 4, "num_attempts": 20, "num_coords": 4, "num_samples": 10, "seed": reducer_seed, "output": True},
        "RDT": {"d_hidden": 32, "n_hidden_layers": 2, "num_epochs": 8, "batch_size": 1024, "num_models": 4, "num_attempts": 20, "num_coords": 4, "num_samples": 10, "seed": reducer_seed, "output": True},
    }

    configs = [(dataset_label, lambda maker=dataset_maker: maker(seed=dataset_seed), qgrid)]

    if dataset_key in {"A", "A_real"}:
        mlp_cfg = _merge_params(mlp_defaults[dataset_key], mlp_reducer_params, c_G=c_G)
        mlp_ctor_cfg = dict(mlp_cfg)
        for key in ["device", "checkpoint_every", "checkpoint_n_grid", "cG", "lr", "weight_decay"]:
            mlp_ctor_cfg.pop(key, None)

        def _make_mlp_reducer(*, q: int | None = None, n_select: int | None = None, cfg: dict[str, Any] = mlp_ctor_cfg, **kw):
            target_q = q if q is not None else n_select
            if target_q is None:
                raise ValueError("q or n_select must be provided for MLPReducer")
            return MLPReducer(n_select=int(target_q), **cfg, **kw)

        methods = [
            ("PCA", lambda q, **_: PCAReducer(q)),
            ("PLS", lambda q, **_: PLSReducer(q)),
            ("HSIC_Lasso", lambda q, **_: HSICSelector(q)),
            ("MLP_unar", _make_mlp_reducer),
        ]
        reducer_extra_kwargs = {
            "MLP_unar": {
                "device": str(device),
                "checkpoint_every": int(mlp_cfg.get("checkpoint_every", 5)),
                "checkpoint_n_grid": int(mlp_cfg.get("checkpoint_n_grid", 19)),
                "cG": float(mlp_cfg.get("cG", 0.25)),
                "lr": float(mlp_cfg.get("lr", 3e-3)),
                "weight_decay": float(mlp_cfg.get("weight_decay", 1e-2)),
            }
        }
        return configs, methods, reducer_extra_kwargs

    rase_cfg = _merge_params(rase_defaults[dataset_key], rase_params, c_G=c_G)
    rase_ctor_cfg = dict(rase_cfg)
    for key in [
        "device",
        "use_gpu_shap",
        "max_models_per_batch",
        "max_shap_models_per_batch",
        "checkpoint_every",
        "checkpoint_n_grid",
        "cG",
        "split_mode",
        "cv_n_splits",
    ]:
        rase_ctor_cfg.pop(key, None)

    def _make_ensemble_reducer(*, q: int | None = None, n_select: int | None = None, cfg: dict[str, Any] = rase_ctor_cfg, **kw):
        target_q = q if q is not None else n_select
        if target_q is None:
            raise ValueError("q or n_select must be provided for EnsembleReducer")
        return EnsembleReducer(n_select=int(target_q), **cfg, **kw)

    methods = [
        ("PCA", lambda q, **_: PCAReducer(q)),
        ("PLS", lambda q, **_: PLSReducer(q)),
    ]
    if dataset_key in {"B_real", "RDT"}:
        methods.append(("UMAP_sup", lambda q, **_: UMAPReducer(q)))
    methods.extend(
        [
            ("HSIC_Lasso", lambda q, **_: HSICSelector(q)),
            ("Ensembleunar", _make_ensemble_reducer),
        ]
    )
    reducer_extra_kwargs = {
        "Ensembleunar": {
            "device": str(device),
            "use_gpu_shap": bool(rase_cfg.get("use_gpu_shap", device.type == "cuda")),
            "max_models_per_batch": int(rase_cfg.get("max_models_per_batch", 500 if device.type == "cuda" else 64)),
            "max_shap_models_per_batch": int(rase_cfg.get("max_shap_models_per_batch", 128 if device.type == "cuda" else 16)),
            "checkpoint_every": int(rase_cfg.get("checkpoint_every", 5)),
            "checkpoint_n_grid": int(rase_cfg.get("checkpoint_n_grid", 19)),
            "cG": float(rase_cfg.get("cG", 0.25)),
            "split_mode": rase_cfg.get("split_mode", "split_60_20_20"),
            "cv_n_splits": int(rase_cfg.get("cv_n_splits", 5)),
        }
    }
    return configs, methods, reducer_extra_kwargs


def _run_named_dataset_suite(
    *,
    dataset_key: str,
    include_unary: bool,
    unary_params: dict | None,
    results_root: str | Path,
    dataset_seed: int = 42,
    reducer_seed: int | None = None,
    model_seed: int = 42,
    c_G: float | None = None,
    qgrids: dict[str, list[int]] | None = None,
    mlp_reducer_params: dict[str, Any] | None = None,
    rase_params: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    device = _detect_device()
    reducer_seed = dataset_seed if reducer_seed is None else reducer_seed
    unary_params_eff = dict(unary_params or {})
    unary_params_eff.setdefault("seed", reducer_seed)
    if c_G is not None:
        unary_params_eff["cG"] = float(c_G)

    configs, methods, reducer_extra_kwargs = _build_dataset_suite_spec(
        dataset_key=dataset_key,
        device=device,
        dataset_seed=dataset_seed,
        reducer_seed=reducer_seed,
        c_G=c_G,
        qgrids=qgrids,
        mlp_reducer_params=mlp_reducer_params,
        rase_params=rase_params,
    )
    return _run_dataset_suite(
        configs=configs,
        methods=methods,
        results_root=results_root,
        include_unary=include_unary,
        unary_params=unary_params_eff,
        reducer_extra_kwargs=reducer_extra_kwargs,
        model_seed=model_seed,
    )


def run_dataset_a(
    *,
    include_unary: bool = True,
    unary_params: dict | None = None,
    results_root: str | Path = "results_A_full",
    dataset_seed: int = 42,
    reducer_seed: int | None = None,
    model_seed: int = 42,
    mlp_reducer_params: dict[str, Any] | None = None,
    c_G: float | None = None,
    qgrid: list[int] | None = None,
):
    qgrids = None if qgrid is None else {"A": list(qgrid)}
    return _run_named_dataset_suite(
        dataset_key="A",
        include_unary=include_unary,
        unary_params=unary_params,
        results_root=results_root,
        dataset_seed=dataset_seed,
        reducer_seed=reducer_seed,
        model_seed=model_seed,
        c_G=c_G,
        qgrids=qgrids,
        mlp_reducer_params=mlp_reducer_params,
    )


def run_dataset_a_real(
    *,
    include_unary: bool = True,
    unary_params: dict | None = None,
    results_root: str | Path = "results_A_real_full",
    dataset_seed: int = 42,
    reducer_seed: int | None = None,
    model_seed: int = 42,
    mlp_reducer_params: dict[str, Any] | None = None,
    c_G: float | None = None,
    qgrid: list[int] | None = None,
):
    qgrids = None if qgrid is None else {"A_real": list(qgrid)}
    return _run_named_dataset_suite(
        dataset_key="A_real",
        include_unary=include_unary,
        unary_params=unary_params,
        results_root=results_root,
        dataset_seed=dataset_seed,
        reducer_seed=reducer_seed,
        model_seed=model_seed,
        c_G=c_G,
        qgrids=qgrids,
        mlp_reducer_params=mlp_reducer_params,
    )


def run_dataset_b(
    *,
    include_unary: bool = True,
    unary_params: dict | None = None,
    results_root: str | Path = "results_B_full_modular",
    dataset_seed: int = 42,
    reducer_seed: int | None = None,
    model_seed: int = 42,
    rase_params: dict[str, Any] | None = None,
    c_G: float | None = None,
    qgrid: list[int] | None = None,
):
    qgrids = None if qgrid is None else {"B": list(qgrid)}
    return _run_named_dataset_suite(
        dataset_key="B",
        include_unary=include_unary,
        unary_params=unary_params,
        results_root=results_root,
        dataset_seed=dataset_seed,
        reducer_seed=reducer_seed,
        model_seed=model_seed,
        c_G=c_G,
        qgrids=qgrids,
        rase_params=rase_params,
    )


def run_dataset_b_real(
    *,
    include_unary: bool = True,
    unary_params: dict | None = None,
    results_root: str | Path = "results_B_real_full",
    dataset_seed: int = 42,
    reducer_seed: int | None = None,
    model_seed: int = 42,
    rase_params: dict[str, Any] | None = None,
    c_G: float | None = None,
    qgrid: list[int] | None = None,
):
    qgrids = None if qgrid is None else {"B_real": list(qgrid)}
    return _run_named_dataset_suite(
        dataset_key="B_real",
        include_unary=include_unary,
        unary_params=unary_params,
        results_root=results_root,
        dataset_seed=dataset_seed,
        reducer_seed=reducer_seed,
        model_seed=model_seed,
        c_G=c_G,
        qgrids=qgrids,
        rase_params=rase_params,
    )


def run_all_multi_seed(
    *,
    seeds: Sequence[int],
    datasets: Sequence[str] = ("A", "A_real", "B", "B_real"),
    include_unary: bool = True,
    unary_params: dict | None = None,
    results_root: str | Path = "results_multi_seed",
    mlp_reducer_params: dict[str, Any] | None = None,
    rase_params: dict[str, Any] | None = None,
    c_G: float | None = None,
    qgrids: dict[str, list[int]] | None = None,
) -> dict[str, Any]:
    seeds = list(seeds)
    if not seeds:
        raise ValueError("seeds must contain at least one value")

    dataset_keys = [_normalize_dataset_key(name) for name in datasets]
    results_root = Path(results_root)
    results_root.mkdir(parents=True, exist_ok=True)

    combined_cls_parts: list[pd.DataFrame] = []
    combined_un_parts: list[pd.DataFrame] = []
    per_seed_outputs: dict[int, dict[str, Any]] = {}

    for seed in seeds:
        seed_root = results_root / f"seed_{seed}"
        seed_root.mkdir(parents=True, exist_ok=True)
        seed_cls_parts: list[pd.DataFrame] = []
        seed_un_parts: list[pd.DataFrame] = []
        per_seed_outputs[seed] = {}

        for dataset_key in dataset_keys:
            dataset_root = seed_root / dataset_key
            df_cls, df_un, agg_cls, agg_un = _run_named_dataset_suite(
                dataset_key=dataset_key,
                include_unary=include_unary,
                unary_params=unary_params,
                results_root=dataset_root,
                dataset_seed=seed,
                reducer_seed=seed,
                model_seed=seed,
                c_G=c_G,
                qgrids=qgrids,
                mlp_reducer_params=mlp_reducer_params,
                rase_params=rase_params,
            )

            if not df_cls.empty:
                df_cls = df_cls.copy()
                df_cls.insert(0, "seed", seed)
                df_cls.insert(1, "dataset_key", dataset_key)
                seed_cls_parts.append(df_cls)
                combined_cls_parts.append(df_cls)

            if not df_un.empty:
                df_un = df_un.copy()
                df_un.insert(0, "seed", seed)
                df_un.insert(1, "dataset_key", dataset_key)
                seed_un_parts.append(df_un)
                combined_un_parts.append(df_un)

            per_seed_outputs[seed][dataset_key] = {
                "df_cls": df_cls,
                "df_un": df_un,
                "agg_cls": agg_cls,
                "agg_un": agg_un,
                "results_root": dataset_root,
            }

        seed_df_cls = pd.concat(seed_cls_parts, ignore_index=True) if seed_cls_parts else pd.DataFrame()
        seed_df_un = pd.concat(seed_un_parts, ignore_index=True) if seed_un_parts else pd.DataFrame()
        seed_agg_cls = _aggregate_classical(seed_df_cls.copy())
        seed_agg_un = _aggregate_unary(seed_df_un.copy())

        seed_df_cls.to_csv(seed_root / "all_classical_results.csv", index=False)
        seed_df_un.to_csv(seed_root / "all_unary_results.csv", index=False)
        seed_agg_cls.to_csv(seed_root / "summary_classical.csv", index=False)
        seed_agg_un.to_csv(seed_root / "summary_unary.csv", index=False)

        with open(seed_root / "results_bundle.pkl", "wb") as f:
            pickle.dump({"df_cls": seed_df_cls, "df_un": seed_df_un, "agg_cls": seed_agg_cls, "agg_un": seed_agg_un}, f)

    df_cls_all = pd.concat(combined_cls_parts, ignore_index=True) if combined_cls_parts else pd.DataFrame()
    df_un_all = pd.concat(combined_un_parts, ignore_index=True) if combined_un_parts else pd.DataFrame()
    agg_cls_by_seed = _aggregate_classical(df_cls_all.copy(), group_keys=["seed", "dataset", "method", "q", "model"])
    agg_un_by_seed = _aggregate_unary(df_un_all.copy(), group_keys=["seed", "dataset", "method", "q", "model"])
    agg_cls_all = _aggregate_classical(df_cls_all.copy())
    agg_un_all = _aggregate_unary(df_un_all.copy())

    df_cls_all.to_csv(results_root / "all_classical_results.csv", index=False)
    df_un_all.to_csv(results_root / "all_unary_results.csv", index=False)
    agg_cls_by_seed.to_csv(results_root / "summary_classical_by_seed.csv", index=False)
    agg_un_by_seed.to_csv(results_root / "summary_unary_by_seed.csv", index=False)
    agg_cls_all.to_csv(results_root / "summary_classical.csv", index=False)
    agg_un_all.to_csv(results_root / "summary_unary.csv", index=False)

    with open(results_root / "results_bundle.pkl", "wb") as f:
        pickle.dump(
            {
                "df_cls": df_cls_all,
                "df_un": df_un_all,
                "agg_cls_by_seed": agg_cls_by_seed,
                "agg_un_by_seed": agg_un_by_seed,
                "agg_cls": agg_cls_all,
                "agg_un": agg_un_all,
                "per_seed_outputs": per_seed_outputs,
            },
            f,
        )

    return {
        "df_cls": df_cls_all,
        "df_un": df_un_all,
        "agg_cls_by_seed": agg_cls_by_seed,
        "agg_un_by_seed": agg_un_by_seed,
        "agg_cls": agg_cls_all,
        "agg_un": agg_un_all,
        "per_seed_outputs": per_seed_outputs,
        "results_root": results_root,
    }


def run_dataset_colon(
    *,
    results_root: str | Path = "results_colon_modular",
    cv_seed: int = 42,
    reducer_seed: int = 42,
    clf_seed: int = 42,
    run_classical_models: bool = True,
    run_unary: bool = True,
    unary_params: dict | None = None,
) -> dict[str, Any]:
    results_root = Path(results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    out: dict[str, Any] = {
        "results_df": core.run_colon_10fold_cv(
            cv_seed=cv_seed,
            reducer_seed=reducer_seed,
            clf_seed=clf_seed,
            results_dir=str(results_root),
        )
    }
    if run_classical_models:
        out["svm"] = run_svm_on_saved_folds(str(results_root), C=1.0, kernel="rbf", gamma="scale", seed=clf_seed)
        out["knn"] = run_knn_on_saved_folds(str(results_root), n_neighbors=5, weights="uniform", seed=clf_seed)
        out["kde"] = run_kde_bayes_on_saved_folds(str(results_root), bandwidth=0.5, seed=clf_seed)
        out["qda"] = run_qda_on_saved_folds(str(results_root), reg_param=0.0, seed=clf_seed)
        out["lda"] = run_shrinkage_lda_on_saved_folds(str(results_root), shrinkage="auto", solver="lsqr", seed=clf_seed)
        out["gnb"] = run_gaussiannb_on_saved_folds(str(results_root), var_smoothing=1e-9, seed=clf_seed)
        out["mlp"] = run_mlp_on_saved_folds(
            str(results_root),
            hidden_layer_sizes=(64, 32),
            activation="relu",
            solver="adam",
            alpha=1e-3,
            max_iter=4000,
            seed=clf_seed,
        )
    if run_unary:
        unary_kwargs = {
            "d_hidden": 64,
            "n_hidden_layers": 2,
            "num_epochs": 250,
            "batch_size": 8,
            "checkpoint_every": 5,
            "checkpoint_n_grid": 19,
            "cG": 0.25,
            "seed": 42,
            "device": str(_detect_device()),
        }
        if unary_params is not None:
            unary_kwargs.update(unary_params)
        out["unary"] = run_unary_on_saved_folds_strict(str(results_root), **unary_kwargs)
    out["comparison"] = summarize_results_dir(results_root)
    return out


run_all_A = run_dataset_a
run_all_A_real = run_dataset_a_real
run_all_B = run_dataset_b
run_all_B_real = run_dataset_b_real


__all__ = [
    "run_all_A",
    "run_all_A_real",
    "run_all_B",
    "run_all_B_real",
    "run_all_multi_seed",
    "run_dataset_a",
    "run_dataset_a_real",
    "run_dataset_b",
    "run_dataset_b_real",
    "run_dataset_colon",
]
