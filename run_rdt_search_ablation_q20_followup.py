from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import run_rdt_search_ablation_q20 as base
from datasets import make_dataset_rdt_waterspout
from reducers import HSICSelector, PCAReducer, PLSReducer


RESULTS_ROOT = Path(os.environ.get("RDT_RESULTS_ROOT", "quick_rdt_search_q20"))


BASE_FULLDATA_SPECS: dict[str, dict[str, Any]] = {
    "Ens_full_h32_m24_a240_c8_s1_ep50_cg010_bp8_bn2048": {
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
    "Ens_full_h32_m24_a240_c8_s2_ep50_cg010_bp8_bn2048": {
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
    "Ens_full_h32_m24_a240_c8_s4_ep50_cg010_bp8_bn2048": {
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
}


def _num_samples_from_method(method_name: str) -> int:
    marker = "_s"
    if marker not in method_name:
        return 10**9
    tail = method_name.split(marker, 1)[1]
    digits = []
    for ch in tail:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    return int("".join(digits)) if digits else 10**9


def _pick_best_completed_full_data_method() -> tuple[str, dict[str, Any], dict[str, float]]:
    un_raw_path = RESULTS_ROOT / "unary_raw.csv"
    if not un_raw_path.exists():
        raise FileNotFoundError(f"Missing unary results: {un_raw_path}")

    df_un = pd.read_csv(un_raw_path)
    if df_un.empty:
        raise ValueError("unary_raw.csv is empty")

    summary = (
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
    )
    summary = summary[
        (summary["q"] == base.Q) & (summary["method"].isin(BASE_FULLDATA_SPECS.keys()))
    ].copy()
    if summary.empty:
        raise ValueError("No completed full_data Ensemble methods found to seed the follow-up run")

    summary["num_samples"] = summary["method"].map(_num_samples_from_method)
    summary = summary.sort_values(
        ["S_test", "F12", "G12", "conflict_rate", "num_samples", "method"],
        ascending=[False, False, True, True, True, True],
    ).reset_index(drop=True)

    best_method = str(summary.loc[0, "method"])
    best_stats = {
        "S_test": float(summary.loc[0, "S_test"]),
        "F12": float(summary.loc[0, "F12"]),
        "G12": float(summary.loc[0, "G12"]),
        "coverage": float(summary.loc[0, "coverage"]),
        "conflict_rate": float(summary.loc[0, "conflict_rate"]),
        "n": float(summary.loc[0, "n"]),
    }
    return best_method, dict(BASE_FULLDATA_SPECS[best_method]), best_stats


def _method_name_for_cg(base_method: str, c_g: float) -> str:
    token = f"cg{int(round(c_g * 100)):03d}"
    if "_cg" in base_method:
        head, tail = base_method.split("_cg", 1)
        suffix = tail.split("_", 1)[1] if "_" in tail else ""
        return f"{head}_{token}_{suffix}" if suffix else f"{head}_{token}"
    return f"{base_method}_{token}"


def main() -> None:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    device_str = base._detect_device_str()

    best_method, best_spec, best_stats = _pick_best_completed_full_data_method()
    print(
        "[followup-select] "
        f"best_method={best_method} "
        f"S_test={best_stats['S_test']:.6f} "
        f"F12={best_stats['F12']:.6f} "
        f"G12={best_stats['G12']:.6f} "
        f"conflict_rate={best_stats['conflict_rate']:.6f} "
        f"n={int(best_stats['n'])}"
    )

    X, y, groups, feature_names = make_dataset_rdt_waterspout(
        seed=base.SEED,
        feature_set=base.FEATURE_SET,
        return_groups=True,
        return_feature_names=True,
        group_by=base.GROUP_BY,
    )
    folds = base._make_folds(X, y, groups)

    candidate_cg_values = [0.15, 0.20, 0.25]
    ensemble_specs: list[tuple[str, dict[str, Any]]] = []
    for c_g in candidate_cg_values:
        spec = dict(best_spec)
        spec["cG"] = c_g
        ensemble_specs.append((_method_name_for_cg(best_method, c_g), spec))

    methods: list[tuple[str, Any, dict[str, Any]]] = [
        (
            "PCA",
            lambda: PCAReducer(base.Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": base.SEED,
                "device": device_str,
            },
        ),
        (
            "PLS",
            lambda: PLSReducer(base.Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": base.SEED,
                "device": device_str,
            },
        ),
        (
            "HSIC_Lasso",
            lambda: HSICSelector(base.Q),
            {
                "d_hidden": 64,
                "n_hidden_layers": 2,
                "num_epochs": 20,
                "batch_size": 2048,
                "checkpoint_every": 5,
                "checkpoint_n_grid": 19,
                "cG": 0.25,
                "seed": base.SEED,
                "device": device_str,
            },
        ),
    ]

    config_specs: dict[str, dict[str, Any]] = {}
    config_specs["best_base_method"] = {
        "method": best_method,
        "selection_metric": "S_test",
        "S_test": best_stats["S_test"],
        "F12": best_stats["F12"],
        "G12": best_stats["G12"],
        "coverage": best_stats["coverage"],
        "conflict_rate": best_stats["conflict_rate"],
        "n": best_stats["n"],
    }
    for name, spec in ensemble_specs:
        factory, unary_params = base._ensemble_factory(device_str, spec)
        methods.append((name, factory, unary_params))
        config_specs[name] = spec

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

    for fold_id in base.FOLD_IDS:
        tr, te = folds[fold_id - 1]
        print(
            f"[fold={fold_id}] train={len(tr)} test={len(te)} "
            f"train_pos={(np.asarray(y)[tr] == 1).sum()} test_pos={(np.asarray(y)[te] == 1).sum()}"
        )
        for method_name, reducer_factory, unary_params in methods:
            already_cls = any(
                row.get("method") == method_name
                and int(row.get("q", -1)) == base.Q
                and int(row.get("fold_id", -1)) == fold_id
                for row in cls_parts
            )
            already_un = any(
                row.get("method") == method_name
                and int(row.get("q", -1)) == base.Q
                and int(row.get("fold_id", -1)) == fold_id
                for row in unary_parts
            )
            if already_cls and already_un:
                print(f"[skip-existing] method={method_name} q={base.Q} fold={fold_id}")
                continue

            print(f"[run] method={method_name} q={base.Q} fold={fold_id}")
            cls_rows, unary_row = base._evaluate_method(
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

    with open(RESULTS_ROOT / "followup_specs.json", "w", encoding="utf-8") as f:
        json.dump(config_specs, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
