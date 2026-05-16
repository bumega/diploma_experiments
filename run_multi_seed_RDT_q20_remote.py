from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import pandas as pd

from evaluation import evaluate_reduction
from pipelines import _aggregate_classical, _aggregate_unary, _build_dataset_suite_spec, _detect_device


RASE_PARAMS = {
    "d_hidden": 64,
    "n_hidden_layers": 2,
    "num_epochs": 20,
    "batch_size": 2048,
    "num_models": 24,
    "num_attempts": 240,
    "num_coords": 12,
    "num_samples": 8,
    "checkpoint_every": 5,
    "checkpoint_n_grid": 19,
    "split_mode": "split_60_20_20",
    "cv_n_splits": 5,
    "cG": 0.25,
    "output": True,
}

UNARY_PARAMS = {
    "d_hidden": 64,
    "n_hidden_layers": 2,
    "num_epochs": 20,
    "batch_size": 2048,
    "checkpoint_every": 5,
    "checkpoint_n_grid": 19,
    "cG": 0.25,
}

SEEDS = [41, 42, 43, 44, 45]
DATASET_KEY = "RDT"
RESULTS_ROOT = Path("results_multi_seed_RDT_q20")
QGRIDS = {"RDT": [20]}


def _load_existing_rows(method_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cls_rows: list[dict[str, Any]] = []
    un_rows: list[dict[str, Any]] = []

    cls_path = method_dir / "all_classical_folds.csv"
    un_path = method_dir / "all_unary_folds.csv"

    if cls_path.exists():
        cls_rows = pd.read_csv(cls_path).to_dict("records")
    if un_path.exists():
        un_rows = pd.read_csv(un_path).to_dict("records")

    return cls_rows, un_rows


def _run_dataset_seed(seed: int) -> dict[str, Any]:
    device = _detect_device()
    unary_params = dict(UNARY_PARAMS)
    unary_params["seed"] = seed
    unary_params["device"] = str(device)

    configs, methods, reducer_extra_kwargs = _build_dataset_suite_spec(
        dataset_key=DATASET_KEY,
        device=device,
        dataset_seed=seed,
        reducer_seed=seed,
        c_G=float(RASE_PARAMS["cG"]),
        qgrids=QGRIDS,
        rase_params=RASE_PARAMS,
    )

    methods = [(name, ctor) for name, ctor in methods if name != "UMAP_sup"]
    reducer_extra_kwargs.pop("UMAP_sup", None)

    seed_root = RESULTS_ROOT / f"seed_{seed}" / DATASET_KEY
    seed_root.mkdir(parents=True, exist_ok=True)

    rows_cls_all: list[dict[str, Any]] = []
    rows_un_all: list[dict[str, Any]] = []

    for dname, maker, qgrid in configs:
        made = maker()
        if len(made) == 4:
            X, y, groups, feature_names = made
        else:
            raise ValueError("RDT dataset maker must return X, y, groups, feature_names")
        dataset_dir = seed_root / dname
        dataset_dir.mkdir(parents=True, exist_ok=True)

        for name, ctor in methods:
            for q in qgrid:
                method_dir = dataset_dir / f"{name}_q{q}"
                method_dir.mkdir(parents=True, exist_ok=True)

                if name != "Ensembleunar" and (method_dir / "all_classical_folds.csv").exists():
                    cls_rows, un_rows = _load_existing_rows(method_dir)
                    rows_cls_all.extend(cls_rows)
                    rows_un_all.extend(un_rows)
                    print(f"[skip-existing] seed={seed} dataset={dname} method={name} q={q}")
                    continue

                print(f"[run] seed={seed} dataset={dname} method={name} q={q}")
                cls_rows, un_rows = evaluate_reduction(
                    X=X,
                    y=y,
                    reducer_name=name,
                    reducer_ctor=ctor,
                    q=q,
                    dataset_name=dname,
                    reducer_kwargs=dict(reducer_extra_kwargs.get(name, {})),
                    compute_unary_on_reduced=True,
                    unary_params=unary_params,
                    results_dir=method_dir,
                    model_seed=seed,
                    groups=groups,
                    feature_names=feature_names,
                )
                rows_cls_all.extend(cls_rows)
                rows_un_all.extend(un_rows)

    df_cls = pd.DataFrame(rows_cls_all)
    df_un = pd.DataFrame(rows_un_all)
    agg_cls = _aggregate_classical(df_cls.copy())
    agg_un = _aggregate_unary(df_un.copy())

    df_cls.to_csv(seed_root / "all_classical_results.csv", index=False)
    df_un.to_csv(seed_root / "all_unary_results.csv", index=False)
    agg_cls.to_csv(seed_root / "summary_classical.csv", index=False)
    agg_un.to_csv(seed_root / "summary_unary.csv", index=False)

    with open(seed_root / "results_bundle.pkl", "wb") as f:
        pickle.dump({"df_cls": df_cls, "df_un": df_un, "agg_cls": agg_cls, "agg_un": agg_un}, f)

    return {"df_cls": df_cls, "df_un": df_un, "agg_cls": agg_cls, "agg_un": agg_un}


def main() -> None:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)

    combined_cls_parts: list[pd.DataFrame] = []
    combined_un_parts: list[pd.DataFrame] = []

    for seed in SEEDS:
        out = _run_dataset_seed(seed)

        df_cls = out["df_cls"].copy()
        df_un = out["df_un"].copy()

        if not df_cls.empty:
            df_cls.insert(0, "seed", seed)
            df_cls.insert(1, "dataset_key", DATASET_KEY)
            combined_cls_parts.append(df_cls)

        if not df_un.empty:
            df_un.insert(0, "seed", seed)
            df_un.insert(1, "dataset_key", DATASET_KEY)
            combined_un_parts.append(df_un)

    df_cls_all = pd.concat(combined_cls_parts, ignore_index=True) if combined_cls_parts else pd.DataFrame()
    df_un_all = pd.concat(combined_un_parts, ignore_index=True) if combined_un_parts else pd.DataFrame()
    agg_cls_by_seed = _aggregate_classical(df_cls_all.copy(), group_keys=["seed", "dataset", "method", "q", "model"])
    agg_un_by_seed = _aggregate_unary(df_un_all.copy(), group_keys=["seed", "dataset", "method", "q", "model"])
    agg_cls_all = _aggregate_classical(df_cls_all.copy())
    agg_un_all = _aggregate_unary(df_un_all.copy())

    df_cls_all.to_csv(RESULTS_ROOT / "all_classical_results.csv", index=False)
    df_un_all.to_csv(RESULTS_ROOT / "all_unary_results.csv", index=False)
    agg_cls_by_seed.to_csv(RESULTS_ROOT / "summary_classical_by_seed.csv", index=False)
    agg_un_by_seed.to_csv(RESULTS_ROOT / "summary_unary_by_seed.csv", index=False)
    agg_cls_all.to_csv(RESULTS_ROOT / "summary_classical.csv", index=False)
    agg_un_all.to_csv(RESULTS_ROOT / "summary_unary.csv", index=False)

    with open(RESULTS_ROOT / "results_bundle.pkl", "wb") as f:
        pickle.dump(
            {
                "df_cls": df_cls_all,
                "df_un": df_un_all,
                "agg_cls_by_seed": agg_cls_by_seed,
                "agg_un_by_seed": agg_un_by_seed,
                "agg_cls": agg_cls_all,
                "agg_un": agg_un_all,
            },
            f,
        )


if __name__ == "__main__":
    main()
