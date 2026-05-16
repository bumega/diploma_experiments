from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import pandas as pd

from evaluation import evaluate_reduction
from pipelines import _aggregate_classical, _aggregate_unary, _build_dataset_suite_spec, _detect_device
from reducers import EnsembleReducer


SEEDS = [41, 42, 43, 44, 45]
DATASET_KEY = "B_real"
RESULTS_ROOT = Path("results_multi_seed_B_real_qaware")
SOURCE_RESULTS_ROOT = Path("results_multi_seed_B_real_tuned")
QGRIDS = {"B_real": [5, 10, 15]}

BASE_UNARY = {
    "d_hidden": 64,
    "n_hidden_layers": 2,
    "batch_size": 2048,
    "checkpoint_every": 5,
    "checkpoint_n_grid": 19,
}

ENSEMBLE_Q_SPECS: dict[int, dict[str, Any]] = {
    5: {
        "d_hidden": 64,
        "n_hidden_layers": 2,
        "num_epochs": 20,
        "batch_size": 2048,
        "num_models": 16,
        "num_attempts": 160,
        "num_coords": 7,
        "num_samples": 14,
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "split_mode": "split_60_20_20",
        "cG": 0.5,
        "output": True,
    },
    10: {
        "d_hidden": 64,
        "n_hidden_layers": 2,
        "num_epochs": 30,
        "batch_size": 2048,
        "num_models": 24,
        "num_attempts": 240,
        "num_coords": 10,
        "num_samples": 8,
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "split_mode": "split_60_20_20",
        "cG": 0.1,
        "output": True,
    },
    15: {
        "d_hidden": 64,
        "n_hidden_layers": 2,
        "num_epochs": 30,
        "batch_size": 2048,
        "num_models": 24,
        "num_attempts": 240,
        "num_coords": 12,
        "num_samples": 8,
        "checkpoint_every": 5,
        "checkpoint_n_grid": 19,
        "split_mode": "split_60_20_20",
        "cG": 0.25,
        "output": True,
    },
}


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


def _load_rows_from_any(method_dir: Path, fallback_method_dir: Path | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if (method_dir / "all_classical_folds.csv").exists():
        return _load_existing_rows(method_dir)
    if fallback_method_dir is not None and (fallback_method_dir / "all_classical_folds.csv").exists():
        return _load_existing_rows(fallback_method_dir)
    return [], []


def _make_ensemble_ctor(*, q: int, seed: int, device_str: str):
    spec = dict(ENSEMBLE_Q_SPECS[q])

    def build(*, n_select: int | None = None, q: int | None = None, **_: Any) -> EnsembleReducer:
        target_q = int(n_select if n_select is not None else q if q is not None else spec.get("n_select", 0))
        if target_q <= 0:
            target_q = int(q or n_select or 0)
        return EnsembleReducer(
            d_hidden=int(spec["d_hidden"]),
            n_hidden_layers=int(spec["n_hidden_layers"]),
            num_epochs=int(spec["num_epochs"]),
            batch_size=int(spec["batch_size"]),
            n_select=target_q,
            num_models=int(spec["num_models"]),
            num_attempts=int(spec["num_attempts"]),
            num_coords=int(spec["num_coords"]),
            num_samples=int(spec["num_samples"]),
            seed=seed,
            output=bool(spec["output"]),
            device=device_str,
            use_gpu_shap=device_str.startswith("cuda"),
            max_models_per_batch=500,
            max_shap_models_per_batch=128,
            checkpoint_every=int(spec["checkpoint_every"]),
            checkpoint_n_grid=int(spec["checkpoint_n_grid"]),
            split_mode=str(spec["split_mode"]),
            cG=float(spec["cG"]),
        )

    return build


def _make_unary_params(*, q: int, seed: int, device_str: str) -> dict[str, Any]:
    spec = ENSEMBLE_Q_SPECS[q]
    unary = dict(BASE_UNARY)
    unary.update(
        {
            "d_hidden": int(spec["d_hidden"]),
            "n_hidden_layers": int(spec["n_hidden_layers"]),
            "num_epochs": int(spec["num_epochs"]),
            "cG": float(spec["cG"]),
            "seed": seed,
            "device": device_str,
        }
    )
    return unary


def _run_dataset_seed(seed: int) -> dict[str, Any]:
    device = _detect_device()
    device_str = str(device)

    configs, methods, reducer_extra_kwargs = _build_dataset_suite_spec(
        dataset_key=DATASET_KEY,
        device=device,
        dataset_seed=seed,
        reducer_seed=seed,
        c_G=0.25,
        qgrids=QGRIDS,
        rase_params=ENSEMBLE_Q_SPECS[5],
    )
    methods = [(name, ctor) for name, ctor in methods if name != "UMAP_sup"]
    reducer_extra_kwargs.pop("UMAP_sup", None)

    seed_root = RESULTS_ROOT / f"seed_{seed}" / DATASET_KEY
    seed_root.mkdir(parents=True, exist_ok=True)

    rows_cls_all: list[dict[str, Any]] = []
    rows_un_all: list[dict[str, Any]] = []

    for dname, maker, qgrid in configs:
        X, y = maker()
        dataset_dir = seed_root / dname
        dataset_dir.mkdir(parents=True, exist_ok=True)
        source_dataset_dir = SOURCE_RESULTS_ROOT / f"seed_{seed}" / DATASET_KEY / dname

        for name, ctor in methods:
            for q in qgrid:
                method_dir = dataset_dir / f"{name}_q{q}"
                method_dir.mkdir(parents=True, exist_ok=True)
                source_method_dir = source_dataset_dir / f"{name}_q{q}"

                if name != "Ensembleunar":
                    cls_rows, un_rows = _load_rows_from_any(method_dir, source_method_dir)
                    if cls_rows:
                        rows_cls_all.extend(cls_rows)
                        rows_un_all.extend(un_rows)
                        origin = "current-root" if (method_dir / "all_classical_folds.csv").exists() else "tuned-root"
                        print(f"[skip-existing:{origin}] seed={seed} dataset={dname} method={name} q={q}")
                        continue

                if name == "Ensembleunar" and (method_dir / "all_classical_folds.csv").exists():
                    cls_rows, un_rows = _load_existing_rows(method_dir)
                    rows_cls_all.extend(cls_rows)
                    rows_un_all.extend(un_rows)
                    print(f"[skip-existing:current-root] seed={seed} dataset={dname} method={name} q={q}")
                    continue

                current_ctor = ctor
                current_reducer_kwargs = dict(reducer_extra_kwargs.get(name, {}))
                current_unary_params = _make_unary_params(q=q, seed=seed, device_str=device_str)

                if name == "Ensembleunar":
                    current_ctor = _make_ensemble_ctor(q=q, seed=seed, device_str=device_str)
                    current_reducer_kwargs = {}

                print(f"[run] seed={seed} dataset={dname} method={name} q={q}")
                cls_rows, un_rows = evaluate_reduction(
                    X=X,
                    y=y,
                    reducer_name=name,
                    reducer_ctor=current_ctor,
                    q=q,
                    dataset_name=dname,
                    reducer_kwargs=current_reducer_kwargs,
                    compute_unary_on_reduced=True,
                    unary_params=current_unary_params,
                    results_dir=method_dir,
                    model_seed=seed,
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
