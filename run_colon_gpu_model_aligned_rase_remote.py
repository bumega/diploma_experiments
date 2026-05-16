from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import torch
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, average_precision_score, balanced_accuracy_score, confusion_matrix, f1_score, matthews_corrcoef, roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler

from datasets import make_dataset_colon
from gpu_model_aligned_rase import GPU_CRITERIA_MAIN, GPU_CRITERION_SHADOW, GpuModelAlignedRaSEReducer, GpuRaseConfig, GpuRbfKernelClassifier


def _criteria(raw: str, include_shadow: bool) -> list[str]:
    out = list(GPU_CRITERIA_MAIN) if raw == "main" else [x.strip() for x in raw.split(",") if x.strip()]
    if include_shadow and GPU_CRITERION_SHADOW not in out:
        out.append(GPU_CRITERION_SHADOW)
    return out


def _metrics(y_true: np.ndarray, pred: np.ndarray, score: np.ndarray) -> dict:
    cm = confusion_matrix(y_true, pred, labels=[0, 1])
    tn, fp, fn, tp = [int(x) for x in cm.ravel()]
    return {
        "accuracy": float(accuracy_score(y_true, pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, pred)),
        "f1": float(f1_score(y_true, pred, zero_division=0)),
        "mcc": float(matthews_corrcoef(y_true, pred)),
        "roc_auc": float(roc_auc_score(y_true, score)) if np.unique(y_true).size == 2 else np.nan,
        "pr_auc": float(average_precision_score(y_true, score)),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
    }


def _summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    cols = ["accuracy", "balanced_accuracy", "f1", "mcc", "roc_auc", "pr_auc", "fit_reducer_seconds", "fit_model_seconds"]
    for criterion, g in df.groupby("criterion"):
        row = {"criterion": criterion, "n_folds": int(g.fold.nunique())}
        for col in cols:
            row[f"{col}_mean"] = float(g[col].mean())
            row[f"{col}_std"] = float(g[col].std(ddof=1))
            row[f"{col}_median"] = float(g[col].median())
        rows.append(row)
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> pd.DataFrame:
    if not torch.cuda.is_available() and args.device.startswith("cuda"):
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is false")

    root = Path(args.results_root)
    root.mkdir(parents=True, exist_ok=True)
    criteria = _criteria(args.criteria, args.include_shadow)
    X, y_raw = make_dataset_colon()
    X = np.asarray(X, dtype=np.float64)
    y = (np.asarray(y_raw).ravel() > 0).astype(int)

    cfg_base = {
        "n_select": args.n_select,
        "num_iterations": args.num_iterations,
        "models_per_iteration": args.models_per_iteration,
        "num_attempts": args.num_attempts,
        "max_subspace_size": args.max_subspace_size,
        "inner_cv_splits": args.inner_cv_splits,
        "candidate_batch_size": args.candidate_batch_size,
        "shadow_repeats": args.shadow_repeats,
        "seed": args.reducer_seed,
        "device": args.device,
        "output": args.verbose,
    }
    info = {
        "backend": "torch_cuda_rbf_kernel_classifier",
        "torch": torch.__version__,
        "cuda_available": torch.cuda.is_available(),
        "device": args.device,
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "criteria": criteria,
        **cfg_base,
    }
    (root / "run_info.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / "status.json").write_text(json.dumps({"status": "running", "criteria": criteria}, indent=2), encoding="utf-8")

    splitter = StratifiedKFold(n_splits=args.outer_folds, shuffle=True, random_state=args.cv_seed)
    splits = list(splitter.split(X, y))
    if args.max_outer_folds is not None:
        splits = splits[: args.max_outer_folds]

    all_rows = []
    for criterion in criteria:
        croot = root / criterion
        croot.mkdir(exist_ok=True)
        rows = []
        for fold, (tr, te) in enumerate(splits, 1):
            fjson = croot / f"fold_{fold:02d}_summary.json"
            if fjson.exists() and not args.force:
                row = json.loads(fjson.read_text(encoding="utf-8"))
                rows.append(row)
                all_rows.append(row)
                print(f"[skip] {criterion} fold={fold}", flush=True)
                continue
            imp = SimpleImputer(strategy="median")
            Xtr = imp.fit_transform(X[tr])
            Xte = imp.transform(X[te])
            sc = StandardScaler()
            Xtr = sc.fit_transform(Xtr)
            Xte = sc.transform(Xte)

            cfg = GpuRaseConfig(**{**cfg_base, "seed": args.reducer_seed + 1000 * fold})
            reducer = GpuModelAlignedRaSEReducer(criterion, cfg)
            t0 = perf_counter()
            Ztr = reducer.fit_transform(Xtr, y[tr])
            Zte = reducer.transform(Xte)
            tred = perf_counter() - t0

            t0 = perf_counter()
            model = GpuRbfKernelClassifier(C=1.0, gamma=args.final_gamma, device=args.device).fit(Ztr, y[tr])
            score = model.decision_function(Zte)
            pred = (score > 0).astype(int)
            tmodel = perf_counter() - t0

            row = {
                "criterion": criterion,
                "fold": fold,
                "backend": "torch_cuda_rbf_kernel_classifier",
                "train_size": int(len(tr)),
                "test_size": int(len(te)),
                "n_selected": int(len(reducer.selected_indices_)),
                "selected_indices": ",".join(map(str, reducer.selected_indices_.tolist())),
                "fit_reducer_seconds": float(tred),
                "fit_model_seconds": float(tmodel),
                **_metrics(y[te], pred, score),
            }
            rows.append(row)
            all_rows.append(row)
            fjson.write_text(json.dumps({**row, "reducer_history": reducer.history_}, ensure_ascii=False, indent=2), encoding="utf-8")
            pd.DataFrame(rows).to_csv(croot / "fold_metrics.csv", index=False)
            _summary(pd.DataFrame(rows)).to_csv(croot / "summary_metrics.csv", index=False)
            print(f"[done-gpu] {criterion} fold={fold} acc={row['accuracy']:.4f} mcc={row['mcc']:.4f} auc={row['roc_auc']:.4f}", flush=True)
        if rows:
            pd.DataFrame(rows).to_csv(croot / "fold_metrics.csv", index=False)
            _summary(pd.DataFrame(rows)).to_csv(croot / "summary_metrics.csv", index=False)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.to_csv(root / "fold_metrics.csv", index=False)
        _summary(df).to_csv(root / "summary_metrics.csv", index=False)
    (root / "status.json").write_text(json.dumps({"status": "finished", "criteria": criteria}, indent=2), encoding="utf-8")
    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--results-root", default="results_colon_gpu_model_aligned_rase_q20_big_all6")
    p.add_argument("--criteria", default="main")
    p.add_argument("--include-shadow", action="store_true")
    p.add_argument("--n-select", type=int, default=20)
    p.add_argument("--outer-folds", type=int, default=10)
    p.add_argument("--max-outer-folds", type=int, default=None)
    p.add_argument("--inner-cv-splits", type=int, default=5)
    p.add_argument("--num-iterations", type=int, default=4)
    p.add_argument("--models-per-iteration", type=int, default=100)
    p.add_argument("--num-attempts", type=int, default=5000)
    p.add_argument("--max-subspace-size", type=int, default=20)
    p.add_argument("--candidate-batch-size", type=int, default=256)
    p.add_argument("--shadow-repeats", type=int, default=5)
    p.add_argument("--final-gamma", type=float, default=0.1)
    p.add_argument("--cv-seed", type=int, default=42)
    p.add_argument("--reducer-seed", type=int, default=42)
    p.add_argument("--device", default="cuda")
    p.add_argument("--force", action="store_true")
    p.add_argument("--verbose", action="store_true")
    run(p.parse_args())


if __name__ == "__main__":
    main()
