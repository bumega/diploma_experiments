from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

from datasets import make_dataset_colon
from model_aligned_rase import CRITERIA_MAIN, CRITERION_SHADOW, ModelAlignedRaSEReducer


def _as_y01(y: np.ndarray) -> np.ndarray:
    return (np.asarray(y).ravel() > 0).astype(int)


def _safe_auc(y_true: np.ndarray, score: np.ndarray) -> float:
    if np.unique(y_true).size < 2:
        return float("nan")
    return float(roc_auc_score(y_true, score))


def _make_imputer() -> SimpleImputer:
    try:
        return SimpleImputer(strategy="median", keep_empty_features=True)
    except TypeError:
        return SimpleImputer(strategy="median")


def _parse_criteria(raw: str, include_shadow: bool) -> list[str]:
    if raw.strip().lower() in {"main", "default"}:
        criteria = list(CRITERIA_MAIN)
    else:
        criteria = [item.strip() for item in raw.split(",") if item.strip()]
    if include_shadow and CRITERION_SHADOW not in criteria:
        criteria.append(CRITERION_SHADOW)
    allowed = set(CRITERIA_MAIN + [CRITERION_SHADOW])
    unknown = sorted(set(criteria) - allowed)
    if unknown:
        raise ValueError(f"Unknown criteria: {unknown}. Allowed: {sorted(allowed)}")
    return criteria


def _summarize(df: pd.DataFrame) -> pd.DataFrame:
    metric_cols = [
        "accuracy",
        "balanced_accuracy",
        "f1",
        "mcc",
        "roc_auc",
        "pr_auc",
        "fit_reducer_seconds",
        "fit_model_seconds",
        "n_selected",
    ]
    rows: list[dict[str, Any]] = []
    for criterion, group in df.groupby("criterion", dropna=False):
        row: dict[str, Any] = {"criterion": criterion, "n_folds": int(group["fold"].nunique())}
        for col in metric_cols:
            vals = pd.to_numeric(group[col], errors="coerce")
            row[f"{col}_mean"] = float(vals.mean(skipna=True))
            row[f"{col}_std"] = float(vals.std(ddof=1, skipna=True))
            row[f"{col}_median"] = float(vals.median(skipna=True))
        rows.append(row)
    return pd.DataFrame(rows).sort_values("criterion").reset_index(drop=True)


def _evaluate_final_svm(
    X_train_red: np.ndarray,
    y_train: np.ndarray,
    X_test_red: np.ndarray,
    y_test: np.ndarray,
    *,
    seed: int,
    class_weight: str | None,
) -> tuple[dict[str, Any], SVC]:
    model = SVC(
        C=1.0,
        gamma="scale",
        kernel="rbf",
        class_weight=class_weight,
        probability=False,
        random_state=seed,
    )
    t0 = perf_counter()
    model.fit(X_train_red, y_train)
    fit_seconds = perf_counter() - t0

    y_pred = model.predict(X_test_red)
    score = model.decision_function(X_test_red)
    cm = confusion_matrix(y_test, y_pred, labels=[0, 1])
    tn, fp, fn, tp = [int(v) for v in cm.ravel()]
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_test, y_pred)),
        "f1": float(f1_score(y_test, y_pred, pos_label=1, zero_division=0)),
        "mcc": float(matthews_corrcoef(y_test, y_pred)),
        "roc_auc": _safe_auc(y_test, score),
        "pr_auc": float(average_precision_score(y_test, score)),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
        "fit_model_seconds": float(fit_seconds),
    }
    return metrics, model


def run(args: argparse.Namespace) -> pd.DataFrame:
    results_root = Path(args.results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    criteria = _parse_criteria(args.criteria, args.include_shadow)

    status_path = results_root / "status.json"
    status_path.write_text(
        json.dumps({"status": "running", "criteria": criteria}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    X, y_raw = make_dataset_colon()
    X = np.asarray(X, dtype=np.float64)
    y = _as_y01(np.asarray(y_raw))

    splitter = StratifiedKFold(n_splits=args.outer_folds, shuffle=True, random_state=args.cv_seed)
    splits = list(splitter.split(X, y))
    if args.max_outer_folds is not None:
        splits = splits[: int(args.max_outer_folds)]

    run_info = {
        "dataset": "colon",
        "n_samples": int(X.shape[0]),
        "n_features": int(X.shape[1]),
        "class_counts": {str(k): int(v) for k, v in zip(*np.unique(y, return_counts=True))},
        "criteria": criteria,
        "n_select": int(args.n_select),
        "outer_folds": int(args.outer_folds),
        "max_outer_folds": args.max_outer_folds,
        "inner_cv_splits": int(args.inner_cv_splits),
        "num_models": int(args.num_models),
        "num_attempts": int(args.num_attempts),
        "max_subspace_size": int(args.max_subspace_size),
        "class_weight": args.class_weight,
        "cv_seed": int(args.cv_seed),
        "reducer_seed": int(args.reducer_seed),
        "model_seed": int(args.model_seed),
    }
    (results_root / "run_info.json").write_text(
        json.dumps(run_info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    all_rows: list[dict[str, Any]] = []
    all_fold_index = np.arange(X.shape[0], dtype=int)

    for criterion in criteria:
        criterion_dir = results_root / criterion
        criterion_dir.mkdir(parents=True, exist_ok=True)
        fold_rows: list[dict[str, Any]] = []

        for fold_id, (train_idx, test_idx) in enumerate(splits, start=1):
            fold_json_path = criterion_dir / f"fold_{fold_id:02d}_summary.json"
            if fold_json_path.exists() and not args.force:
                with open(fold_json_path, "r", encoding="utf-8") as f:
                    row = json.load(f)
                fold_rows.append(row)
                all_rows.append(row)
                print(f"[skip-existing] criterion={criterion} fold={fold_id}")
                continue

            print("=" * 90)
            print(f"[run] criterion={criterion} fold={fold_id}/{len(splits)}")
            X_train_raw, X_test_raw = X[train_idx], X[test_idx]
            y_train, y_test = y[train_idx], y[test_idx]

            imputer = _make_imputer()
            X_train_imp = imputer.fit_transform(X_train_raw)
            X_test_imp = imputer.transform(X_test_raw)

            scaler = StandardScaler()
            X_train_sc = scaler.fit_transform(X_train_imp)
            X_test_sc = scaler.transform(X_test_imp)

            reducer = ModelAlignedRaSEReducer(
                n_select=args.n_select,
                criterion=criterion,
                num_models=args.num_models,
                num_attempts=args.num_attempts,
                max_subspace_size=args.max_subspace_size,
                inner_cv_splits=args.inner_cv_splits,
                lambda_std=args.lambda_std,
                mu_red=args.mu_red,
                ebic_gamma=args.ebic_gamma,
                bic_mode=args.bic_mode,
                shadow_repeats=args.shadow_repeats,
                class_weight=args.class_weight,
                seed=args.reducer_seed + 1000 * fold_id,
                output=args.verbose,
            )

            t0 = perf_counter()
            X_train_red = reducer.fit_transform(X_train_sc, y_train)
            X_test_red = reducer.transform(X_test_sc)
            fit_reducer_seconds = perf_counter() - t0

            metrics, _ = _evaluate_final_svm(
                X_train_red,
                y_train,
                X_test_red,
                y_test,
                seed=args.model_seed + fold_id,
                class_weight=args.class_weight,
            )
            selected = reducer.selected_indices_.astype(int).tolist()

            row = {
                "criterion": criterion,
                "fold": int(fold_id),
                "train_size": int(train_idx.size),
                "test_size": int(test_idx.size),
                "n_selected": int(len(selected)),
                "selected_indices": ",".join(map(str, selected)),
                "train_indices": ",".join(map(str, train_idx.astype(int).tolist())),
                "test_indices": ",".join(map(str, test_idx.astype(int).tolist())),
                "fit_reducer_seconds": float(fit_reducer_seconds),
                **metrics,
            }
            fold_rows.append(row)
            all_rows.append(row)

            detail = {
                **row,
                "selected_indices_list": selected,
                "train_indices_list": train_idx.astype(int).tolist(),
                "test_indices_list": test_idx.astype(int).tolist(),
                "reducer_history": reducer.history_,
                "feature_importances": reducer.feature_importances_.astype(float).tolist(),
            }
            with open(fold_json_path, "w", encoding="utf-8") as f:
                json.dump(detail, f, ensure_ascii=False, indent=2)

            pd.DataFrame(fold_rows).sort_values("fold").to_csv(
                criterion_dir / "fold_metrics.csv",
                index=False,
            )
            summary = _summarize(pd.DataFrame(fold_rows))
            summary.to_csv(criterion_dir / "summary_metrics.csv", index=False)

            seen_test = np.fromiter(
                (int(item) for row_cur in fold_rows for item in str(row_cur["test_indices"]).split(",") if item),
                dtype=int,
            )
            if seen_test.size and np.unique(seen_test).size > all_fold_index.size:
                raise RuntimeError("Collected more unique test indices than samples")

            print(
                f"[done] criterion={criterion} fold={fold_id} "
                f"acc={row['accuracy']:.4f} bal_acc={row['balanced_accuracy']:.4f} "
                f"f1={row['f1']:.4f} mcc={row['mcc']:.4f} auc={row['roc_auc']:.4f}"
            )

        if fold_rows:
            df_fold = pd.DataFrame(fold_rows).sort_values("fold").reset_index(drop=True)
            df_fold.to_csv(criterion_dir / "fold_metrics.csv", index=False)
            _summarize(df_fold).to_csv(criterion_dir / "summary_metrics.csv", index=False)

    df_all = pd.DataFrame(all_rows)
    if not df_all.empty:
        df_all = df_all.sort_values(["criterion", "fold"]).reset_index(drop=True)
        df_all.to_csv(results_root / "fold_metrics.csv", index=False)
        _summarize(df_all).to_csv(results_root / "summary_metrics.csv", index=False)

    status_path.write_text(
        json.dumps({"status": "finished", "criteria": criteria}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return df_all


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run model-aligned RaSE on Colon cancer with 10-fold CV.")
    parser.add_argument("--results-root", default="results_colon_model_aligned_rase_q20")
    parser.add_argument("--criteria", default="main", help="Comma-separated criteria or 'main'.")
    parser.add_argument("--include-shadow", action="store_true")
    parser.add_argument("--n-select", type=int, default=20)
    parser.add_argument("--outer-folds", type=int, default=10)
    parser.add_argument("--max-outer-folds", type=int, default=None)
    parser.add_argument("--inner-cv-splits", type=int, default=5)
    parser.add_argument("--num-models", type=int, default=20)
    parser.add_argument("--num-attempts", type=int, default=32)
    parser.add_argument("--max-subspace-size", type=int, default=20)
    parser.add_argument("--lambda-std", type=float, default=0.5)
    parser.add_argument("--mu-red", type=float, default=0.05)
    parser.add_argument("--ebic-gamma", type=float, default=0.5)
    parser.add_argument("--bic-mode", choices=["bic", "ebic"], default="ebic")
    parser.add_argument("--shadow-repeats", type=int, default=5)
    parser.add_argument("--class-weight", default="balanced")
    parser.add_argument("--cv-seed", type=int, default=42)
    parser.add_argument("--reducer-seed", type=int, default=42)
    parser.add_argument("--model-seed", type=int, default=42)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if str(args.class_weight).lower() in {"none", "null"}:
        args.class_weight = None
    run(args)


if __name__ == "__main__":
    main()
