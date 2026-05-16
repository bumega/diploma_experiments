from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import accuracy_score, average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

import nirs_core as core
from unary import compute_unary_metrics, compute_unary_predictions, train_unary_pair_three_splits

_clone_estimator = core._clone_estimator
_get_score_vector = core._get_score_vector
KDEBayesClassifier = core.KDEBayesClassifier
get_models = core.get_models


def _classification_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    acc = float(accuracy_score(y_true, y_pred))
    err = float(1.0 - acc)
    f1 = float(f1_score(y_true, y_pred, pos_label=1, average="binary", zero_division=0))
    return {"accuracy": acc, "classification_error": err, "f1_score": f1}


def _ranking_metrics(y_true: np.ndarray, score: np.ndarray) -> dict[str, float]:
    y = np.asarray(y_true).astype(int).ravel()
    s = np.asarray(score, dtype=float).ravel()
    order = np.argsort(-s)
    y_sorted = y[order]
    n_pos = int(y.sum())
    out: dict[str, float] = {}

    for k in (10, 50, 100):
        kk = min(k, len(y_sorted))
        if kk == 0:
            out[f"precision_at_{k}"] = np.nan
            out[f"recall_at_{k}"] = np.nan
            continue
        tp = int(y_sorted[:kk].sum())
        out[f"precision_at_{k}"] = float(tp / kk)
        out[f"recall_at_{k}"] = float(tp / n_pos) if n_pos else np.nan

    kk = min(max(n_pos, 1), len(y_sorted))
    tp = int(y_sorted[:kk].sum()) if kk else 0
    out["precision_at_n_pos"] = float(tp / kk) if kk else np.nan
    out["recall_at_n_pos"] = float(tp / n_pos) if n_pos else np.nan

    fp_cum = np.cumsum(1 - y_sorted)
    tp_cum = np.cumsum(y_sorted)
    for fp_limit in (1, 5, 10, 50, 100):
        valid = np.flatnonzero(fp_cum <= fp_limit)
        out[f"recall_at_fp_{fp_limit}"] = float(tp_cum[valid[-1]] / n_pos) if n_pos and valid.size else 0.0
    return out


def _make_median_imputer() -> SimpleImputer:
    try:
        return SimpleImputer(strategy="median", keep_empty_features=True)
    except TypeError:
        return SimpleImputer(strategy="median")


def _strict_unary_on_reduced(
    Ztr: np.ndarray,
    ytr_orig: np.ndarray,
    Zte: np.ndarray,
    yte_orig: np.ndarray,
    unary_params: dict[str, Any],
) -> dict[str, Any]:
    unary_fit = train_unary_pair_three_splits(
        X_train_sc=Ztr,
        y_train=ytr_orig,
        d_hidden=unary_params.get("d_hidden", 32),
        n_hidden_layers=unary_params.get("n_hidden_layers", 1),
        num_epochs=unary_params.get("num_epochs", 50),
        batch_size=unary_params.get("batch_size", 256),
        lr=unary_params.get("lr", 3e-3),
        weight_decay=unary_params.get("weight_decay", 1e-2),
        checkpoint_every=unary_params.get("checkpoint_every", 5),
        checkpoint_n_grid=unary_params.get("checkpoint_n_grid", 19),
        cG=unary_params.get("cG", 0.25),
        par_fraction=unary_params.get("par_fraction", 0.2),
        ckpt_fraction=unary_params.get("ckpt_fraction", 0.2),
        seed=unary_params.get("seed", 42),
        device=unary_params.get("device"),
    )

    device = next(unary_fit["model_pos"].parameters()).device
    X_te_t = torch.as_tensor(Zte, dtype=torch.float32, device=device)
    y_te_t = torch.as_tensor(yte_orig, dtype=torch.int64, device=device)
    pred = compute_unary_predictions(
        X_te_t,
        unary_fit["model_pos"],
        unary_fit["model_neg"],
        beta_pos=float(unary_fit["beta_pos"]),
        beta_neg=float(unary_fit["beta_neg"]),
    )
    metrics = compute_unary_metrics(
        y_true=y_te_t.detach().cpu().numpy(),
        pred=pred,
        cG=float(unary_fit["cG"]),
    )
    return {**unary_fit, "test_metrics": metrics}


def evaluate_reduction(
    X: np.ndarray,
    y: np.ndarray,
    reducer_name: str,
    reducer_ctor: Any,
    q: int,
    dataset_name: str,
    n_splits: int = 5,
    reducer_kwargs: Optional[Dict[str, Any]] = None,
    compute_unary_on_reduced: bool = True,
    unary_params: Optional[Dict[str, Any]] = None,
    results_dir: Optional[str | Path] = None,
    model_seed: int = 42,
    groups: Optional[np.ndarray] = None,
    feature_names: Optional[List[str]] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    reducer_kwargs = reducer_kwargs or {}
    unary_params = unary_params or {}

    save_root = Path(results_dir) if results_dir is not None else None
    if save_root is not None:
        save_root.mkdir(parents=True, exist_ok=True)

    y_orig = np.asarray(y).ravel()
    y_bin = (y_orig == 1).astype(int) if set(np.unique(y_orig)) == {-1, 1} else y_orig.astype(int)
    if groups is not None:
        groups_arr = np.asarray(groups).ravel()
        unique_groups = np.unique(groups_arr)
        pos_groups = np.unique(groups_arr[y_bin == 1])
        neg_groups = np.unique(groups_arr[y_bin == 0])
        effective_splits = min(n_splits, len(unique_groups), len(pos_groups), len(neg_groups))
        if effective_splits >= 2:
            splitter = StratifiedGroupKFold(n_splits=effective_splits, shuffle=True, random_state=42)
            split_iter = splitter.split(X, y_bin, groups=groups_arr)
            split_kind = "StratifiedGroupKFold"
        else:
            fallback_splits = min(n_splits, int(np.bincount(y_bin).min()))
            if fallback_splits < 2:
                raise ValueError("Need at least two samples per class, or at least two positive and negative groups, for CV.")
            splitter = StratifiedKFold(n_splits=fallback_splits, shuffle=True, random_state=42)
            split_iter = splitter.split(X, y_bin)
            split_kind = "StratifiedKFold_fallback"
    else:
        splitter = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        split_iter = splitter.split(X, y_bin)
        split_kind = "StratifiedKFold"

    rows_cls: List[Dict[str, Any]] = []
    rows_unary: List[Dict[str, Any]] = []

    for fold, (tr, te) in enumerate(split_iter, 1):
        print(f"fold = {fold}")
        fold_dir = save_root / f"fold_{fold:02d}" if save_root is not None else None
        if fold_dir is not None:
            fold_dir.mkdir(parents=True, exist_ok=True)
            fold_meta_path = fold_dir / "fold_meta.json"
            fold_cls_path = fold_dir / "classical_metrics.csv"
            fold_unary_path = fold_dir / "unary_metrics.json"
            if fold_meta_path.exists() and fold_cls_path.exists():
                fold_cls_df = pd.read_csv(fold_cls_path)
                if not fold_cls_df.empty:
                    rows_cls.extend(fold_cls_df.to_dict("records"))
                    if compute_unary_on_reduced and fold_unary_path.exists():
                        with open(fold_unary_path, "r", encoding="utf-8") as f:
                            rows_unary.append(json.load(f))
                    print(f"[skip-fold-existing] fold={fold}")
                    continue

        Xtr, Xte = X[tr], X[te]
        ytr_bin, yte_bin = y_bin[tr], y_bin[te]
        ytr_orig, yte_orig = y_orig[tr], y_orig[te]

        imputer = _make_median_imputer()
        Xtr = imputer.fit_transform(Xtr)
        Xte = imputer.transform(Xte)
        if feature_names is not None:
            from rdt_dataset import repair_training_matrix

            Xtr = repair_training_matrix(Xtr, feature_names)
            Xte = repair_training_matrix(Xte, feature_names)

        scaler = StandardScaler()
        Xtr_s = scaler.fit_transform(Xtr)
        Xte_s = scaler.transform(Xte)

        models = get_models(seed=model_seed + fold)
        base_scores: Dict[str, tuple[float, float, float, dict[str, float]]] = {}
        for mname, base_model in models.items():
            m0 = _clone_estimator(base_model)
            t0 = core.perf_counter()
            m0.fit(Xtr_s, ytr_bin)
            t0 = core.perf_counter() - t0
            p0 = _get_score_vector(m0, Xte_s)
            base_scores[mname] = (
                roc_auc_score(yte_bin, p0),
                average_precision_score(yte_bin, p0),
                t0,
                _ranking_metrics(yte_bin, p0),
            )

        try:
            red = reducer_ctor(q=q, **reducer_kwargs)
        except TypeError:
            red = reducer_ctor(n_select=q, **reducer_kwargs)

        tR = core.perf_counter()
        Ztr = red.fit_transform(Xtr_s, ytr_orig)
        Zte = red.transform(Xte_s)
        tR = core.perf_counter() - tR

        selected_indices = None
        if hasattr(red, "selected_indices_") and red.selected_indices_ is not None:
            selected_indices = np.asarray(red.selected_indices_, dtype=int).tolist()

        if fold_dir is not None:
            with open(fold_dir / "fold_meta.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "fold": fold,
                        "dataset": dataset_name,
                        "reducer_name": reducer_name,
                        "q": int(q),
                        "train_indices": tr.tolist(),
                        "test_indices": te.tolist(),
                        "selected_indices": selected_indices,
                        "split_kind": split_kind,
                        "train_shape_before": list(Xtr.shape),
                        "test_shape_before": list(Xte.shape),
                        "train_shape_after": list(np.asarray(Ztr).shape),
                        "test_shape_after": list(np.asarray(Zte).shape),
                        "t_reducer": float(tR),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

        fold_cls_rows = []
        for mname, base_model in models.items():
            auc0, ap0, t0, rank0 = base_scores[mname]
            m1 = _clone_estimator(base_model)
            t1 = core.perf_counter()
            m1.fit(Ztr, ytr_bin)
            t1 = core.perf_counter() - t1
            p1 = _get_score_vector(m1, Zte)
            rank1 = _ranking_metrics(yte_bin, p1)
            row = {
                "dataset": dataset_name,
                "method": reducer_name,
                "q": q,
                "model": mname,
                "fold": fold,
                "AUC": roc_auc_score(yte_bin, p1),
                "PR_AUC": average_precision_score(yte_bin, p1),
                "ΔAUC": roc_auc_score(yte_bin, p1) - auc0,
                "ΔPR_AUC": average_precision_score(yte_bin, p1) - ap0,
                "AUC_base": auc0,
                "PR_AUC_base": ap0,
                "t_reducer": tR,
                "t_model": t1,
                "t_model_base": t0,
                "n_selected": None if selected_indices is None else len(selected_indices),
                "selected_indices": None if selected_indices is None else ",".join(map(str, selected_indices)),
            }
            row.update(rank1)
            row.update({f"{key}_base": value for key, value in rank0.items()})
            rows_cls.append(row)
            fold_cls_rows.append(row)

        if fold_dir is not None and fold_cls_rows:
            pd.DataFrame(fold_cls_rows).to_csv(fold_dir / "classical_metrics.csv", index=False)
            if selected_indices is not None:
                with open(fold_dir / "selected_indices.json", "w", encoding="utf-8") as f:
                    json.dump({"selected_indices": selected_indices}, f, ensure_ascii=False, indent=2)

        if compute_unary_on_reduced:
            unary_eval = _strict_unary_on_reduced(
                Ztr=np.asarray(Ztr, dtype=np.float32),
                ytr_orig=np.asarray(ytr_orig, dtype=np.int64),
                Zte=np.asarray(Zte, dtype=np.float32),
                yte_orig=np.asarray(yte_orig, dtype=np.int64),
                unary_params=unary_params,
            )
            m_un = unary_eval["test_metrics"]
            unary_row = {
                "dataset": dataset_name,
                "method": reducer_name,
                "q": q,
                "model": "UnaryMLP",
                "fold": fold,
                "F12": float(m_un["F12"]),
                "G12": float(m_un["G12"]),
                "S_test": float(m_un["S"]),
                "a1": float(m_un["a1"]),
                "a2": float(m_un["a2"]),
                "b1": float(m_un["b1"]),
                "b2": float(m_un["b2"]),
                "coverage": float(m_un["coverage"]),
                "reject_rate": float(m_un["reject_rate"]),
                "conflict_rate": float(m_un["conflict_rate"]),
                "wrong_exclusive_rate": float(m_un["wrong_exclusive_rate"]),
                "correct_exclusive_rate": float(m_un["correct_exclusive_rate"]),
                "selective_accuracy": float(m_un["selective_accuracy"]) if not np.isnan(m_un["selective_accuracy"]) else np.nan,
                "selective_f1": float(m_un["selective_f1"]) if not np.isnan(m_un["selective_f1"]) else np.nan,
                "conservative_accuracy": float(m_un["conservative_accuracy"]) if not np.isnan(m_un["conservative_accuracy"]) else np.nan,
                "conservative_error": float(m_un["conservative_error"]) if not np.isnan(m_un["conservative_error"]) else np.nan,
                "beta_pos": float(unary_eval["beta_pos"]),
                "beta_neg": float(unary_eval["beta_neg"]),
                "best_epoch": int(unary_eval["best_epoch"]),
                "S_par": float(unary_eval["best_S_par"]),
                "S_ckpt": float(unary_eval["best_S_ckpt"]),
                "F12_par": float(unary_eval["best_F12_par"]),
                "G12_par": float(unary_eval["best_G12_par"]),
                "F12_ckpt": float(unary_eval["best_F12_ckpt"]),
                "G12_ckpt": float(unary_eval["best_G12_ckpt"]),
                "cG": float(unary_eval["cG"]),
                "fit_size_inner": int(unary_eval["fit_size"]),
                "par_size_inner": int(unary_eval["par_size"]),
                "ckpt_size_inner": int(unary_eval["ckpt_size"]),
                "t_reducer": tR,
                "t_model": 0.0,
                "unary_device": str(next(unary_eval["model_pos"].parameters()).device),
                "n_selected": None if selected_indices is None else len(selected_indices),
                "selected_indices": None if selected_indices is None else ",".join(map(str, selected_indices)),
            }
            rows_unary.append(unary_row)
            if fold_dir is not None:
                with open(fold_dir / "unary_metrics.json", "w", encoding="utf-8") as f:
                    json.dump(unary_row, f, ensure_ascii=False, indent=2)

    if save_root is not None:
        if rows_cls:
            pd.DataFrame(rows_cls).to_csv(save_root / "all_classical_folds.csv", index=False)
        if rows_unary:
            pd.DataFrame(rows_unary).to_csv(save_root / "all_unary_folds.csv", index=False)

    return rows_cls, rows_unary


def _run_classifier_on_saved_folds(
    results_dir: str,
    *,
    model_builder: Callable[[int], Any],
    model_name: str,
    seed: int = 42,
    save_name: Optional[str] = None,
) -> pd.DataFrame:
    return core._run_classifier_on_saved_folds(
        results_dir=results_dir,
        model_builder=model_builder,
        model_name=model_name,
        seed=seed,
        save_name=save_name,
    )


run_svm_on_saved_folds = core.run_svm_on_saved_folds
run_knn_on_saved_folds = core.run_knn_on_saved_folds
run_kde_bayes_on_saved_folds = core.run_kde_bayes_on_saved_folds
run_qda_on_saved_folds = core.run_qda_on_saved_folds
run_shrinkage_lda_on_saved_folds = core.run_shrinkage_lda_on_saved_folds
run_gaussiannb_on_saved_folds = core.run_gaussiannb_on_saved_folds
run_mlp_on_saved_folds = core.run_mlp_on_saved_folds


__all__ = [
    "KDEBayesClassifier",
    "_classification_metrics",
    "_run_classifier_on_saved_folds",
    "evaluate_reduction",
    "run_gaussiannb_on_saved_folds",
    "run_kde_bayes_on_saved_folds",
    "run_knn_on_saved_folds",
    "run_mlp_on_saved_folds",
    "run_qda_on_saved_folds",
    "run_shrinkage_lda_on_saved_folds",
    "run_svm_on_saved_folds",
]
