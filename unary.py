from __future__ import annotations

from typing import Any

import numpy as np
import torch

import nirs_core as core

train_one_model = core.train_one_model
model_train = core.model_train
metrics_from_sheet = core.metrics_from_sheet
tune_betas_joint = core.tune_betas_joint
strict_unary_metrics = core.strict_unary_metrics
train_unary_pair_three_splits = core.train_unary_pair_three_splits
run_unary_on_saved_folds_strict = core.run_unary_on_saved_folds_strict


def compute_unary_predictions(
    X: torch.Tensor,
    model_pos: torch.nn.Module,
    model_neg: torch.nn.Module,
    beta_pos: float,
    beta_neg: float,
) -> dict[str, Any]:
    return core.unary_predict_detailed(
        X,
        model_pos,
        model_neg,
        beta_pos=beta_pos,
        beta_neg=beta_neg,
    )


def compute_unary_metrics(
    y_true: np.ndarray,
    pred: dict[str, Any],
    *,
    cG: float,
    h_tau: float = 1e-3,
) -> dict[str, Any]:
    return core.strict_unary_metrics(y_true=y_true, pred=pred, cG=cG, h_tau=h_tau)


def compute_strict_unary_metrics(
    y_true: np.ndarray,
    pred: dict[str, Any],
    *,
    cG: float,
    h_tau: float = 1e-3,
) -> dict[str, Any]:
    return compute_unary_metrics(y_true=y_true, pred=pred, cG=cG, h_tau=h_tau)


def select_unary_operating_point(
    X_par: torch.Tensor,
    y_par: torch.Tensor,
    model_pos: torch.nn.Module,
    model_neg: torch.nn.Module,
    *,
    fmin: torch.Tensor,
    fmax: torch.Tensor,
    cG: float,
    checkpoint_n_grid: int = 19,
    h_tau: float = 1e-3,
) -> dict[str, Any]:
    return core._select_betas_on_par_fixed_cG(
        X_par=X_par,
        y_par=y_par,
        model_pos=model_pos,
        model_neg=model_neg,
        fmin=fmin,
        fmax=fmax,
        cG=cG,
        checkpoint_n_grid=checkpoint_n_grid,
        h_tau=h_tau,
    )


def score_unary_operating_point(
    X_eval: torch.Tensor,
    y_eval: torch.Tensor,
    model_pos: torch.nn.Module,
    model_neg: torch.nn.Module,
    *,
    beta_pos: float,
    beta_neg: float,
    cG: float,
) -> dict[str, Any]:
    return core._score_fixed_operating_point(
        X_eval=X_eval,
        y_eval=y_eval,
        model_pos=model_pos,
        model_neg=model_neg,
        beta_pos=beta_pos,
        beta_neg=beta_neg,
        cG=cG,
    )


__all__ = [
    "compute_strict_unary_metrics",
    "compute_unary_metrics",
    "compute_unary_predictions",
    "metrics_from_sheet",
    "model_train",
    "run_unary_on_saved_folds_strict",
    "score_unary_operating_point",
    "select_unary_operating_point",
    "strict_unary_metrics",
    "train_one_model",
    "train_unary_pair_three_splits",
    "tune_betas_joint",
]
