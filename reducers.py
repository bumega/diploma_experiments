from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import torch
from sklearn.model_selection import train_test_split

import nirs_core as core
from unary import score_unary_operating_point, train_unary_pair_three_splits

try:
    from umap import UMAP  # type: ignore
except Exception:  # pragma: no cover
    try:
        from umap.umap_ import UMAP  # type: ignore
    except Exception:  # pragma: no cover
        UMAP = None

if UMAP is not None:
    core.UMAP = UMAP


ReducerBase = core.ReducerBase
PCAReducer = core.PCAReducer
PLSReducer = core.PLSReducer
HSICSelector = core.HSICSelector
EnsembleReducer = core.EnsembleReducer


class UMAPReducer(core.ReducerBase):
    def __init__(self, n_components: int):
        if UMAP is None:
            raise ImportError("UMAP is not available. Install `umap-learn` to use UMAPReducer.")
        self.umap = UMAP(
            n_components=n_components,
            random_state=42,
            n_neighbors=15,
            min_dist=0.1,
        )

    def fit(self, X, y=None):
        Xn = self._to_numpy(X)
        yn = None if y is None else self._to_numpy_y(y).ravel()
        self.umap.fit(Xn, y=yn)
        return self

    def transform(self, X):
        Xn = self._to_numpy(X)
        return self.umap.transform(Xn)


@dataclass
class UnaryChooserConfig:
    checkpoint_every: int = 5
    checkpoint_n_grid: int = 19
    cG: float = 0.25
    par_fraction: float = 0.20
    ckpt_fraction: float = 0.20
    lr: float = 3e-3
    weight_decay: float = 1e-2
    device: str | None = None


def choose_coord_criterion_aligned(
    X: torch.Tensor,
    y: torch.Tensor,
    d_hidden: int,
    n_hidden_layers: int,
    num_epochs: int,
    batch_size: int = 128,
    seed: int = 0,
    output: bool = False,
    *,
    chooser_cfg: UnaryChooserConfig | None = None,
):
    chooser_cfg = chooser_cfg or UnaryChooserConfig()

    X_np = np.asarray(X.detach().cpu().numpy(), dtype=np.float32)
    y_np = np.asarray(y.detach().cpu().numpy(), dtype=np.int64).ravel()

    train_idx, val_idx = train_test_split(
        np.arange(len(y_np)),
        test_size=0.2,
        random_state=seed,
        stratify=y_np,
    )

    X_tr = X_np[train_idx]
    y_tr = y_np[train_idx]
    X_val = X_np[val_idx]
    y_val = y_np[val_idx]

    rows: list[dict[str, Any]] = []
    best_idx = 0
    best_score = -float("inf")

    for local_idx in range(X_np.shape[1]):
        X_tr_upd = np.delete(X_tr, local_idx, axis=1)
        X_val_upd = np.delete(X_val, local_idx, axis=1)

        unary_fit = train_unary_pair_three_splits(
            X_train_sc=X_tr_upd,
            y_train=y_tr,
            d_hidden=d_hidden,
            n_hidden_layers=n_hidden_layers,
            num_epochs=num_epochs,
            batch_size=batch_size,
            lr=chooser_cfg.lr,
            weight_decay=chooser_cfg.weight_decay,
            checkpoint_every=chooser_cfg.checkpoint_every,
            checkpoint_n_grid=chooser_cfg.checkpoint_n_grid,
            cG=chooser_cfg.cG,
            par_fraction=chooser_cfg.par_fraction,
            ckpt_fraction=chooser_cfg.ckpt_fraction,
            seed=seed,
            device=chooser_cfg.device,
        )

        val_device = next(unary_fit["model_pos"].parameters()).device
        val_metrics = score_unary_operating_point(
            X_eval=torch.as_tensor(X_val_upd, dtype=torch.float32, device=val_device),
            y_eval=torch.as_tensor(y_val, dtype=torch.int64, device=val_device),
            model_pos=unary_fit["model_pos"],
            model_neg=unary_fit["model_neg"],
            beta_pos=float(unary_fit["beta_pos"]),
            beta_neg=float(unary_fit["beta_neg"]),
            cG=float(unary_fit["cG"]),
        )

        row = {
            "removed_idx": int(local_idx),
            "beta_pos": float(unary_fit["beta_pos"]),
            "beta_neg": float(unary_fit["beta_neg"]),
            "cG": float(unary_fit["cG"]),
            "best_epoch": int(unary_fit["best_epoch"]),
            "S_par": float(unary_fit["best_S_par"]),
            "F12_par": float(unary_fit["best_F12_par"]),
            "G12_par": float(unary_fit["best_G12_par"]),
            "S_ckpt": float(unary_fit["best_S_ckpt"]),
            "F12_ckpt": float(unary_fit["best_F12_ckpt"]),
            "G12_ckpt": float(unary_fit["best_G12_ckpt"]),
            "S": float(val_metrics["S"]),
            "F12": float(val_metrics["F12"]),
            "G12": float(val_metrics["G12"]),
            "a1": float(val_metrics["a1"]),
            "a2": float(val_metrics["a2"]),
            "b1": float(val_metrics["b1"]),
            "b2": float(val_metrics["b2"]),
            "coverage": float(val_metrics["coverage"]),
            "conflict_rate": float(val_metrics["conflict_rate"]),
            "reject_rate": float(val_metrics["reject_rate"]),
            "wrong_exclusive_rate": float(val_metrics["wrong_exclusive_rate"]),
            "correct_exclusive_rate": float(val_metrics["correct_exclusive_rate"]),
            "selective_accuracy": float(val_metrics["selective_accuracy"])
            if not np.isnan(val_metrics["selective_accuracy"])
            else np.nan,
            "selective_f1": float(val_metrics["selective_f1"])
            if not np.isnan(val_metrics["selective_f1"])
            else np.nan,
            "conservative_accuracy": float(val_metrics["conservative_accuracy"])
            if not np.isnan(val_metrics["conservative_accuracy"])
            else np.nan,
        }
        rows.append(row)

        if row["S"] > best_score:
            best_score = row["S"]
            best_idx = local_idx

        if output:
            print(
                f"[MLP_unar] remove={local_idx} "
                f"S={row['S']:.6f} F12={row['F12']:.6f} G12={row['G12']:.6f} "
                f"epoch={row['best_epoch']} beta_pos={row['beta_pos']:.6f} beta_neg={row['beta_neg']:.6f}"
            )

    df = pd.DataFrame(rows).sort_values(["S", "F12", "G12"], ascending=[False, False, True]).reset_index(drop=True)
    return int(best_idx), df


class MLPReducer(core.ReducerBase):
    def __init__(
        self,
        d_hidden,
        n_hidden_layers,
        num_epochs,
        batch_size,
        n_select,
        seed=0,
        output=False,
        device: str = "cpu",
        checkpoint_every: int = 5,
        checkpoint_n_grid: int = 19,
        cG: float = 0.25,
        par_fraction: float = 0.20,
        ckpt_fraction: float = 0.20,
        lr: float = 3e-3,
        weight_decay: float = 1e-2,
    ):
        self.d_hidden = d_hidden
        self.n_hidden_layers = n_hidden_layers
        self.num_epochs = num_epochs
        self.batch_size = batch_size
        self.seed = seed
        self.output = output
        self.n_select = n_select
        self.selected_indices_ = None
        self.removed_indices_ = None
        self.history_ = []
        self.device = torch.device(device)
        self.chooser_cfg = UnaryChooserConfig(
            checkpoint_every=checkpoint_every,
            checkpoint_n_grid=checkpoint_n_grid,
            cG=cG,
            par_fraction=par_fraction,
            ckpt_fraction=ckpt_fraction,
            lr=lr,
            weight_decay=weight_decay,
            device=str(self.device),
        )

    def _to_torch2d(self, X):
        if isinstance(X, torch.Tensor):
            if X.ndim != 2:
                raise ValueError("X must be 2D")
            return X.to(device=self.device, dtype=torch.float32)
        X = np.asarray(X)
        if X.ndim != 2:
            raise ValueError("X must be 2D")
        return torch.from_numpy(X).to(device=self.device, dtype=torch.float32)

    def _to_torch1d_int(self, y):
        if isinstance(y, torch.Tensor):
            if y.ndim != 1:
                raise ValueError("y must be 1D")
            return y.to(device=self.device, dtype=torch.int64)
        y = np.asarray(y).ravel()
        return torch.from_numpy(y.astype(np.int64)).to(device=self.device)

    def fit(self, X, y):
        X_t = self._to_torch2d(X)
        y_t = self._to_torch1d_int(y)
        keep = list(range(X_t.shape[1]))
        removed = []

        while len(keep) > self.n_select:
            X_cur = X_t[:, keep]
            best_idx, df = choose_coord_criterion_aligned(
                X=X_cur,
                y=y_t,
                d_hidden=self.d_hidden,
                n_hidden_layers=self.n_hidden_layers,
                num_epochs=self.num_epochs,
                batch_size=self.batch_size,
                seed=self.seed,
                output=self.output,
                chooser_cfg=self.chooser_cfg,
            )

            removed_global = keep[best_idx]
            removed.append(removed_global)
            del keep[best_idx]

            self.history_.append(
                {
                    "removed_global": removed_global,
                    "n_left": len(keep),
                    "criterion": "strict_unary",
                    "df": df,
                }
            )

        self.selected_indices_ = keep
        self.removed_indices_ = removed
        return self

    def transform(self, X):
        if self.selected_indices_ is None:
            raise AssertionError("Call fit() before transform().")
        if isinstance(X, torch.Tensor):
            return X[:, self.selected_indices_]
        X = np.asarray(X)
        return X[:, self.selected_indices_]

    def fit_transform(self, X, y):
        return self.fit(X, y).transform(X)

    def get_support(self, indices=False):
        if self.selected_indices_ is None:
            raise AssertionError("Call fit() first.")
        if indices:
            return np.array(self.selected_indices_, dtype=int)

        mask = np.zeros(max(self.selected_indices_) + 1 if self.selected_indices_ else self.n_select, dtype=bool)
        if self.selected_indices_:
            mask[self.selected_indices_] = True
        return mask


__all__ = [
    "EnsembleReducer",
    "HSICSelector",
    "MLPReducer",
    "PCAReducer",
    "PLSReducer",
    "ReducerBase",
    "UMAPReducer",
    "choose_coord_criterion_aligned",
]
