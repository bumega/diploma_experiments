from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import torch
import torch.nn as nn

from gpu_model_aligned_rase import (
    GPU_CRITERIA_MAIN,
    GPU_CRITERION_SHADOW,
    GpuModelAlignedRaSEReducer,
    GpuRaseConfig,
    _make_cv_folds,
)
from nirs_core import _expand_anchor_set, _project_box_simplex, dynamic_bounds


class FixedRbfDecisionModel(nn.Module):
    def __init__(self, centers: torch.Tensor, coef: torch.Tensor, gamma: float):
        super().__init__()
        self.register_buffer("centers", centers.detach().clone())
        self.register_buffer("coef", coef.detach().clone())
        self.gamma = float(gamma)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        K = torch.exp(-self.gamma * torch.cdist(x, self.centers).pow(2))
        return (K @ self.coef).unsqueeze(-1)


def _fit_fixed_rbf_model(
    X: torch.Tensor,
    y: torch.Tensor,
    *,
    C: float = 1.0,
    gamma: float = 0.1,
) -> FixedRbfDecisionModel:
    y_signed = y.to(dtype=X.dtype) * 2.0 - 1.0
    K = torch.exp(-float(gamma) * torch.cdist(X, X).pow(2))
    eye = torch.eye(K.shape[0], device=X.device, dtype=X.dtype)
    coef = torch.linalg.solve(K + (1.0 / float(C)) * eye, y_signed.unsqueeze(-1)).squeeze(-1)
    return FixedRbfDecisionModel(X, coef, gamma=gamma).to(X.device).eval()


@dataclass
class ShapAggregationConfig:
    background_size: int = 32
    eval_size: int = 48
    final_rbf_C: float = 1.0
    final_rbf_gamma: float = 0.1
    shap_fallback_to_gradient: bool = True


class GpuShapModelAlignedRaSEReducer(GpuModelAlignedRaSEReducer):
    """Model-aligned RaSE that keeps SHAP eta/probability update from EnsembleReducer."""

    def __init__(
        self,
        criterion: str,
        config: GpuRaseConfig | None = None,
        shap_config: ShapAggregationConfig | None = None,
    ):
        super().__init__(criterion=criterion, config=config)
        self.shap_config = shap_config or ShapAggregationConfig()
        self.coord_prob_: np.ndarray | None = None
        self.eta_history_: list[np.ndarray] = []
        self.eta_final_: np.ndarray | None = None
        self.anchor_set_: np.ndarray = np.array([], dtype=int)
        self.iteration_history_: list[dict[str, Any]] = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GpuShapModelAlignedRaSEReducer":
        cfg = self.config
        device = torch.device(cfg.device)
        X_t = torch.as_tensor(np.asarray(X, dtype=np.float32), device=device)
        y_np = (np.asarray(y).ravel() > 0).astype(np.int64)
        y_t = torch.as_tensor(y_np, device=device, dtype=torch.long)
        n_features = int(X_t.shape[1])
        rng = np.random.default_rng(cfg.seed)
        folds_np = _make_cv_folds(y_np, cfg.inner_cv_splits, cfg.seed)
        folds = [
            (
                torch.as_tensor(tr, device=device, dtype=torch.long),
                torch.as_tensor(val, device=device, dtype=torch.long),
            )
            for tr, val in folds_np
        ]

        corr = self._safe_corrcoef_gpu(X_t)
        y_float = y_t.to(dtype=torch.float32)
        y_center = y_float - y_float.mean()
        X_center = X_t - X_t.mean(dim=0)
        rel_mrmr = (X_center * y_center[:, None]).sum(dim=0).abs()
        rel_mrmr = rel_mrmr / torch.clamp(torch.linalg.norm(X_center, dim=0) * torch.linalg.norm(y_center), min=1e-12)
        rel_hsic = self._hsic_relevance_gpu(X_t, y_t) if self.criterion == "hsic_redundancy" else rel_mrmr

        self.coord_prob_ = np.full(n_features, 1.0 / n_features, dtype=np.float64)
        self.anchor_set_ = np.array([], dtype=int)
        self.history_ = []
        self.iteration_history_ = []
        self.eta_history_ = []
        self.eta_final_ = None

        max_size = int(min(cfg.max_subspace_size, n_features))
        for step in range(cfg.num_iterations):
            selected_models: list[dict[str, Any]] = []

            for local_model_id in range(cfg.models_per_iteration):
                best = self._select_best_subspace(
                    X_t=X_t,
                    y_t=y_t,
                    rng=rng,
                    sample_prob=self.coord_prob_,
                    max_size=max_size,
                    folds=folds,
                    corr=corr,
                    rel_mrmr=rel_mrmr,
                    rel_hsic=rel_hsic,
                )
                selected_models.append(best)
                self.history_.append(
                    {
                        "iteration": step + 1,
                        "model": local_model_id + 1,
                        "criterion": self.criterion,
                        "score": float(best["score"]),
                        "subspace_size": int(len(best["indices"])),
                        "selected_in_block": best["indices"].astype(int).tolist(),
                        **best["detail"],
                    }
                )
                if cfg.output and ((local_model_id + 1) % 10 == 0 or local_model_id == 0):
                    print(
                        f"[gpu-shap-rase:{self.criterion}] iter={step+1}/{cfg.num_iterations} "
                        f"model={local_model_id+1}/{cfg.models_per_iteration} "
                        f"score={best['score']:.6f} d={len(best['indices'])}",
                        flush=True,
                    )

            eta_step = self._compute_shap_eta(X_t, y_t, selected_models, n_features, rng)
            self.eta_history_.append(eta_step.copy())

            progress = 0.0 if cfg.num_iterations <= 1 else step / float(cfg.num_iterations - 1)
            gamma = 0.80 + (0.60 - 0.80) * progress
            coord_raw = (1.0 - gamma) * self.coord_prob_ + gamma * eta_step
            coord_raw = coord_raw / max(coord_raw.sum(), 1e-12)

            anchor_prev = self.anchor_set_.copy()
            lb, ub, caps_info = dynamic_bounds(
                n=n_features,
                q=cfg.n_select,
                A_idx=anchor_prev,
                step=step,
                n_steps=cfg.num_iterations,
            )
            self.coord_prob_ = _project_box_simplex(coord_raw, lb=lb, ub=ub)
            self.anchor_set_ = _expand_anchor_set(
                scores=eta_step,
                A_prev=anchor_prev,
                q=cfg.n_select,
                step=step,
                n_steps=cfg.num_iterations,
            )
            self.iteration_history_.append(
                {
                    "iteration": step + 1,
                    "gamma": float(gamma),
                    "anchor_prev": anchor_prev.astype(int).tolist(),
                    "anchor_next": self.anchor_set_.astype(int).tolist(),
                    **{k: float(v) if isinstance(v, (int, float, np.floating)) else v for k, v in caps_info.items()},
                }
            )
            if cfg.output:
                print(
                    f"[gpu-shap-rase:{self.criterion}] finished iter={step+1}/{cfg.num_iterations} "
                    f"|A|={len(self.anchor_set_)} eta_top={np.argsort(eta_step)[-5:].tolist()}",
                    flush=True,
                )

        tail = min(3, len(self.eta_history_))
        self.eta_final_ = np.mean(self.eta_history_[-tail:], axis=0)
        self.eta_final_ = self.eta_final_ / max(float(self.eta_final_.sum()), 1e-12)
        self.feature_importances_ = self.eta_final_.copy()
        self.selected_indices_ = np.sort(np.argsort(self.eta_final_)[-cfg.n_select:]).astype(int)
        return self

    def _select_best_subspace(
        self,
        *,
        X_t: torch.Tensor,
        y_t: torch.Tensor,
        rng: np.random.Generator,
        sample_prob: np.ndarray,
        max_size: int,
        folds: list[tuple[torch.Tensor, torch.Tensor]],
        corr: torch.Tensor,
        rel_mrmr: torch.Tensor,
        rel_hsic: torch.Tensor,
    ) -> dict[str, Any]:
        cfg = self.config
        best_score = -float("inf")
        best_idx: np.ndarray | None = None
        best_detail: dict[str, Any] = {}
        remaining = cfg.num_attempts
        while remaining > 0:
            bsz = min(cfg.candidate_batch_size, remaining)
            cand, mask = self._draw_candidates(rng, sample_prob, bsz, max_size)
            cand_t = torch.as_tensor(cand, device=X_t.device, dtype=torch.long)
            mask_t = torch.as_tensor(mask, device=X_t.device, dtype=torch.float32)
            scores, details = self._score_batch(X_t, y_t, cand_t, mask_t, folds, corr, rel_mrmr, rel_hsic)
            loc = int(torch.argmax(scores).detach().cpu().item())
            score = float(scores[loc].detach().cpu().item())
            if score > best_score:
                valid = mask[loc].astype(bool)
                best_score = score
                best_idx = cand[loc, valid].copy()
                best_detail = {k: float(v[loc].detach().cpu().item()) for k, v in details.items()}
            remaining -= bsz
        assert best_idx is not None
        return {"score": best_score, "indices": best_idx, "detail": best_detail}

    def _compute_shap_eta(
        self,
        X_t: torch.Tensor,
        y_t: torch.Tensor,
        selected_models: list[dict[str, Any]],
        n_features: int,
        rng: np.random.Generator,
    ) -> np.ndarray:
        shap_cfg = self.shap_config
        coord_sum = np.zeros(n_features, dtype=np.float64)
        n_blocks = 0
        n = X_t.shape[0]
        bg_size = min(shap_cfg.background_size, n)
        ev_size = min(shap_cfg.eval_size, n)
        bg_idx = torch.as_tensor(rng.choice(n, size=bg_size, replace=False), device=X_t.device, dtype=torch.long)
        ev_idx = torch.as_tensor(rng.choice(n, size=ev_size, replace=False), device=X_t.device, dtype=torch.long)

        for item in selected_models:
            idx_np = np.asarray(item["indices"], dtype=int)
            idx_t = torch.as_tensor(idx_np, device=X_t.device, dtype=torch.long)
            X_sub = X_t[:, idx_t]
            model = _fit_fixed_rbf_model(
                X_sub,
                y_t,
                C=shap_cfg.final_rbf_C,
                gamma=shap_cfg.final_rbf_gamma,
            )
            X_bg = X_sub[bg_idx]
            X_ev = X_sub[ev_idx].detach().clone().requires_grad_(True)
            vals = self._gradient_shap_values(model, X_bg, X_ev)
            vals_np = np.asarray(vals.detach().cpu().numpy(), dtype=np.float64).reshape(-1)
            vals_np = np.maximum(vals_np, 0.0)
            if not np.any(np.isfinite(vals_np)) or vals_np.sum() <= 0:
                vals_np = np.ones_like(vals_np, dtype=np.float64)
            phi = vals_np / vals_np.sum()
            u_sub = np.minimum(1.0, len(idx_np) * phi)
            coord_sum[idx_np] += u_sub
            n_blocks += 1

        if n_blocks == 0:
            return np.full(n_features, 1.0 / n_features, dtype=np.float64)
        eta = coord_sum / float(n_blocks)
        eta_sum = float(eta.sum())
        if not np.isfinite(eta_sum) or eta_sum <= 0:
            eta[:] = 1.0 / n_features
        else:
            eta /= eta_sum
        return eta

    @staticmethod
    def _gradient_shap_values(model: nn.Module, X_bg: torch.Tensor, X_ev: torch.Tensor) -> torch.Tensor:
        # Gradient SHAP-style attribution: average gradients along random baselines-to-input paths.
        samples = 8
        acc = torch.zeros(X_ev.shape[1], device=X_ev.device, dtype=X_ev.dtype)
        for _ in range(samples):
            base = X_bg[torch.randint(0, X_bg.shape[0], (X_ev.shape[0],), device=X_ev.device)]
            alpha = torch.rand(X_ev.shape[0], 1, device=X_ev.device, dtype=X_ev.dtype)
            z = (base + alpha * (X_ev - base)).detach().clone().requires_grad_(True)
            out = model(z).sum()
            grad = torch.autograd.grad(out, z, retain_graph=False, create_graph=False)[0]
            acc += ((X_ev - base) * grad).abs().mean(dim=0)
        return acc / float(samples)

    @staticmethod
    def _safe_corrcoef_gpu(X: torch.Tensor) -> torch.Tensor:
        Xc = X - X.mean(dim=0, keepdim=True)
        denom = torch.linalg.norm(Xc, dim=0, keepdim=True).T @ torch.linalg.norm(Xc, dim=0, keepdim=True)
        corr = Xc.T @ Xc / torch.clamp(denom, min=1e-12)
        corr = torch.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0).abs()
        corr.fill_diagonal_(0.0)
        return corr

    @staticmethod
    def _hsic_relevance_gpu(X: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
        n, p = X.shape
        L = (y[:, None] == y[None, :]).to(dtype=X.dtype)
        L = L - L.mean(dim=0, keepdim=True) - L.mean(dim=1, keepdim=True) + L.mean()
        vals = []
        for j in range(p):
            x = X[:, j]
            d = torch.pdist(x.reshape(-1, 1))
            d = d[d > 1e-12]
            gamma = 1.0 if d.numel() == 0 else 1.0 / torch.clamp(2.0 * torch.median(d).pow(2), min=1e-12)
            K = torch.exp(-gamma * (x[:, None] - x[None, :]).pow(2))
            K = K - K.mean(dim=0, keepdim=True) - K.mean(dim=1, keepdim=True) + K.mean()
            vals.append(torch.clamp((K * L).sum() / float((n - 1) ** 2), min=0.0))
        return torch.stack(vals)

    def transform(self, X: np.ndarray) -> np.ndarray:
        if self.selected_indices_ is None:
            raise AssertionError("Call fit first")
        return np.asarray(X)[:, self.selected_indices_]

    def fit_transform(self, X: np.ndarray, y: np.ndarray) -> np.ndarray:
        return self.fit(X, y).transform(X)


__all__ = [
    "GPU_CRITERIA_MAIN",
    "GPU_CRITERION_SHADOW",
    "GpuShapModelAlignedRaSEReducer",
    "ShapAggregationConfig",
]
