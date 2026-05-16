from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import torch


GPU_CRITERIA_MAIN = [
    "cv_mcc_svm_rbf",
    "cv_auc_svm_rbf",
    "hsic_redundancy",
    "mrmr_redundancy",
    "bic_logreg",
]
GPU_CRITERION_SHADOW = "shadow_cv_mcc"


@dataclass
class GpuRaseConfig:
    n_select: int = 20
    num_iterations: int = 4
    models_per_iteration: int = 100
    num_attempts: int = 5000
    max_subspace_size: int = 20
    inner_cv_splits: int = 5
    candidate_batch_size: int = 256
    lambda_std: float = 0.5
    mu_red: float = 0.05
    shadow_repeats: int = 5
    ridge_C_grid: tuple[float, ...] = (0.1, 1.0, 10.0)
    gamma_grid: tuple[float, ...] = (0.01, 0.1, 1.0)
    seed: int = 42
    device: str = "cuda"
    output: bool = False

    @property
    def total_models(self) -> int:
        return int(self.num_iterations * self.models_per_iteration)


def _torch_auc(y01: torch.Tensor, score: torch.Tensor) -> torch.Tensor:
    y = y01.to(dtype=torch.bool)
    pos = score[y]
    neg = score[~y]
    if pos.numel() == 0 or neg.numel() == 0:
        return score.new_tensor(0.5)
    cmp = (pos[:, None] > neg[None, :]).to(score.dtype)
    ties = (pos[:, None] == neg[None, :]).to(score.dtype) * 0.5
    return (cmp + ties).mean()


def _torch_mcc(y01: torch.Tensor, pred01: torch.Tensor) -> torch.Tensor:
    y = y01.to(dtype=torch.bool)
    p = pred01.to(dtype=torch.bool)
    tp = (p & y).sum(dtype=torch.float32)
    tn = (~p & ~y).sum(dtype=torch.float32)
    fp = (p & ~y).sum(dtype=torch.float32)
    fn = (~p & y).sum(dtype=torch.float32)
    denom = torch.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return torch.where(denom > 0, (tp * tn - fp * fn) / denom, torch.zeros_like(denom))


def _safe_corrcoef(X: torch.Tensor) -> torch.Tensor:
    Xc = X - X.mean(dim=0, keepdim=True)
    denom = torch.linalg.norm(Xc, dim=0, keepdim=True).T @ torch.linalg.norm(Xc, dim=0, keepdim=True)
    corr = Xc.T @ Xc / torch.clamp(denom, min=1e-12)
    corr = torch.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0).abs()
    corr.fill_diagonal_(0.0)
    return corr


def _median_gamma_1d(x: torch.Tensor) -> torch.Tensor:
    d = torch.pdist(x.reshape(-1, 1))
    d = d[d > 1e-12]
    if d.numel() == 0:
        return x.new_tensor(1.0)
    sigma = torch.median(d)
    return 1.0 / torch.clamp(2.0 * sigma * sigma, min=1e-12)


def _center_kernel(K: torch.Tensor) -> torch.Tensor:
    return K - K.mean(dim=0, keepdim=True) - K.mean(dim=1, keepdim=True) + K.mean()


def _hsic_relevance(X: torch.Tensor, y01: torch.Tensor) -> torch.Tensor:
    n, p = X.shape
    L = (y01[:, None] == y01[None, :]).to(dtype=X.dtype)
    Lc = _center_kernel(L)
    out = []
    for j in range(p):
        x = X[:, j]
        gamma = _median_gamma_1d(x)
        K = torch.exp(-gamma * (x[:, None] - x[None, :]).pow(2))
        out.append((_center_kernel(K) * Lc).sum() / float((n - 1) ** 2))
    return torch.clamp(torch.stack(out), min=0.0)


def _make_cv_folds(y01: np.ndarray, n_splits: int, seed: int) -> list[tuple[np.ndarray, np.ndarray]]:
    from sklearn.model_selection import StratifiedKFold

    _, counts = np.unique(y01, return_counts=True)
    n = min(int(n_splits), int(counts.min()))
    splitter = StratifiedKFold(n_splits=n, shuffle=True, random_state=seed)
    return list(splitter.split(np.zeros((len(y01), 1)), y01))


class GpuModelAlignedRaSEReducer:
    def __init__(self, criterion: str, config: GpuRaseConfig | None = None):
        allowed = set(GPU_CRITERIA_MAIN + [GPU_CRITERION_SHADOW])
        if criterion not in allowed:
            raise ValueError(f"Unknown criterion={criterion!r}")
        self.criterion = criterion
        self.config = config or GpuRaseConfig()
        self.selected_indices_: np.ndarray | None = None
        self.feature_importances_: np.ndarray | None = None
        self.history_: list[dict[str, Any]] = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GpuModelAlignedRaSEReducer":
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

        corr = _safe_corrcoef(X_t)
        y_float = y_t.to(dtype=torch.float32)
        y_center = y_float - y_float.mean()
        rel_mrmr = ((X_t - X_t.mean(dim=0)) * y_center[:, None]).sum(dim=0).abs()
        rel_mrmr = rel_mrmr / torch.clamp(torch.linalg.norm(X_t - X_t.mean(dim=0), dim=0) * torch.linalg.norm(y_center), min=1e-12)
        rel_hsic = _hsic_relevance(X_t, y_t) if self.criterion == "hsic_redundancy" else rel_mrmr

        counts = torch.zeros(n_features, device=device)
        score_sum = torch.zeros(n_features, device=device)
        prob = np.full(n_features, 1.0 / n_features, dtype=np.float64)
        max_size = int(min(cfg.max_subspace_size, n_features))
        total_models = cfg.total_models
        self.history_ = []

        for model_id in range(1, total_models + 1):
            best_score = -float("inf")
            best_idx: np.ndarray | None = None
            best_detail: dict[str, Any] = {}

            remaining = cfg.num_attempts
            while remaining > 0:
                bsz = min(cfg.candidate_batch_size, remaining)
                cand, mask = self._draw_candidates(rng, prob, bsz, max_size)
                cand_t = torch.as_tensor(cand, device=device, dtype=torch.long)
                mask_t = torch.as_tensor(mask, device=device, dtype=torch.float32)

                scores, details = self._score_batch(X_t, y_t, cand_t, mask_t, folds, corr, rel_mrmr, rel_hsic)
                loc = int(torch.argmax(scores).detach().cpu().item())
                cur = float(scores[loc].detach().cpu().item())
                if cur > best_score:
                    best_score = cur
                    best_idx = cand[loc, mask[loc].astype(bool)].copy()
                    best_detail = {k: float(v[loc].detach().cpu().item()) for k, v in details.items()}
                remaining -= bsz

            assert best_idx is not None
            best_t = torch.as_tensor(best_idx, device=device, dtype=torch.long)
            counts[best_t] += 1.0
            score_sum[best_t] += float(best_score)
            empirical = (counts + 1e-6)
            empirical = empirical / empirical.sum()
            prob = empirical.detach().cpu().numpy()
            prob = 0.25 / n_features + 0.75 * prob
            prob = prob / prob.sum()

            row = {
                "model": model_id,
                "iteration": int((model_id - 1) // cfg.models_per_iteration + 1),
                "score": best_score,
                "subspace_size": int(best_idx.size),
                "selected_in_block": best_idx.astype(int).tolist(),
                **best_detail,
            }
            self.history_.append(row)
            if cfg.output and (model_id == 1 or model_id % 10 == 0):
                print(f"[gpu-rase:{self.criterion}] model={model_id}/{total_models} score={best_score:.6f} d={best_idx.size}", flush=True)

        importance = counts + 1e-6 * score_sum
        selected = torch.argsort(importance)[-cfg.n_select:].sort().values
        self.selected_indices_ = selected.detach().cpu().numpy().astype(int)
        self.feature_importances_ = importance.detach().cpu().numpy()
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        if self.selected_indices_ is None:
            raise AssertionError("Call fit first")
        return np.asarray(X)[:, self.selected_indices_]

    def fit_transform(self, X: np.ndarray, y: np.ndarray) -> np.ndarray:
        return self.fit(X, y).transform(X)

    def _draw_candidates(
        self,
        rng: np.random.Generator,
        prob: np.ndarray,
        batch_size: int,
        max_size: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        sizes = rng.integers(1, max_size + 1, size=batch_size)
        cand = np.zeros((batch_size, max_size), dtype=np.int64)
        mask = np.zeros((batch_size, max_size), dtype=np.float32)
        for i, size in enumerate(sizes):
            idx = rng.choice(prob.size, size=int(size), replace=False, p=prob)
            cand[i, : int(size)] = np.sort(idx)
            mask[i, : int(size)] = 1.0
        return cand, mask

    def _redundancy(self, corr: torch.Tensor, cand: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        sub = corr[cand[:, :, None], cand[:, None, :]]
        pair_mask = mask[:, :, None] * mask[:, None, :]
        upper = torch.triu(torch.ones_like(pair_mask), diagonal=1)
        valid = pair_mask * upper
        denom = valid.sum(dim=(1, 2)).clamp_min(1.0)
        return (sub * valid).sum(dim=(1, 2)) / denom

    def _score_batch(
        self,
        X: torch.Tensor,
        y: torch.Tensor,
        cand: torch.Tensor,
        mask: torch.Tensor,
        folds: list[tuple[torch.Tensor, torch.Tensor]],
        corr: torch.Tensor,
        rel_mrmr: torch.Tensor,
        rel_hsic: torch.Tensor,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        cfg = self.config
        red = self._redundancy(corr, cand, mask)
        size = mask.sum(dim=1).clamp_min(1.0)

        if self.criterion == "mrmr_redundancy":
            rel = (rel_mrmr[cand] * mask).sum(dim=1) / size
            return rel - cfg.mu_red * red, {"relevance": rel, "redundancy": red}
        if self.criterion == "hsic_redundancy":
            rel = (rel_hsic[cand] * mask).sum(dim=1) / size
            return rel - cfg.mu_red * red, {"relevance": rel, "redundancy": red}
        if self.criterion == "bic_logreg":
            score = self._score_bic_batch(X, y, cand, mask, red)
            return score, {"redundancy": red}
        if self.criterion == "cv_mcc_svm_rbf":
            mean, std = self._score_rbf_cv_batch(X, y, cand, mask, folds, metric="mcc")
            return mean - cfg.lambda_std * std - cfg.mu_red * red, {"cv_mean": mean, "cv_std": std, "redundancy": red}
        if self.criterion == "cv_auc_svm_rbf":
            mean, std = self._score_rbf_cv_batch(X, y, cand, mask, folds, metric="auc")
            return mean - cfg.lambda_std * std - cfg.mu_red * red, {"cv_mean": mean, "cv_std": std, "redundancy": red}
        if self.criterion == GPU_CRITERION_SHADOW:
            mean, std = self._score_rbf_cv_batch(X, y, cand, mask, folds, metric="mcc")
            base = mean - cfg.lambda_std * std - cfg.mu_red * red
            shadows = []
            for rep in range(cfg.shadow_repeats):
                Xs = X.clone()
                perm = torch.randperm(X.shape[0], device=X.device)
                uniq = torch.unique(cand[mask.to(dtype=torch.bool)])
                Xs[:, uniq] = Xs[perm][:, uniq]
                sm, ss = self._score_rbf_cv_batch(Xs, y, cand, mask, folds, metric="mcc")
                shadows.append(sm - cfg.lambda_std * ss - cfg.mu_red * red)
            q95 = torch.quantile(torch.stack(shadows), 0.95, dim=0)
            return base - q95, {"base_score": base, "shadow_q95": q95}
        raise AssertionError(self.criterion)

    def _gather(self, X: torch.Tensor, cand: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        out = X[:, cand].permute(1, 0, 2).contiguous()
        return out * mask[:, None, :]

    def _score_rbf_cv_batch(
        self,
        X: torch.Tensor,
        y: torch.Tensor,
        cand: torch.Tensor,
        mask: torch.Tensor,
        folds: list[tuple[torch.Tensor, torch.Tensor]],
        *,
        metric: str,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        cfg = self.config
        B = cand.shape[0]
        Xc = self._gather(X, cand, mask)
        vals_by_param = []
        y_signed = y.to(dtype=torch.float32) * 2.0 - 1.0

        for gamma in cfg.gamma_grid:
            gamma_f = float(gamma)
            for C in cfg.ridge_C_grid:
                alpha = 1.0 / float(C)
                fold_vals = []
                for tr, val in folds:
                    Xtr = Xc[:, tr, :]
                    Xval = Xc[:, val, :]
                    ytr = y_signed[tr].expand(B, -1)
                    dist = torch.cdist(Xtr, Xtr).pow(2)
                    K = torch.exp(-gamma_f * dist)
                    eye = torch.eye(K.shape[-1], device=X.device, dtype=X.dtype).expand(B, -1, -1)
                    coef = torch.linalg.solve(K + alpha * eye, ytr.unsqueeze(-1)).squeeze(-1)
                    Kval = torch.exp(-gamma_f * torch.cdist(Xval, Xtr).pow(2))
                    score = torch.bmm(Kval, coef.unsqueeze(-1)).squeeze(-1)
                    if metric == "mcc":
                        pred = (score > 0).to(dtype=torch.long)
                        fold_vals.append(torch.stack([_torch_mcc(y[val], pred[b]) for b in range(B)]))
                    else:
                        fold_vals.append(torch.stack([_torch_auc(y[val], score[b]) for b in range(B)]))
                fold_stack = torch.stack(fold_vals)
                vals_by_param.append((fold_stack.mean(dim=0), fold_stack.std(dim=0, unbiased=True)))

        means = torch.stack([x[0] for x in vals_by_param])
        stds = torch.stack([x[1] for x in vals_by_param])
        objective = means - cfg.lambda_std * stds
        loc = torch.argmax(objective, dim=0)
        return means[loc, torch.arange(B, device=X.device)], stds[loc, torch.arange(B, device=X.device)]

    def _score_bic_batch(self, X: torch.Tensor, y: torch.Tensor, cand: torch.Tensor, mask: torch.Tensor, red: torch.Tensor) -> torch.Tensor:
        cfg = self.config
        Xc = self._gather(X, cand, mask)
        B, n, d = Xc.shape
        ones = torch.ones(B, n, 1, device=X.device, dtype=X.dtype)
        Xa = torch.cat([ones, Xc], dim=2)
        y_signed = (y.to(dtype=X.dtype) * 2.0 - 1.0).expand(B, -1)
        XtX = torch.bmm(Xa.transpose(1, 2), Xa)
        eye = torch.eye(d + 1, device=X.device, dtype=X.dtype).expand(B, -1, -1)
        beta = torch.linalg.solve(XtX + 1.0 * eye, torch.bmm(Xa.transpose(1, 2), y_signed.unsqueeze(-1))).squeeze(-1)
        logits = torch.bmm(Xa, beta.unsqueeze(-1)).squeeze(-1)
        prob = torch.sigmoid(logits).clamp(1e-6, 1.0 - 1e-6)
        y_float = y.to(dtype=X.dtype).expand(B, -1)
        nll = -(y_float * prob.log() + (1.0 - y_float) * (1.0 - prob).log()).sum(dim=1)
        k = mask.sum(dim=1) + 1.0
        p = float(X.shape[1])
        log_comb = torch.lgamma(torch.tensor(p + 1.0, device=X.device)) - torch.lgamma(k + 1.0) - torch.lgamma(torch.tensor(p, device=X.device) - k + 1.0)
        ebic = 2.0 * nll + k * math.log(float(n)) + 2.0 * 0.5 * log_comb
        return -ebic - cfg.mu_red * red


class GpuRbfKernelClassifier:
    def __init__(self, C: float = 1.0, gamma: float = 0.1, device: str = "cuda"):
        self.C = float(C)
        self.gamma = float(gamma)
        self.device = torch.device(device)
        self.X_train_: torch.Tensor | None = None
        self.coef_: torch.Tensor | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GpuRbfKernelClassifier":
        X_t = torch.as_tensor(np.asarray(X, dtype=np.float32), device=self.device)
        y_t = torch.as_tensor((np.asarray(y).ravel() > 0).astype(np.float32) * 2.0 - 1.0, device=self.device)
        K = torch.exp(-self.gamma * torch.cdist(X_t, X_t).pow(2))
        eye = torch.eye(K.shape[0], device=self.device)
        self.coef_ = torch.linalg.solve(K + (1.0 / self.C) * eye, y_t.unsqueeze(-1)).squeeze(-1)
        self.X_train_ = X_t
        return self

    def decision_function(self, X: np.ndarray) -> np.ndarray:
        if self.X_train_ is None or self.coef_ is None:
            raise AssertionError("Call fit first")
        X_t = torch.as_tensor(np.asarray(X, dtype=np.float32), device=self.device)
        K = torch.exp(-self.gamma * torch.cdist(X_t, self.X_train_).pow(2))
        return (K @ self.coef_).detach().cpu().numpy()

    def predict(self, X: np.ndarray) -> np.ndarray:
        return (self.decision_function(X) > 0).astype(int)
