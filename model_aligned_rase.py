from __future__ import annotations

import math
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, matthews_corrcoef, roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.svm import SVC


CRITERIA_MAIN = [
    "cv_mcc_svm_rbf",
    "cv_auc_svm_rbf",
    "hsic_redundancy",
    "mrmr_redundancy",
    "bic_logreg",
]
CRITERION_SHADOW = "shadow_cv_mcc"


@dataclass
class CriterionResult:
    score: float
    detail: dict[str, Any] = field(default_factory=dict)


def _as_y01(y: np.ndarray) -> np.ndarray:
    y_arr = np.asarray(y).ravel()
    return (y_arr > 0).astype(int)


def _safe_std(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return float(np.std(np.asarray(values, dtype=float), ddof=1))


def _safe_auc(y_true: np.ndarray, score: np.ndarray) -> float:
    if np.unique(y_true).size < 2:
        return 0.5
    return float(roc_auc_score(y_true, score))


def _median_bandwidth_1d(x: np.ndarray) -> float:
    x = np.asarray(x, dtype=float).ravel()
    if x.size <= 1:
        return 1.0
    diffs = np.abs(x[:, None] - x[None, :])
    vals = diffs[np.triu_indices_from(diffs, k=1)]
    vals = vals[np.isfinite(vals) & (vals > 0)]
    if vals.size == 0:
        return 1.0
    return float(np.median(vals))


def _center_kernel(K: np.ndarray) -> np.ndarray:
    return K - K.mean(axis=0, keepdims=True) - K.mean(axis=1, keepdims=True) + K.mean()


def _rbf_kernel_1d(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float).ravel()
    sigma = _median_bandwidth_1d(x)
    denom = 2.0 * sigma * sigma
    if not np.isfinite(denom) or denom <= 0:
        denom = 2.0
    dist2 = (x[:, None] - x[None, :]) ** 2
    return np.exp(-dist2 / denom)


def _hsic_from_kernels(K: np.ndarray, L: np.ndarray) -> float:
    n = int(K.shape[0])
    if n <= 1:
        return 0.0
    Kc = _center_kernel(K)
    Lc = _center_kernel(L)
    value = np.sum(Kc * Lc) / float((n - 1) ** 2)
    return float(max(0.0, value))


def _feature_hsic(x: np.ndarray, y01: np.ndarray) -> float:
    K = _rbf_kernel_1d(x)
    L = (y01[:, None] == y01[None, :]).astype(float)
    return _hsic_from_kernels(K, L)


def _abs_corr(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=float).ravel()
    b = np.asarray(b, dtype=float).ravel()
    if a.size <= 1 or b.size <= 1:
        return 0.0
    sa = float(np.std(a))
    sb = float(np.std(b))
    if sa <= 1e-12 or sb <= 1e-12:
        return 0.0
    c = float(np.corrcoef(a, b)[0, 1])
    return abs(c) if np.isfinite(c) else 0.0


class ModelAlignedRaSEReducer:
    """RaSE-style random subspace selector with non-unary model-aligned criteria."""

    def __init__(
        self,
        n_select: int = 20,
        criterion: str = "cv_mcc_svm_rbf",
        *,
        num_models: int = 20,
        num_attempts: int = 32,
        max_subspace_size: int | None = None,
        inner_cv_splits: int = 5,
        svm_C_grid: tuple[float, ...] = (0.1, 1.0, 10.0),
        svm_gamma_grid: tuple[str | float, ...] = ("scale", 0.01, 0.1),
        lambda_std: float = 0.5,
        mu_red: float = 0.05,
        ebic_gamma: float = 0.5,
        bic_mode: str = "ebic",
        shadow_repeats: int = 5,
        exploration: float = 0.25,
        class_weight: str | dict[int, float] | None = "balanced",
        seed: int = 42,
        output: bool = False,
    ) -> None:
        allowed = set(CRITERIA_MAIN + [CRITERION_SHADOW])
        if criterion not in allowed:
            raise ValueError(f"Unknown criterion={criterion!r}. Expected one of {sorted(allowed)}")
        if bic_mode not in {"bic", "ebic"}:
            raise ValueError("bic_mode must be 'bic' or 'ebic'")

        self.n_select = int(n_select)
        self.criterion = str(criterion)
        self.num_models = int(num_models)
        self.num_attempts = int(num_attempts)
        self.max_subspace_size = None if max_subspace_size is None else int(max_subspace_size)
        self.inner_cv_splits = int(inner_cv_splits)
        self.svm_C_grid = tuple(svm_C_grid)
        self.svm_gamma_grid = tuple(svm_gamma_grid)
        self.lambda_std = float(lambda_std)
        self.mu_red = float(mu_red)
        self.ebic_gamma = float(ebic_gamma)
        self.bic_mode = str(bic_mode)
        self.shadow_repeats = int(shadow_repeats)
        self.exploration = float(exploration)
        self.class_weight = class_weight
        self.seed = int(seed)
        self.output = bool(output)

        self.rng_ = np.random.default_rng(self.seed)
        self.selected_indices_: np.ndarray | None = None
        self.feature_importances_: np.ndarray | None = None
        self.history_: list[dict[str, Any]] = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "ModelAlignedRaSEReducer":
        Xn = np.asarray(X, dtype=np.float64)
        y01 = _as_y01(y)
        if Xn.ndim != 2:
            raise ValueError("X must be a 2D array")
        if Xn.shape[0] != y01.size:
            raise ValueError("X and y have inconsistent lengths")
        if np.unique(y01).size != 2:
            raise ValueError("Need two classes for model-aligned RaSE")

        n_features = int(Xn.shape[1])
        if self.n_select > n_features:
            raise ValueError("n_select cannot exceed n_features")

        state = self._prepare_state(Xn, y01)
        counts = np.zeros(n_features, dtype=float)
        score_sum = np.zeros(n_features, dtype=float)
        sample_prob = np.full(n_features, 1.0 / n_features, dtype=float)
        max_size = min(n_features, self.max_subspace_size or max(self.n_select, 1))

        self.history_ = []
        for block_id in range(1, self.num_models + 1):
            best_result: CriterionResult | None = None
            best_indices: np.ndarray | None = None

            for _ in range(self.num_attempts):
                size = self._sample_subspace_size(max_size)
                idx = np.sort(self.rng_.choice(n_features, size=size, replace=False, p=sample_prob))
                result = self._score_subspace(Xn, y01, idx, state)
                if best_result is None or result.score > best_result.score:
                    best_result = result
                    best_indices = idx

            assert best_result is not None and best_indices is not None
            counts[best_indices] += 1.0
            score_sum[best_indices] += float(best_result.score)

            empirical = counts + 1e-9
            empirical /= empirical.sum()
            sample_prob = self.exploration / n_features + (1.0 - self.exploration) * empirical
            sample_prob /= sample_prob.sum()

            row = {
                "block": block_id,
                "criterion": self.criterion,
                "score": float(best_result.score),
                "subspace_size": int(best_indices.size),
                "selected_in_block": best_indices.astype(int).tolist(),
                **best_result.detail,
            }
            self.history_.append(row)
            if self.output:
                print(
                    f"[{self.criterion}] block={block_id}/{self.num_models} "
                    f"score={best_result.score:.6f} d={best_indices.size} "
                    f"idx={best_indices.tolist()}"
                )

        mean_score = np.divide(score_sum, np.maximum(counts, 1.0))
        centered_score = mean_score.copy()
        finite = np.isfinite(centered_score)
        if finite.any():
            lo, hi = np.nanmin(centered_score[finite]), np.nanmax(centered_score[finite])
            centered_score = (centered_score - lo) / (hi - lo + 1e-12)
        else:
            centered_score[:] = 0.0
        importance = counts + 1e-3 * centered_score
        self.feature_importances_ = importance
        self.selected_indices_ = np.sort(np.argsort(importance)[-self.n_select:]).astype(int)
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        if self.selected_indices_ is None:
            raise AssertionError("Call fit() before transform().")
        return np.asarray(X)[:, self.selected_indices_]

    def fit_transform(self, X: np.ndarray, y: np.ndarray) -> np.ndarray:
        return self.fit(X, y).transform(X)

    def get_support(self, indices: bool = False) -> np.ndarray:
        if self.selected_indices_ is None:
            raise AssertionError("Call fit() before get_support().")
        if indices:
            return self.selected_indices_.copy()
        mask = np.zeros_like(self.feature_importances_, dtype=bool)
        mask[self.selected_indices_] = True
        return mask

    def _sample_subspace_size(self, max_size: int) -> int:
        grid = np.arange(1, max_size + 1, dtype=int)
        weights = grid.astype(float)
        weights /= weights.sum()
        return int(self.rng_.choice(grid, p=weights))

    def _prepare_state(self, X: np.ndarray, y01: np.ndarray) -> dict[str, Any]:
        _, counts = np.unique(y01, return_counts=True)
        n_splits = min(self.inner_cv_splits, int(counts.min()))
        if n_splits < 2:
            raise ValueError("Need at least two samples per class for internal CV")
        splitter = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=self.seed)
        folds = list(splitter.split(np.zeros((y01.size, 1), dtype=float), y01))

        state: dict[str, Any] = {
            "folds": folds,
            "hsic_relevance": None,
            "mrmr_relevance": None,
            "corr_matrix": None,
            "hsic_pair_cache": {},
        }
        if self.criterion == "hsic_redundancy":
            state["hsic_relevance"] = np.asarray([_feature_hsic(X[:, j], y01) for j in range(X.shape[1])])
        if self.criterion == "mrmr_redundancy":
            y_float = y01.astype(float)
            state["mrmr_relevance"] = np.asarray([_abs_corr(X[:, j], y_float) for j in range(X.shape[1])])
            state["corr_matrix"] = self._safe_corr_matrix(X)
        if self.criterion in {"cv_mcc_svm_rbf", "cv_auc_svm_rbf", CRITERION_SHADOW}:
            state["corr_matrix"] = self._safe_corr_matrix(X)
        return state

    @staticmethod
    def _safe_corr_matrix(X: np.ndarray) -> np.ndarray:
        corr = np.corrcoef(np.asarray(X, dtype=float), rowvar=False)
        corr = np.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)
        np.fill_diagonal(corr, 0.0)
        return np.abs(corr)

    def _corr_redundancy(self, indices: np.ndarray, state: dict[str, Any]) -> float:
        if indices.size <= 1:
            return 0.0
        corr = state.get("corr_matrix")
        if corr is None:
            return 0.0
        sub = corr[np.ix_(indices, indices)]
        vals = sub[np.triu_indices_from(sub, k=1)]
        return float(vals.mean()) if vals.size else 0.0

    def _hsic_redundancy(self, X: np.ndarray, indices: np.ndarray, state: dict[str, Any]) -> float:
        if indices.size <= 1:
            return 0.0
        cache: dict[tuple[int, int], float] = state["hsic_pair_cache"]
        values: list[float] = []
        for pos_i, i in enumerate(indices[:-1]):
            for j in indices[pos_i + 1:]:
                key = (int(min(i, j)), int(max(i, j)))
                if key not in cache:
                    cache[key] = _hsic_from_kernels(_rbf_kernel_1d(X[:, key[0]]), _rbf_kernel_1d(X[:, key[1]]))
                values.append(cache[key])
        return float(np.mean(values)) if values else 0.0

    def _score_subspace(
        self,
        X: np.ndarray,
        y01: np.ndarray,
        indices: np.ndarray,
        state: dict[str, Any],
    ) -> CriterionResult:
        if self.criterion == "cv_mcc_svm_rbf":
            return self._score_cv_svm(X, y01, indices, state, metric="mcc")
        if self.criterion == "cv_auc_svm_rbf":
            return self._score_cv_svm(X, y01, indices, state, metric="auc")
        if self.criterion == CRITERION_SHADOW:
            return self._score_shadow_cv_mcc(X, y01, indices, state)
        if self.criterion == "hsic_redundancy":
            relevance = float(np.mean(state["hsic_relevance"][indices]))
            red = self._hsic_redundancy(X, indices, state)
            return CriterionResult(relevance - self.mu_red * red, {"relevance": relevance, "redundancy": red})
        if self.criterion == "mrmr_redundancy":
            relevance = float(np.mean(state["mrmr_relevance"][indices]))
            red = self._corr_redundancy(indices, state)
            return CriterionResult(relevance - self.mu_red * red, {"relevance": relevance, "redundancy": red})
        if self.criterion == "bic_logreg":
            return self._score_bic_logreg(X, y01, indices)
        raise AssertionError(f"Unhandled criterion: {self.criterion}")

    def _score_cv_svm(
        self,
        X: np.ndarray,
        y01: np.ndarray,
        indices: np.ndarray,
        state: dict[str, Any],
        *,
        metric: str,
    ) -> CriterionResult:
        red = self._corr_redundancy(indices, state)
        Xs = X[:, indices]
        best: dict[str, Any] | None = None
        for C in self.svm_C_grid:
            for gamma in self.svm_gamma_grid:
                vals: list[float] = []
                for tr, val in state["folds"]:
                    model = SVC(
                        C=float(C),
                        gamma=gamma,
                        kernel="rbf",
                        class_weight=self.class_weight,
                        random_state=self.seed,
                    )
                    model.fit(Xs[tr], y01[tr])
                    if metric == "mcc":
                        vals.append(float(matthews_corrcoef(y01[val], model.predict(Xs[val]))))
                    else:
                        vals.append(_safe_auc(y01[val], model.decision_function(Xs[val])))
                mean_val = float(np.mean(vals))
                std_val = _safe_std(vals)
                score = mean_val - self.lambda_std * std_val - self.mu_red * red
                if best is None or score > best["score"]:
                    best = {
                        "score": score,
                        "cv_mean": mean_val,
                        "cv_std": std_val,
                        "redundancy": red,
                        "svm_C": float(C),
                        "svm_gamma": gamma,
                    }
        assert best is not None
        return CriterionResult(float(best["score"]), best)

    def _score_shadow_cv_mcc(
        self,
        X: np.ndarray,
        y01: np.ndarray,
        indices: np.ndarray,
        state: dict[str, Any],
    ) -> CriterionResult:
        base = self._score_cv_svm(X, y01, indices, state, metric="mcc")
        shadow_scores: list[float] = []
        X_shadow = X.copy()
        for _ in range(max(1, self.shadow_repeats)):
            for idx in indices:
                X_shadow[:, idx] = self.rng_.permutation(X[:, idx])
            shadow = self._score_cv_svm(X_shadow, y01, indices, state, metric="mcc")
            shadow_scores.append(float(shadow.score))
        q95 = float(np.quantile(shadow_scores, 0.95))
        detail = dict(base.detail)
        detail.update({"base_score": float(base.score), "shadow_q95": q95})
        return CriterionResult(float(base.score - q95), detail)

    def _score_bic_logreg(self, X: np.ndarray, y01: np.ndarray, indices: np.ndarray) -> CriterionResult:
        Xs = X[:, indices]
        model = LogisticRegression(
            penalty="l2",
            C=1.0,
            solver="lbfgs",
            class_weight=self.class_weight,
            max_iter=5000,
            random_state=self.seed,
        )
        model.fit(Xs, y01)
        prob = np.clip(model.predict_proba(Xs), 1e-9, 1.0 - 1e-9)
        n = int(y01.size)
        k = int(indices.size + 1)
        neg2ll = 2.0 * log_loss(y01, prob, labels=[0, 1], normalize=False)
        penalty = k * math.log(max(n, 2))
        if self.bic_mode == "ebic":
            p = int(X.shape[1])
            penalty += 2.0 * self.ebic_gamma * self._log_comb(p, k)
        criterion = float(neg2ll + penalty)
        return CriterionResult(-criterion, {"bic_value": criterion, "bic_mode": self.bic_mode})

    @staticmethod
    @lru_cache(maxsize=4096)
    def _log_comb(n: int, k: int) -> float:
        if k < 0 or k > n:
            return 0.0
        return float(math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1))


__all__ = [
    "CRITERIA_MAIN",
    "CRITERION_SHADOW",
    "CriterionResult",
    "ModelAlignedRaSEReducer",
]
