from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

import nirs_core as core


def _read_precomputed_stats(csv_path: Path, metrics: Iterable[str]) -> Dict[str, Tuple[float, float]]:
    return core._read_precomputed_stats(csv_path, metrics)


def summarize_results_dir(
    results_dir: str | Path,
    *,
    our_method_file: str = "results.csv",
    save: bool = True,
) -> pd.DataFrame:
    root = Path(results_dir).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    metric_files = [
        "results.csv",
        "svm_results.csv",
        "knn_results.csv",
        "gaussiannb_results.csv",
        "kde_bayes_results.csv",
        "qda_results.csv",
        "shrinkage_lda_results.csv",
        "mlp_results.csv",
    ]
    available = [name for name in metric_files if (root / name).exists()]
    if our_method_file not in available:
        raise FileNotFoundError(f"Our method file was not found in {root}: {our_method_file}")

    rows = []
    our_stats = _read_precomputed_stats(root / our_method_file, ["accuracy", "f1_score"])
    for filename in available:
        stats = _read_precomputed_stats(root / filename, ["accuracy", "f1_score"])
        rows.append(
            {
                "file": filename,
                "accuracy_mean": stats["accuracy"][0],
                "accuracy_std": stats["accuracy"][1],
                "f1_mean": stats["f1_score"][0],
                "f1_std": stats["f1_score"][1],
            }
        )

    all_df = pd.DataFrame(rows)
    others_df = all_df.loc[all_df["file"] != our_method_file].copy()
    best_other_accuracy = others_df.sort_values(["accuracy_mean", "f1_mean"], ascending=False).iloc[0] if not others_df.empty else None
    best_other_f1 = others_df.sort_values(["f1_mean", "accuracy_mean"], ascending=False).iloc[0] if not others_df.empty else None

    summary = pd.DataFrame(
        [
            {
                "our_method_file": our_method_file,
                "our_accuracy_mean": our_stats["accuracy"][0],
                "our_accuracy_std": our_stats["accuracy"][1],
                "best_other_accuracy_file": None if best_other_accuracy is None else best_other_accuracy["file"],
                "best_other_accuracy_mean": None if best_other_accuracy is None else best_other_accuracy["accuracy_mean"],
                "best_other_accuracy_std": None if best_other_accuracy is None else best_other_accuracy["accuracy_std"],
                "accuracy_margin": None if best_other_accuracy is None else our_stats["accuracy"][0] - float(best_other_accuracy["accuracy_mean"]),
                "our_f1_mean": our_stats["f1_score"][0],
                "our_f1_std": our_stats["f1_score"][1],
                "best_other_f1_file": None if best_other_f1 is None else best_other_f1["file"],
                "best_other_f1_mean": None if best_other_f1 is None else best_other_f1["f1_mean"],
                "best_other_f1_std": None if best_other_f1 is None else best_other_f1["f1_std"],
                "f1_margin": None if best_other_f1 is None else our_stats["f1_score"][0] - float(best_other_f1["f1_mean"]),
                "accuracy_rank": int((all_df["accuracy_mean"] > our_stats["accuracy"][0]).sum() + 1),
                "f1_rank": int((all_df["f1_mean"] > our_stats["f1_score"][0]).sum() + 1),
            }
        ]
    )

    unary_path = root / "unary_strict_results.csv"
    if unary_path.exists():
        unary_stats = _read_precomputed_stats(
            unary_path,
            [
                "S_test",
                "selective_accuracy",
                "selective_f1",
            ],
        )
        summary["unary_S_test_mean"] = unary_stats["S_test"][0]
        summary["unary_S_test_std"] = unary_stats["S_test"][1]
        summary["unary_selective_accuracy_mean"] = unary_stats["selective_accuracy"][0]
        summary["unary_selective_accuracy_std"] = unary_stats["selective_accuracy"][1]
        summary["unary_selective_f1_mean"] = unary_stats["selective_f1"][0]
        summary["unary_selective_f1_std"] = unary_stats["selective_f1"][1]

    if save:
        summary.to_csv(root / "final_method_comparison.csv", index=False)
        (root / "final_method_comparison.json").write_text(
            summary.to_json(orient="records", force_ascii=False, indent=2),
            encoding="utf-8",
        )

    return summary


def print_ablation_summary(root_dir: str | Path) -> str:
    return core.print_ablation_summary(root_dir)


def summarize_all_colon_result_dirs(
    root_dir: str | Path,
    *,
    dir_prefix: str = "results_colon_cv_seed",
    our_method_file: str = "results.csv",
    save: bool = True,
) -> pd.DataFrame:
    root = Path(root_dir).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    rows = []
    for run_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith(dir_prefix)):
        try:
            summary = summarize_results_dir(run_dir, our_method_file=our_method_file, save=False)
            row = summary.iloc[0].to_dict()
            row["run_dir"] = run_dir.name
            rows.append(row)
        except Exception as exc:
            rows.append(
                {
                    "run_dir": run_dir.name,
                    "error": str(exc),
                }
            )

    df = pd.DataFrame(rows)
    if save and not df.empty:
        df.to_csv(root / "all_colon_method_comparisons.csv", index=False)
    return df


def summarize_reduction_results(results_dir: str | Path, *, save: bool = True) -> dict[str, pd.DataFrame]:
    root = Path(results_dir).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    summary_classical_path = root / "summary_classical.csv"
    summary_unary_path = root / "summary_unary.csv"
    all_classical_path = root / "all_classical_results.csv"
    all_unary_path = root / "all_unary_results.csv"

    out: dict[str, pd.DataFrame] = {}

    if summary_classical_path.exists():
        summary_classical = pd.read_csv(summary_classical_path)
        if not summary_classical.empty:
            ranked_classical = summary_classical.sort_values(
                ["dataset", "model", "dAUC_mean", "PR_AUC_mean"],
                ascending=[True, True, False, False],
            ).reset_index(drop=True)
            out["summary_classical"] = ranked_classical

    if summary_unary_path.exists():
        summary_unary = pd.read_csv(summary_unary_path)
        if not summary_unary.empty:
            ranked_unary = summary_unary.sort_values(
                ["dataset", "S_test_mean", "F12_mean", "coverage_mean"],
                ascending=[True, False, False, False],
            ).reset_index(drop=True)
            out["summary_unary"] = ranked_unary

    if save:
        if "summary_classical" in out:
            out["summary_classical"].to_csv(root / "final_reduction_comparison_classical.csv", index=False)
        if "summary_unary" in out:
            out["summary_unary"].to_csv(root / "final_reduction_comparison_unary.csv", index=False)
        meta = {
            "results_dir": str(root),
            "has_all_classical_results": all_classical_path.exists(),
            "has_all_unary_results": all_unary_path.exists(),
            "has_summary_classical": summary_classical_path.exists(),
            "has_summary_unary": summary_unary_path.exists(),
        }
        (root / "final_reduction_comparison_meta.json").write_text(
            pd.Series(meta).to_json(force_ascii=False, indent=2),
            encoding="utf-8",
        )

    return out


def analyze_b_like_results(
    results_dir: str | Path,
    *,
    our_method: str,
    save: bool = True,
) -> dict[str, pd.DataFrame]:
    root = Path(results_dir).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    summary_classical_path = root / "summary_classical.csv"
    if not summary_classical_path.exists():
        raise FileNotFoundError(f"File not found: {summary_classical_path}")

    classical = pd.read_csv(summary_classical_path)
    if classical.empty:
        raise ValueError(f"Empty file: {summary_classical_path}")

    classical = classical.copy()
    classical["is_our_method"] = classical["method"].astype(str) == str(our_method)

    per_model_rows = []
    q_overall_rows = []

    grouped = classical.groupby(["dataset", "q", "model"], dropna=False)
    for (dataset, q, model), group in grouped:
        group = group.sort_values(
            ["dAUC_mean", "PR_AUC_mean", "AUC_mean"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        group["rank_by_dAUC"] = group.index + 1

        our_rows = group.loc[group["is_our_method"]].copy()
        if our_rows.empty:
            per_model_rows.append(
                {
                    "dataset": dataset,
                    "q": q,
                    "model": model,
                    "our_method": our_method,
                    "our_method_found": False,
                }
            )
            continue

        our_row = our_rows.iloc[0]
        better = group.loc[group["rank_by_dAUC"] < int(our_row["rank_by_dAUC"])].copy()
        best_other = group.loc[group["method"] != our_method].head(1)
        best_other_row = best_other.iloc[0] if not best_other.empty else None

        per_model_rows.append(
            {
                "dataset": dataset,
                "q": q,
                "model": model,
                "our_method": our_method,
                "our_method_found": True,
                "our_rank": int(our_row["rank_by_dAUC"]),
                "n_methods": int(len(group)),
                "our_dAUC_mean": float(our_row["dAUC_mean"]),
                "our_PR_AUC_mean": float(our_row["PR_AUC_mean"]),
                "our_AUC_mean": float(our_row["AUC_mean"]),
                "best_method": None if best_other_row is None else best_other_row["method"],
                "best_dAUC_mean": None if best_other_row is None else float(best_other_row["dAUC_mean"]),
                "best_PR_AUC_mean": None if best_other_row is None else float(best_other_row["PR_AUC_mean"]),
                "best_AUC_mean": None if best_other_row is None else float(best_other_row["AUC_mean"]),
                "delta_to_best_dAUC": None if best_other_row is None else float(our_row["dAUC_mean"] - best_other_row["dAUC_mean"]),
                "delta_to_best_PR_AUC": None if best_other_row is None else float(our_row["PR_AUC_mean"] - best_other_row["PR_AUC_mean"]),
                "delta_to_best_AUC": None if best_other_row is None else float(our_row["AUC_mean"] - best_other_row["AUC_mean"]),
                "better_methods_count": int(len(better)),
                "better_methods": "" if better.empty else "; ".join(
                    f"{row.method}(dAUC={row.dAUC_mean:.6f},PR_AUC={row.PR_AUC_mean:.6f},AUC={row.AUC_mean:.6f})"
                    for row in better.itertuples(index=False)
                ),
            }
        )

    per_model_df = pd.DataFrame(per_model_rows)

    if not per_model_df.empty:
        q_grouped = per_model_df.loc[per_model_df["our_method_found"]].groupby(["dataset", "q"], dropna=False)
        q_overall_df = (
            q_grouped.agg(
                n_models=("model", "count"),
                first_place_count=("our_rank", lambda s: int((s == 1).sum())),
                mean_rank=("our_rank", "mean"),
                median_rank=("our_rank", "median"),
                max_rank=("our_rank", "max"),
            )
            .reset_index()
        )
        q_overall_df["first_place_summary"] = q_overall_df.apply(
            lambda row: f"{our_method} is first in {int(row['first_place_count'])} of {int(row['n_models'])} models for q={row['q']}",
            axis=1,
        )
    else:
        q_overall_df = pd.DataFrame()

    out: dict[str, pd.DataFrame] = {
        "per_model_q": per_model_df,
        "overall_by_q": q_overall_df,
    }

    summary_unary_path = root / "summary_unary.csv"
    if summary_unary_path.exists():
        unary = pd.read_csv(summary_unary_path)
        if not unary.empty:
            unary = unary.copy()
            unary["is_our_method"] = unary["method"].astype(str) == str(our_method)
            unary_rows = []
            for (dataset, q), group in unary.groupby(["dataset", "q"], dropna=False):
                group = group.sort_values(
                    ["S_test_mean", "F12_mean", "coverage_mean"],
                    ascending=[False, False, False],
                ).reset_index(drop=True)
                group["rank_by_S"] = group.index + 1
                our_rows = group.loc[group["is_our_method"]]
                if our_rows.empty:
                    unary_rows.append(
                        {
                            "dataset": dataset,
                            "q": q,
                            "our_method": our_method,
                            "our_method_found": False,
                        }
                    )
                    continue
                our_row = our_rows.iloc[0]
                better = group.loc[group["rank_by_S"] < int(our_row["rank_by_S"])].copy()
                unary_rows.append(
                    {
                        "dataset": dataset,
                        "q": q,
                        "our_method": our_method,
                        "our_method_found": True,
                        "our_rank_by_S": int(our_row["rank_by_S"]),
                        "our_S_test_mean": float(our_row["S_test_mean"]),
                        "our_F12_mean": float(our_row["F12_mean"]),
                        "our_coverage_mean": float(our_row["coverage_mean"]),
                        "better_methods_count": int(len(better)),
                        "better_methods": "" if better.empty else "; ".join(
                            f"{row.method}(S={row.S_test_mean:.6f},F12={row.F12_mean:.6f},coverage={row.coverage_mean:.6f})"
                            for row in better.itertuples(index=False)
                        ),
                    }
                )
            out["unary_by_q"] = pd.DataFrame(unary_rows)

    if save:
        out["per_model_q"].to_csv(root / "b_like_method_ranking_per_model_q.csv", index=False)
        out["overall_by_q"].to_csv(root / "b_like_method_ranking_overall_by_q.csv", index=False)
        if "unary_by_q" in out:
            out["unary_by_q"].to_csv(root / "b_like_method_ranking_unary_by_q.csv", index=False)

    return out


__all__ = [
    "_read_precomputed_stats",
    "analyze_b_like_results",
    "print_ablation_summary",
    "summarize_all_colon_result_dirs",
    "summarize_reduction_results",
    "summarize_results_dir",
]
