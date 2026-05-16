from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def summarize_classical(root: Path) -> None:
    rows = []
    for f in root.glob("seed_*/*/DS-*/*/all_classical_folds.csv"):
        seed = int(f.parts[-5].split("_")[1])
        df = pd.read_csv(f)
        df["seed"] = seed
        rows.append(df)

    if not rows:
        print("NO_CLASSICAL_DATA")
        return

    full = pd.concat(rows, ignore_index=True)
    method_q = (
        full.groupby(["method", "q"])
        .agg(
            auc_mean=("AUC", "mean"),
            auc_std=("AUC", "std"),
            pr_mean=("PR_AUC", "mean"),
            n=("AUC", "size"),
        )
        .reset_index()
        .sort_values(["auc_mean", "pr_mean"], ascending=False)
    )

    by_model = (
        full.groupby(["method", "q", "model"])
        .agg(
            auc_mean=("AUC", "mean"),
            auc_std=("AUC", "std"),
            pr_mean=("PR_AUC", "mean"),
            n=("AUC", "size"),
        )
        .reset_index()
        .sort_values(["auc_mean", "pr_mean"], ascending=False)
    )

    pivot = by_model.pivot_table(index=["q", "model"], columns="method", values="auc_mean")
    comps = []
    for (q, model), row in pivot.iterrows():
        if "Ensembleunar" not in row or pd.isna(row["Ensembleunar"]):
            continue
        others = row.drop(labels=["Ensembleunar"]).dropna()
        if others.empty:
            continue
        best_other = others.idxmax()
        best_other_auc = float(others.max())
        ens_auc = float(row["Ensembleunar"])
        comps.append(
            {
                "q": q,
                "model": model,
                "ens_auc": ens_auc,
                "best_other": best_other,
                "best_other_auc": best_other_auc,
                "gap": ens_auc - best_other_auc,
            }
        )
    comp_df = pd.DataFrame(comps).sort_values("gap")

    print("CLASSICAL_METHOD_Q")
    print(method_q.to_csv(index=False))
    print("CLASSICAL_TOP_MODELS")
    print(by_model.head(25).to_csv(index=False))
    print("ENSEMBLE_VS_BEST_OTHER")
    print(comp_df.to_csv(index=False))

    for metric in ("AUC", "PR_AUC"):
        agg_metric = (
            full.groupby(["method", "q", "model"])
            .agg(value=(metric, "mean"))
            .reset_index()
        )
        rank_rows = []
        for (q, model), grp in agg_metric.groupby(["q", "model"]):
            grp = grp.sort_values("value", ascending=False).reset_index(drop=True)
            if "Ensembleunar" not in set(grp["method"]):
                continue
            grp["rank"] = range(1, len(grp) + 1)
            ens_row = grp.loc[grp["method"] == "Ensembleunar"].iloc[0]
            better = grp.loc[grp["rank"] < ens_row["rank"], ["method", "value"]]
            rank_rows.append(
                {
                    "q": q,
                    "model": model,
                    "metric": metric,
                    "rank": int(ens_row["rank"]),
                    "n_methods": int(len(grp)),
                    "ens_value": float(ens_row["value"]),
                    "winner": grp.iloc[0]["method"],
                    "winner_value": float(grp.iloc[0]["value"]),
                    "gap_to_winner": float(ens_row["value"] - grp.iloc[0]["value"]),
                    "better_methods": "; ".join(
                        f"{row.method}:{row.value - ens_row['value']:.6f}"
                        for row in better.itertuples(index=False)
                    ),
                }
            )
        rank_df = pd.DataFrame(rank_rows)
        if rank_df.empty:
            print(f"ENSEMBLE_RANK_COUNTS_{metric}")
            print("NO_DATA")
            continue
        rank_counts = (
            rank_df.groupby("rank")
            .size()
            .rename("count")
            .reset_index()
            .sort_values("rank")
        )
        print(f"ENSEMBLE_RANK_COUNTS_{metric}")
        print(rank_counts.to_csv(index=False))
    print(f"ENSEMBLE_RANK_DETAILS_{metric}")
    print(rank_df.sort_values(["rank", "gap_to_winner", "q", "model"]).to_csv(index=False))

    pair_rows = []
    for (q, model), grp in full.groupby(["q", "model"]):
        sub = (
            grp.groupby("method")
            .agg(
                auc_mean=("AUC", "mean"),
                auc_std=("AUC", "std"),
                pr_mean=("PR_AUC", "mean"),
                pr_std=("PR_AUC", "std"),
                n=("AUC", "size"),
            )
            .reset_index()
        )
        methods = set(sub["method"])
        if not {"Ensembleunar", "PLS"}.issubset(methods):
            continue
        ens = sub.loc[sub["method"] == "Ensembleunar"].iloc[0]
        pls = sub.loc[sub["method"] == "PLS"].iloc[0]
        auc_thr = ((float(ens["auc_std"]) ** 2) + (float(pls["auc_std"]) ** 2)) ** 0.5
        pr_thr = ((float(ens["pr_std"]) ** 2) + (float(pls["pr_std"]) ** 2)) ** 0.5
        auc_gap = float(ens["auc_mean"] - pls["auc_mean"])
        pr_gap = float(ens["pr_mean"] - pls["pr_mean"])
        pair_rows.append(
            {
                "q": q,
                "model": model,
                "auc_gap": auc_gap,
                "auc_thr": auc_thr,
                "auc_status": (
                    "ensemble_win"
                    if auc_gap > auc_thr
                    else "pls_win"
                    if -auc_gap > auc_thr
                    else "tie_noise"
                ),
                "pr_gap": pr_gap,
                "pr_thr": pr_thr,
                "pr_status": (
                    "ensemble_win"
                    if pr_gap > pr_thr
                    else "pls_win"
                    if -pr_gap > pr_thr
                    else "tie_noise"
                ),
            }
        )
    pair_df = pd.DataFrame(pair_rows)
    if not pair_df.empty:
        std_summary = pd.DataFrame(
            {
                "metric": ["AUC", "PR_AUC"],
                "ensemble_std_mean": [pair_df["auc_thr"].mean(), pair_df["pr_thr"].mean()],
                "note": ["combined_threshold_mean", "combined_threshold_mean"],
            }
        )
        pair_std_rows = []
        for metric_prefix in ["auc", "pr"]:
            std_rows = []
            for (q, model), grp in full.groupby(["q", "model"]):
                sub = (
                    grp.groupby("method")
                    .agg(std=(("AUC" if metric_prefix == "auc" else "PR_AUC"), "std"))
                    .reset_index()
                )
                methods = set(sub["method"])
                if not {"Ensembleunar", "PLS"}.issubset(methods):
                    continue
                ens_std = float(sub.loc[sub["method"] == "Ensembleunar", "std"].iloc[0])
                pls_std = float(sub.loc[sub["method"] == "PLS", "std"].iloc[0])
                pair_std_rows.append(
                    {
                        "metric": "AUC" if metric_prefix == "auc" else "PR_AUC",
                        "q": q,
                        "model": model,
                        "ensemble_std": ens_std,
                        "pls_std": pls_std,
                        "std_gap": ens_std - pls_std,
                        "noisier": "Ensembleunar"
                        if ens_std > pls_std
                        else "PLS"
                        if pls_std > ens_std
                        else "equal",
                    }
                )
        pair_std_df = pd.DataFrame(pair_std_rows)
        if not pair_std_df.empty:
            print("ENSEMBLE_VS_PLS_STD_COUNTS")
            print(
                pair_std_df.groupby(["metric", "noisier"])
                .size()
                .rename("count")
                .reset_index()
                .to_csv(index=False)
            )
            print("ENSEMBLE_VS_PLS_STD_SUMMARY")
            print(
                pair_std_df.groupby("metric")
                .agg(
                    ensemble_std_mean=("ensemble_std", "mean"),
                    pls_std_mean=("pls_std", "mean"),
                    std_gap_mean=("std_gap", "mean"),
                )
                .reset_index()
                .to_csv(index=False)
            )
            print("ENSEMBLE_VS_PLS_STD_DETAILS")
            print(pair_std_df.sort_values(["metric", "q", "model"]).to_csv(index=False))
        print("ENSEMBLE_VS_PLS_NOISE_COUNTS_AUC")
        print(pair_df.groupby("auc_status").size().rename("count").reset_index().to_csv(index=False))
        print("ENSEMBLE_VS_PLS_NOISE_COUNTS_PR_AUC")
        print(pair_df.groupby("pr_status").size().rename("count").reset_index().to_csv(index=False))
        print("ENSEMBLE_VS_PLS_NOISE_DETAILS")
        print(pair_df.sort_values(["q", "model"]).to_csv(index=False))

    methods_all = sorted(full["method"].dropna().unique().tolist())
    pairwise_rows = []
    for (q, model), grp in full.groupby(["q", "model"]):
        sub = (
            grp.groupby("method")
            .agg(
                auc_mean=("AUC", "mean"),
                auc_std=("AUC", "std"),
                pr_mean=("PR_AUC", "mean"),
                pr_std=("PR_AUC", "std"),
            )
            .reset_index()
        )
        stats = {row["method"]: row for _, row in sub.iterrows()}
        if "Ensembleunar" not in stats:
            continue
        ens = stats["Ensembleunar"]
        for other in methods_all:
            if other == "Ensembleunar" or other not in stats:
                continue
            oth = stats[other]
            auc_gap = float(ens["auc_mean"] - oth["auc_mean"])
            auc_thr = ((float(ens["auc_std"]) ** 2) + (float(oth["auc_std"]) ** 2)) ** 0.5
            pr_gap = float(ens["pr_mean"] - oth["pr_mean"])
            pr_thr = ((float(ens["pr_std"]) ** 2) + (float(oth["pr_std"]) ** 2)) ** 0.5
            pairwise_rows.append(
                {
                    "q": q,
                    "model": model,
                    "other": other,
                    "auc_gap": auc_gap,
                    "auc_thr": auc_thr,
                    "auc_status": "ensemble_win"
                    if auc_gap > auc_thr
                    else "other_win"
                    if -auc_gap > auc_thr
                    else "tie_noise",
                    "pr_gap": pr_gap,
                    "pr_thr": pr_thr,
                    "pr_status": "ensemble_win"
                    if pr_gap > pr_thr
                    else "other_win"
                    if -pr_gap > pr_thr
                    else "tie_noise",
                }
            )
    if pairwise_rows:
        pairwise_df = pd.DataFrame(pairwise_rows)
        print("ENSEMBLE_VS_ALL_NOISE_COUNTS_AUC")
        print(
            pairwise_df.groupby(["other", "auc_status"])
            .size()
            .rename("count")
            .reset_index()
            .to_csv(index=False)
        )
        print("ENSEMBLE_VS_ALL_NOISE_COUNTS_PR_AUC")
        print(
            pairwise_df.groupby(["other", "pr_status"])
            .size()
            .rename("count")
            .reset_index()
            .to_csv(index=False)
        )
        print("ENSEMBLE_VS_ALL_NOISE_DETAILS")
        print(pairwise_df.sort_values(["other", "q", "model"]).to_csv(index=False))

    avg_pairwise_rows = []
    for (q, model), grp in full.groupby(["q", "model"]):
        sub = (
            grp.groupby("method")
            .agg(
                auc_mean=("AUC", "mean"),
                auc_std=("AUC", "std"),
                pr_mean=("PR_AUC", "mean"),
                pr_std=("PR_AUC", "std"),
            )
            .reset_index()
        )
        stats = {row["method"]: row for _, row in sub.iterrows()}
        if "Ensembleunar" not in stats:
            continue
        ens = stats["Ensembleunar"]
        for other in methods_all:
            if other == "Ensembleunar" or other not in stats:
                continue
            oth = stats[other]
            auc_gap = float(ens["auc_mean"] - oth["auc_mean"])
            auc_thr = (float(ens["auc_std"]) + float(oth["auc_std"])) / 2.0
            pr_gap = float(ens["pr_mean"] - oth["pr_mean"])
            pr_thr = (float(ens["pr_std"]) + float(oth["pr_std"])) / 2.0
            avg_pairwise_rows.append(
                {
                    "q": q,
                    "model": model,
                    "other": other,
                    "auc_gap": auc_gap,
                    "auc_thr_avg": auc_thr,
                    "auc_status_avg": "ensemble_win"
                    if auc_gap > auc_thr
                    else "other_win"
                    if -auc_gap > auc_thr
                    else "tie_noise",
                    "pr_gap": pr_gap,
                    "pr_thr_avg": pr_thr,
                    "pr_status_avg": "ensemble_win"
                    if pr_gap > pr_thr
                    else "other_win"
                    if -pr_gap > pr_thr
                    else "tie_noise",
                }
            )
    if avg_pairwise_rows:
        avg_pairwise_df = pd.DataFrame(avg_pairwise_rows)
        print("ENSEMBLE_VS_ALL_AVGSTD_COUNTS_AUC")
        print(
            avg_pairwise_df.groupby(["other", "auc_status_avg"])
            .size()
            .rename("count")
            .reset_index()
            .to_csv(index=False)
        )
        print("ENSEMBLE_VS_ALL_AVGSTD_COUNTS_PR_AUC")
        print(
            avg_pairwise_df.groupby(["other", "pr_status_avg"])
            .size()
            .rename("count")
            .reset_index()
            .to_csv(index=False)
        )
        print("ENSEMBLE_VS_ALL_AVGSTD_DETAILS")
        print(avg_pairwise_df.sort_values(["other", "q", "model"]).to_csv(index=False))


def summarize_unary(root: Path) -> None:
    rows = []
    for f in root.glob("seed_*/*/DS-*/*/all_unary_folds.csv"):
        seed = int(f.parts[-5].split("_")[1])
        df = pd.read_csv(f)
        df["seed"] = seed
        rows.append(df)

    if not rows:
        print("NO_UNARY_DATA")
        return

    full = pd.concat(rows, ignore_index=True)
    cols = full.columns.tolist()
    score_col = "S_test" if "S_test" in cols else ("S" if "S" in cols else None)
    f12_col = "F12" if "F12" in cols else None
    g12_col = "G12" if "G12" in cols else None

    agg_map = {"n": ("seed", "size")}
    if score_col:
        agg_map["score_mean"] = (score_col, "mean")
        agg_map["score_std"] = (score_col, "std")
    if f12_col:
        agg_map["f12_mean"] = (f12_col, "mean")
        agg_map["f12_std"] = (f12_col, "std")
    if g12_col:
        agg_map["g12_mean"] = (g12_col, "mean")
        agg_map["g12_std"] = (g12_col, "std")

    method_q = (
        full.groupby(["method", "q"])
        .agg(**agg_map)
        .reset_index()
        .sort_values([c for c in ["score_mean", "f12_mean"] if c in agg_map], ascending=False)
    )

    print("UNARY_METHOD_Q")
    print(method_q.to_csv(index=False))

    unary_pair_rows = []
    for q, grp in full.groupby(["q"]):
        stats = grp.groupby("method").agg(
            score_mean=(score_col, "mean") if score_col else ("seed", "size"),
            score_std=(score_col, "std") if score_col else ("seed", "size"),
            f12_mean=(f12_col, "mean") if f12_col else ("seed", "size"),
            f12_std=(f12_col, "std") if f12_col else ("seed", "size"),
            g12_mean=(g12_col, "mean") if g12_col else ("seed", "size"),
            g12_std=(g12_col, "std") if g12_col else ("seed", "size"),
        )
        if "Ensembleunar" not in stats.index or "PLS" not in stats.index:
            continue
        rec = {"q": q}
        for prefix, mean_col, std_col in [
            ("score", "score_mean", "score_std"),
            ("f12", "f12_mean", "f12_std"),
            ("g12", "g12_mean", "g12_std"),
        ]:
            if mean_col not in stats.columns:
                continue
            ens_mean = float(stats.loc["Ensembleunar", mean_col])
            pls_mean = float(stats.loc["PLS", mean_col])
            ens_std = float(stats.loc["Ensembleunar", std_col]) if pd.notna(stats.loc["Ensembleunar", std_col]) else 0.0
            pls_std = float(stats.loc["PLS", std_col]) if pd.notna(stats.loc["PLS", std_col]) else 0.0
            thr = (ens_std**2 + pls_std**2) ** 0.5
            gap = ens_mean - pls_mean
            rec[f"{prefix}_gap"] = gap
            rec[f"{prefix}_thr"] = thr
            rec[f"{prefix}_status"] = (
                "ensemble_win"
                if gap > thr
                else "pls_win"
                if -gap > thr
                else "tie_noise"
            )
        unary_pair_rows.append(rec)
    if unary_pair_rows:
        unary_pair_df = pd.DataFrame(unary_pair_rows).sort_values("q")
        print("ENSEMBLE_VS_PLS_UNARY_BY_Q")
        print(unary_pair_df.to_csv(index=False))
        unary_std_rows = []
        for q, grp in full.groupby(["q"]):
            stats = grp.groupby("method").agg(
                S_std=("S_test", "std") if "S_test" in full.columns else ("S", "std"),
                F12_std=("F12", "std") if "F12" in full.columns else ("seed", "size"),
                G12_std=("G12", "std") if "G12" in full.columns else ("seed", "size"),
            )
            if "Ensembleunar" not in stats.index or "PLS" not in stats.index:
                continue
            for c in ["S_std", "F12_std", "G12_std"]:
                ens_std = float(stats.loc["Ensembleunar", c]) if pd.notna(stats.loc["Ensembleunar", c]) else 0.0
                pls_std = float(stats.loc["PLS", c]) if pd.notna(stats.loc["PLS", c]) else 0.0
                unary_std_rows.append(
                    {
                        "q": q,
                        "metric": c.replace("_std", ""),
                        "ensemble_std": ens_std,
                        "pls_std": pls_std,
                        "std_gap": ens_std - pls_std,
                        "noisier": "Ensembleunar"
                        if ens_std > pls_std
                        else "PLS"
                        if pls_std > ens_std
                        else "equal",
                    }
                )
        if unary_std_rows:
            unary_std_df = pd.DataFrame(unary_std_rows)
            print("ENSEMBLE_VS_PLS_UNARY_STD_SUMMARY")
            print(
                unary_std_df.groupby("metric")
                .agg(
                    ensemble_std_mean=("ensemble_std", "mean"),
                    pls_std_mean=("pls_std", "mean"),
                    std_gap_mean=("std_gap", "mean"),
                )
                .reset_index()
                .to_csv(index=False)
            )

    unary_methods = sorted(full["method"].dropna().unique().tolist())
    unary_pairwise_rows = []
    for q, grp in full.groupby(["q"]):
        stats = grp.groupby("method").agg(
            S_mean=("S_test", "mean") if "S_test" in full.columns else ("S", "mean"),
            S_std=("S_test", "std") if "S_test" in full.columns else ("S", "std"),
            F12_mean=("F12", "mean") if "F12" in full.columns else ("seed", "size"),
            F12_std=("F12", "std") if "F12" in full.columns else ("seed", "size"),
            G12_mean=("G12", "mean") if "G12" in full.columns else ("seed", "size"),
            G12_std=("G12", "std") if "G12" in full.columns else ("seed", "size"),
        )
        if "Ensembleunar" not in stats.index:
            continue
        ens = stats.loc["Ensembleunar"]
        for other in unary_methods:
            if other == "Ensembleunar" or other not in stats.index:
                continue
            oth = stats.loc[other]
            rec = {"q": q, "other": other}
            for prefix in ["S", "F12", "G12"]:
                gap = float(ens[f"{prefix}_mean"] - oth[f"{prefix}_mean"])
                thr = ((float(ens[f"{prefix}_std"]) ** 2) + (float(oth[f"{prefix}_std"]) ** 2)) ** 0.5
                rec[f"{prefix}_gap"] = gap
                rec[f"{prefix}_thr"] = thr
                rec[f"{prefix}_status"] = (
                    "ensemble_win" if gap > thr else "other_win" if -gap > thr else "tie_noise"
                )
            unary_pairwise_rows.append(rec)
    if unary_pairwise_rows:
        unary_pairwise_df = pd.DataFrame(unary_pairwise_rows)
        for prefix in ["S", "F12", "G12"]:
            print(f"ENSEMBLE_VS_ALL_UNARY_COUNTS_{prefix}")
            print(
                unary_pairwise_df.groupby(["other", f"{prefix}_status"])
                .size()
                .rename("count")
                .reset_index()
                .to_csv(index=False)
            )
        print("ENSEMBLE_VS_ALL_UNARY_DETAILS")
        print(unary_pairwise_df.sort_values(["other", "q"]).to_csv(index=False))

        unary_avg_rows = []
        for q, grp in full.groupby(["q"]):
            stats = grp.groupby("method").agg(
                S_mean=("S_test", "mean") if "S_test" in full.columns else ("S", "mean"),
                S_std=("S_test", "std") if "S_test" in full.columns else ("S", "std"),
                F12_mean=("F12", "mean") if "F12" in full.columns else ("seed", "size"),
                F12_std=("F12", "std") if "F12" in full.columns else ("seed", "size"),
                G12_mean=("G12", "mean") if "G12" in full.columns else ("seed", "size"),
                G12_std=("G12", "std") if "G12" in full.columns else ("seed", "size"),
            )
            if "Ensembleunar" not in stats.index:
                continue
            ens = stats.loc["Ensembleunar"]
            for other in unary_methods:
                if other == "Ensembleunar" or other not in stats.index:
                    continue
                oth = stats.loc[other]
                rec = {"q": q, "other": other}
                for prefix in ["S", "F12", "G12"]:
                    gap = float(ens[f"{prefix}_mean"] - oth[f"{prefix}_mean"])
                    thr = (float(ens[f"{prefix}_std"]) + float(oth[f"{prefix}_std"])) / 2.0
                    rec[f"{prefix}_gap"] = gap
                    rec[f"{prefix}_thr_avg"] = thr
                    rec[f"{prefix}_status_avg"] = (
                        "ensemble_win" if gap > thr else "other_win" if -gap > thr else "tie_noise"
                    )
                unary_avg_rows.append(rec)
        if unary_avg_rows:
            unary_avg_df = pd.DataFrame(unary_avg_rows)
            for prefix in ["S", "F12", "G12"]:
                print(f"ENSEMBLE_VS_ALL_UNARY_AVGSTD_COUNTS_{prefix}")
                print(
                    unary_avg_df.groupby(["other", f"{prefix}_status_avg"])
                    .size()
                    .rename("count")
                    .reset_index()
                    .to_csv(index=False)
                )
            print("ENSEMBLE_VS_ALL_UNARY_AVGSTD_DETAILS")
            print(unary_avg_df.sort_values(["other", "q"]).to_csv(index=False))

    if {"method", "q"}.issubset(full.columns):
        group_cols = ["q"]
        maybe_model_col = "model" if "model" in full.columns else None
        if maybe_model_col:
            group_cols.append(maybe_model_col)
        elif "fold" in full.columns:
            group_cols.append("fold")

        numeric_cols = [c for c in ["S_test", "S", "F12", "G12"] if c in full.columns]
        if numeric_cols and {"Ensembleunar", "PLS"}.issubset(set(full["method"])):
            rows = []
            for key, grp in full.groupby(group_cols):
                stats = grp.groupby("method").agg({c: ["mean", "std"] for c in numeric_cols})
                if "Ensembleunar" not in stats.index or "PLS" not in stats.index:
                    continue
                rec = {}
                if isinstance(key, tuple):
                    for col, val in zip(group_cols, key):
                        rec[col] = val
                else:
                    rec[group_cols[0]] = key
                for c in numeric_cols:
                    ens_mean = float(stats.loc["Ensembleunar", (c, "mean")])
                    pls_mean = float(stats.loc["PLS", (c, "mean")])
                    ens_std = float(stats.loc["Ensembleunar", (c, "std")]) if pd.notna(stats.loc["Ensembleunar", (c, "std")]) else 0.0
                    pls_std = float(stats.loc["PLS", (c, "std")]) if pd.notna(stats.loc["PLS", (c, "std")]) else 0.0
                    thr = (ens_std**2 + pls_std**2) ** 0.5
                    gap = ens_mean - pls_mean
                    rec[f"{c}_gap"] = gap
                    rec[f"{c}_thr"] = thr
                    rec[f"{c}_status"] = (
                        "ensemble_win"
                        if gap > thr
                        else "pls_win"
                        if -gap > thr
                        else "tie_noise"
                    )
                rows.append(rec)
            unary_cmp = pd.DataFrame(rows)
            if not unary_cmp.empty:
                for c in numeric_cols:
                    print(f"ENSEMBLE_VS_PLS_UNARY_COUNTS_{c}")
                    print(
                        unary_cmp.groupby(f"{c}_status")
                        .size()
                        .rename("count")
                        .reset_index()
                        .to_csv(index=False)
                    )
                print("ENSEMBLE_VS_PLS_UNARY_DETAILS")
                print(unary_cmp.to_csv(index=False))


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: remote_partial_analysis.py <results_root>")
    root = Path(sys.argv[1])
    summarize_classical(root)
    summarize_unary(root)


if __name__ == "__main__":
    main()
