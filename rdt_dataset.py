from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold


DEFAULT_PUBLIC_URL = "https://disk.yandex.ru/d/GVZWXp3cwy0NmA"
DEFAULT_RAW_DIR = Path("data") / "rdt_raw"
DEFAULT_PROCESSED_DIR = Path("data") / "rdt_processed"
DEFAULT_LABELS = Path(r"C:\Users\nikit\Downloads\waterspoutCells.csv")

SENTINELS = ["", "-99999", "-99999.000000", "nan", "NaN", "NAN"]
MISSING_RATE_LIMIT = 0.80
CORR_DROP_THRESHOLD = 0.995
SENTINEL_ABS_LIMIT = 99990.0
PTROPO_ABS_LIMIT = 1_000_000.0
PHYSICAL_TEMP_ABS_LIMIT = 200.0
SURF_RATIO_MAX_VALID = 1.05

PIXEL_COL = "lon_lat_bt_ir108_pixels_bt_wv62_pixels_bt_wv73_pixels_bt_ir87_pixels_bt_ir120_pixels"
SURF_COL = "surf_t"
COMPLEX_COLS = {SURF_COL, PIXEL_COL}

ID_TIME_COLS = {"num", "dt", "fnum", "fdt", "bdt"}
COORD_COLS = {"pos_lat", "pos_lon"}
LEAKAGE_COLS = {"severity", "confidence_level"}
RAW_TECH_COLS = {"cbase", "maskconv", "indexconvval", "ctopp"}
LABEL_META_COLS = {"id_water", "label_bnum", "label_bdt"}
BUILD_META_COLS = {"source_file", "source_path", "date_folder", "file_dt"}

EXCLUDE_FROM_TRAINING = (
    ID_TIME_COLS
    | COORD_COLS
    | LEAKAGE_COLS
    | RAW_TECH_COLS
    | COMPLEX_COLS
    | LABEL_META_COLS
    | BUILD_META_COLS
    | {"y_exact", "positive_key"}
)

PREFERRED_CORR_REPRESENTATIVES = [
    "tmin",
    "tavg",
    "tth",
    "btd",
    "wbtd",
    "rdmax",
    "rdq1",
    "btd3max",
    "btd4max",
    "btd5max",
]

SURF_THRESHOLDS = [5, 0, -10, -20, -30, -40, -50]
BT_CHANNELS = ["bt_ir108", "bt_wv62", "bt_wv73", "bt_ir87", "bt_ir120"]
BT_DIFFS = {
    "diff_wv62_ir108": ("bt_wv62", "bt_ir108"),
    "diff_wv73_ir108": ("bt_wv73", "bt_ir108"),
    "diff_ir87_ir108": ("bt_ir87", "bt_ir108"),
    "diff_ir120_ir108": ("bt_ir120", "bt_ir108"),
    "diff_ir87_ir120": ("bt_ir87", "bt_ir120"),
}
PIXEL_STATS = ["mean", "std", "min", "max", "p10", "p25", "median", "p75", "p90"]


def _make_logger(log_file: str | Path | None = None):
    handle = None
    if log_file:
        path = _resolve_path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a", encoding="utf-8")

    def log(message: str) -> None:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{stamp}] {message}"
        print(line, flush=True)
        if handle is not None:
            handle.write(line + "\n")
            handle.flush()

    return log, handle


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return _project_root() / p


def _public_api_url(public_url: str, *, path: str | None = None, download: bool = False, limit: int = 1000, offset: int = 0) -> str:
    endpoint = "resources/download" if download else "resources"
    query = {
        "public_key": public_url,
    }
    if path is not None:
        query["path"] = path
    if not download:
        query["limit"] = str(limit)
        query["offset"] = str(offset)
    return "https://cloud-api.yandex.net/v1/disk/public/" + endpoint + "?" + urllib.parse.urlencode(query)


def _fetch_json(url: str, *, retries: int = 4, sleep_s: float = 1.0) -> dict:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "rdt-dataset-builder/1.0"})
            with urllib.request.urlopen(req, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - network path
            last_error = exc
            time.sleep(sleep_s * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_error}") from last_error


def _download_url(url: str, target: Path, *, retries: int = 4) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "rdt-dataset-builder/1.0"})
            with urllib.request.urlopen(req, timeout=180) as response, tmp.open("wb") as fh:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    fh.write(chunk)
            tmp.replace(target)
            return
        except Exception as exc:  # pragma: no cover - network path
            last_error = exc
            if tmp.exists():
                tmp.unlink()
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Failed to download {url}: {last_error}") from last_error


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_table(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        return path
    except Exception:
        csv_path = path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        return csv_path


def _read_table(path: Path) -> pd.DataFrame:
    if path.exists():
        if path.suffix == ".parquet":
            try:
                return pd.read_parquet(path)
            except Exception:
                pkl_path = path.with_suffix(".pkl")
                if pkl_path.exists():
                    return pd.read_pickle(pkl_path)
                csv_gz_path = path.with_suffix(".csv.gz")
                if csv_gz_path.exists():
                    return pd.read_csv(csv_gz_path)
                csv_path = path.with_suffix(".csv")
                if csv_path.exists():
                    return pd.read_csv(csv_path)
                raise
        if path.suffix == ".pkl":
            return pd.read_pickle(path)
        return pd.read_csv(path)
    pkl_path = path.with_suffix(".pkl")
    if pkl_path.exists():
        return pd.read_pickle(pkl_path)
    csv_gz_path = path.with_suffix(".csv.gz")
    if csv_gz_path.exists():
        return pd.read_csv(csv_gz_path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(path)


def _stat_timestamp_from_name(path: str | Path) -> str | None:
    name = Path(path).name
    m = re.search(r"RDT__(\d{12})_", name)
    return m.group(1) if m else None


def _normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["num", "dt", "fnum", "fdt", "bdt"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out


def read_stat_file(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    df = pd.read_csv(
        p,
        sep=";",
        na_values=SENTINELS,
        keep_default_na=True,
        low_memory=False,
    )
    df = _normalize_key_columns(df)
    df["source_file"] = p.name
    df["source_path"] = str(p)
    df["date_folder"] = p.parent.name
    df["file_dt"] = _stat_timestamp_from_name(p.name)
    return df


def _read_labels(labels_path: str | Path) -> pd.DataFrame:
    labels = pd.read_csv(labels_path, dtype=str).rename(columns={"bnum": "label_bnum", "bdt": "label_bdt"})
    for col in ["id_water", "num", "dt", "label_bnum", "label_bdt"]:
        if col in labels.columns:
            labels[col] = pd.to_numeric(labels[col], errors="coerce").astype("Int64")
    labels["positive_key"] = labels["dt"].astype("string") + "_" + labels["num"].astype("string")
    return labels


def parse_surf_t(value: object, surface: object = np.nan) -> dict[str, float]:
    result: dict[str, float] = {f"surf_area_le_{t}": np.nan for t in SURF_THRESHOLDS}
    result.update({f"surf_ratio_le_{t}": np.nan for t in SURF_THRESHOLDS})
    result.update({"surf_n_levels": 0.0, "surf_min_threshold": np.nan, "surf_max_threshold": np.nan})
    if pd.isna(value):
        return result
    pairs: list[tuple[float, float]] = []
    for token in str(value).split("@"):
        if not token:
            continue
        try:
            threshold, area = token.split("_", 1)
            pairs.append((float(threshold), float(area)))
        except ValueError:
            continue
    if not pairs:
        return result
    pairs.sort(key=lambda x: x[0], reverse=True)
    thresholds = [p[0] for p in pairs]
    result["surf_n_levels"] = float(len(pairs))
    result["surf_min_threshold"] = float(min(thresholds))
    result["surf_max_threshold"] = float(max(thresholds))
    lookup = dict(pairs)
    surface_float = pd.to_numeric(pd.Series([surface]), errors="coerce").iloc[0]
    for threshold in SURF_THRESHOLDS:
        area = lookup.get(float(threshold), np.nan)
        result[f"surf_area_le_{threshold}"] = area
        if pd.notna(area) and pd.notna(surface_float) and float(surface_float) > 0:
            result[f"surf_ratio_le_{threshold}"] = float(area) / float(surface_float)
    return result


def _empty_pixel_result() -> dict[str, float]:
    out = {
        "pixel_count_parsed": 0.0,
        "pixel_count_mismatch": np.nan,
    }
    for name in BT_CHANNELS + list(BT_DIFFS):
        for stat in PIXEL_STATS:
            out[f"pixel_{name}_{stat}"] = np.nan
    return out


def parse_pixels(value: object, expected_count: object = np.nan) -> dict[str, float]:
    result = _empty_pixel_result()
    if pd.isna(value):
        return result
    rows: list[list[float]] = []
    for token in str(value).split("@"):
        if not token:
            continue
        parts = token.split("_")
        if len(parts) != 7:
            continue
        try:
            rows.append([float(x) for x in parts])
        except ValueError:
            continue
    if not rows:
        return result
    arr = np.asarray(rows, dtype=float)
    result["pixel_count_parsed"] = float(arr.shape[0])
    expected = pd.to_numeric(pd.Series([expected_count]), errors="coerce").iloc[0]
    if pd.notna(expected):
        result["pixel_count_mismatch"] = float(arr.shape[0] - float(expected))
    data = pd.DataFrame(arr[:, 2:], columns=BT_CHANNELS)
    data = data.mask(data.abs() >= SENTINEL_ABS_LIMIT)
    for diff_name, (left, right) in BT_DIFFS.items():
        data[diff_name] = data[left] - data[right]
    for name in BT_CHANNELS + list(BT_DIFFS):
        series = data[name].dropna()
        if series.empty:
            continue
        result[f"pixel_{name}_mean"] = float(series.mean())
        result[f"pixel_{name}_std"] = float(series.std(ddof=0))
        result[f"pixel_{name}_min"] = float(series.min())
        result[f"pixel_{name}_max"] = float(series.max())
        result[f"pixel_{name}_p10"] = float(series.quantile(0.10))
        result[f"pixel_{name}_p25"] = float(series.quantile(0.25))
        result[f"pixel_{name}_median"] = float(series.median())
        result[f"pixel_{name}_p75"] = float(series.quantile(0.75))
        result[f"pixel_{name}_p90"] = float(series.quantile(0.90))
    return result


def build_complex_features(df: pd.DataFrame, *, chunk_size: int = 5000, log=None) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    total = len(df)
    for start in range(0, total, chunk_size):
        stop = min(start + chunk_size, total)
        chunk = df.iloc[start:stop]
        surf_rows = [
            parse_surf_t(v, s)
            for v, s in zip(
                chunk.get(SURF_COL, pd.Series(index=chunk.index)),
                chunk.get("surface", pd.Series(index=chunk.index)),
            )
        ]
        pixel_rows = [
            parse_pixels(v, n)
            for v, n in zip(
                chunk.get(PIXEL_COL, pd.Series(index=chunk.index)),
                chunk.get("num_pixels", pd.Series(index=chunk.index)),
            )
        ]
        parts.append(pd.concat([pd.DataFrame(surf_rows, index=chunk.index), pd.DataFrame(pixel_rows, index=chunk.index)], axis=1))
        if log is not None:
            log(f"parsed complex features rows {stop}/{total}")
    if not parts:
        return pd.DataFrame(index=df.index)
    return pd.concat(parts).sort_index()


@dataclass
class FeatureAudit:
    scalar_clean: list[str]
    complex_clean: list[str]
    all_no_coords: list[str]
    report: pd.DataFrame
    corr_report: pd.DataFrame


def _numeric_feature_frame(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    series = {col: pd.to_numeric(df[col], errors="coerce") for col in cols}
    if not series:
        return pd.DataFrame(index=df.index)
    return pd.DataFrame(series, index=df.index)


def _feature_priority(col: str, missing_rate: float) -> tuple[int, float, str]:
    if col in PREFERRED_CORR_REPRESENTATIVES:
        return (0, missing_rate, col)
    if col.endswith("_st"):
        return (3, missing_rate, col)
    if col.endswith("90") or col.endswith("q2"):
        return (2, missing_rate, col)
    if col.endswith("min") and col not in {"tmin", "wvmin", "wv2min", "ir39min", "ir87min", "ir120min"}:
        return (2, missing_rate, col)
    return (1, missing_rate, col)


def audit_feature_columns(cells: pd.DataFrame, complex_features: pd.DataFrame) -> FeatureAudit:
    scalar_candidates = [
        col
        for col in cells.columns
        if col not in EXCLUDE_FROM_TRAINING and col not in complex_features.columns
    ]
    scalar = _numeric_feature_frame(cells, scalar_candidates)
    complex_numeric = _numeric_feature_frame(complex_features, complex_features.columns)
    all_features = pd.concat([scalar, complex_numeric], axis=1)

    rows = []
    for col in all_features.columns:
        series = all_features[col]
        non_missing = series.dropna()
        missing_rate = float(series.isna().mean())
        unique_non_missing = int(non_missing.nunique(dropna=True))
        rows.append(
            {
                "feature": col,
                "group": "complex" if col in complex_numeric.columns else "scalar",
                "missing_rate": missing_rate,
                "unique_non_missing": unique_non_missing,
                "excluded_always": False,
                "drop_reason": "",
            }
        )
    report = pd.DataFrame(rows)
    if report.empty:
        return FeatureAudit([], [], [], report, pd.DataFrame())

    report.loc[report["missing_rate"] > MISSING_RATE_LIMIT, "drop_reason"] = "missing_rate_gt_0.80"
    report.loc[report["unique_non_missing"] <= 1, "drop_reason"] = "constant_or_empty"

    kept = report.loc[report["drop_reason"].eq(""), "feature"].tolist()
    missing_map = dict(zip(report["feature"], report["missing_rate"]))
    corr_drops: dict[str, str] = {}
    corr_rows = []
    if len(kept) > 1:
        corr = all_features[kept].corr(min_periods=max(20, min(100, len(all_features) // 5))).abs()
        for i, left in enumerate(kept):
            if left in corr_drops:
                continue
            for right in kept[i + 1 :]:
                if right in corr_drops:
                    continue
                value = corr.loc[left, right]
                if pd.isna(value) or value < CORR_DROP_THRESHOLD:
                    continue
                ordered = sorted([left, right], key=lambda c: _feature_priority(c, missing_map.get(c, 1.0)))
                keep, drop = ordered[0], ordered[1]
                corr_drops[drop] = f"corr_ge_0.995_with:{keep}"
                corr_rows.append({"dropped": drop, "kept": keep, "abs_corr": float(value)})
    for col, reason in corr_drops.items():
        report.loc[report["feature"].eq(col), "drop_reason"] = reason

    scalar_clean = report.loc[(report["group"].eq("scalar")) & (report["drop_reason"].eq("")), "feature"].tolist()
    complex_clean = report.loc[(report["group"].eq("complex")) & (report["drop_reason"].eq("")), "feature"].tolist()

    all_no_coords_candidates = [
        col
        for col in cells.columns
        if col not in (ID_TIME_COLS | COORD_COLS | COMPLEX_COLS | BUILD_META_COLS | LABEL_META_COLS | {"y_exact", "positive_key"})
    ]
    all_no_coords = []
    for col in all_no_coords_candidates:
        numeric = pd.to_numeric(cells[col], errors="coerce")
        if numeric.dropna().nunique() > 1:
            all_no_coords.append(col)

    return FeatureAudit(
        scalar_clean=scalar_clean,
        complex_clean=complex_clean,
        all_no_coords=all_no_coords,
        report=report.sort_values(["drop_reason", "group", "feature"]).reset_index(drop=True),
        corr_report=pd.DataFrame(corr_rows),
    )


def _attach_labels(cells: pd.DataFrame, labels: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = cells.copy()
    out["positive_key"] = out["dt"].astype("string") + "_" + out["num"].astype("string")
    positive_keys = set(labels["positive_key"].dropna().astype(str))
    out["y_exact"] = out["positive_key"].isin(positive_keys).astype(int)
    label_meta = labels[["positive_key", "id_water", "label_bnum", "label_bdt"]].drop_duplicates("positive_key")
    out = out.merge(label_meta, on="positive_key", how="left")

    counts = out.groupby("positive_key", dropna=False).size().rename("matches").reset_index()
    label_report = labels.merge(counts, on="positive_key", how="left")
    label_report["matches"] = label_report["matches"].fillna(0).astype(int)
    return out, label_report


def _imbalance_report(cells: pd.DataFrame, label_report: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total = int(len(cells))
    positives = int(cells["y_exact"].sum())
    negatives = total - positives
    rows.append(
        {
            "section": "overall",
            "key": "all",
            "rows": total,
            "positives": positives,
            "negatives": negatives,
            "positive_rate": positives / total if total else np.nan,
            "negative_positive_ratio": negatives / positives if positives else np.inf,
        }
    )
    for group in ["date_folder", "source_file", "categ"]:
        if group not in cells.columns:
            continue
        grouped = cells.groupby(group, dropna=False)["y_exact"].agg(["count", "sum"]).reset_index()
        for _, row in grouped.iterrows():
            count = int(row["count"])
            pos = int(row["sum"])
            rows.append(
                {
                    "section": group,
                    "key": row[group],
                    "rows": count,
                    "positives": pos,
                    "negatives": count - pos,
                    "positive_rate": pos / count if count else np.nan,
                    "negative_positive_ratio": (count - pos) / pos if pos else np.inf,
                }
            )
    labels_total = int(len(label_report))
    labels_found_once = int((label_report["matches"] == 1).sum()) if not label_report.empty else 0
    labels_missing = int((label_report["matches"] == 0).sum()) if not label_report.empty else 0
    labels_duplicate = int((label_report["matches"] > 1).sum()) if not label_report.empty else 0
    rows.extend(
        [
            {"section": "labels", "key": "labels_total", "rows": labels_total},
            {"section": "labels", "key": "labels_found_once", "rows": labels_found_once},
            {"section": "labels", "key": "labels_missing", "rows": labels_missing},
            {"section": "labels", "key": "labels_duplicate", "rows": labels_duplicate},
        ]
    )
    if "date_folder" in cells.columns and cells["y_exact"].nunique() > 1 and cells["date_folder"].nunique() >= 2:
        n_splits = min(5, cells["date_folder"].nunique())
        if n_splits >= 2:
            groups = cells["date_folder"].astype(str).to_numpy()
            y = cells["y_exact"].to_numpy()
            for fold, (_, test_idx) in enumerate(GroupKFold(n_splits=n_splits).split(cells, y, groups=groups), start=1):
                fold_total = int(len(test_idx))
                fold_pos = int(y[test_idx].sum())
                rows.append(
                    {
                        "section": "groupkfold_date",
                        "key": f"fold_{fold:02d}",
                        "rows": fold_total,
                        "positives": fold_pos,
                        "negatives": fold_total - fold_pos,
                        "positive_rate": fold_pos / fold_total if fold_total else np.nan,
                        "negative_positive_ratio": (fold_total - fold_pos) / fold_pos if fold_pos else np.inf,
                    }
                )
    return pd.DataFrame(rows)


def _feature_groups_ablation(feature_sets: dict[str, list[str]]) -> pd.DataFrame:
    rows = []
    for name, cols in feature_sets.items():
        rows.append({"feature_set": name, "n_features": len(cols), "features": ",".join(cols)})
    return pd.DataFrame(rows)


def _primary_feature_names(processed_dir: Path) -> list[str]:
    groups_path = processed_dir / "feature_groups_ablation.csv"
    groups = pd.read_csv(groups_path)
    row = groups.loc[groups["feature_set"].eq("primary")]
    if row.empty:
        raise KeyError(f"primary feature set is missing in {groups_path}")
    features = str(row.iloc[0]["features"]).split(",")
    return [feature for feature in features if feature]


def _is_structural_surf_feature(feature: str) -> bool:
    return feature.startswith("surf_area_le_") or feature.startswith("surf_ratio_le_")


def _allows_large_positive_values(feature: str) -> bool:
    return feature in {"surface", "surface_st", "num_pixels"} or feature.startswith("surf_area_le_")


def _is_temperature_like_feature(feature: str) -> bool:
    if feature.startswith("pixel_bt_") or feature.startswith("pixel_diff_"):
        return True
    return feature.startswith(
        (
            "tth",
            "tmin",
            "tavg",
            "wv",
            "wv2",
            "btd",
            "wbtd",
            "v06",
            "ir16",
            "ir39",
            "ir87",
            "ir120",
            "rd",
        )
    )


def _clean_training_feature_series(
    feature: str,
    series: pd.Series,
    frame: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    clean = pd.to_numeric(series, errors="coerce")
    values = clean.to_numpy(dtype="float64", copy=False)
    invalid = pd.Series(clean.notna().to_numpy() & ~np.isfinite(values), index=clean.index)

    abs_values = clean.abs()
    if not _allows_large_positive_values(feature):
        invalid |= clean.notna() & (abs_values >= SENTINEL_ABS_LIMIT)
    else:
        invalid |= clean.notna() & (clean <= -SENTINEL_ABS_LIMIT)

    if _is_temperature_like_feature(feature):
        invalid |= clean.notna() & (abs_values > PHYSICAL_TEMP_ABS_LIMIT)
    if feature == "ptropo":
        invalid |= clean.notna() & (abs_values > PTROPO_ABS_LIMIT)
    if feature == "categ_age":
        invalid |= clean.notna() & (clean < 0)
    if feature.startswith("surf_ratio_le_"):
        invalid |= clean.notna() & ((clean < 0) | (clean > SURF_RATIO_MAX_VALID))
    if feature.startswith("surf_area_le_") and "surface" in frame.columns:
        surface = pd.to_numeric(frame["surface"], errors="coerce")
        invalid |= clean.notna() & surface.notna() & (surface > 0) & (clean > surface * SURF_RATIO_MAX_VALID)

    if invalid.any():
        clean = clean.mask(invalid)
    return clean, invalid


def _postprocess_training_feature_values(feature: str, series: pd.Series, frame: pd.DataFrame) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce")
    if feature.startswith("surf_ratio_le_"):
        return clean.clip(lower=0.0, upper=1.0)
    if feature.startswith("surf_area_le_") and "surface" in frame.columns:
        surface = pd.to_numeric(frame["surface"], errors="coerce")
        return clean.mask(clean.notna() & surface.notna() & (surface > 0) & (clean > surface), surface)
    return clean


def _enforce_surf_consistency(frame: pd.DataFrame) -> pd.DataFrame:
    area_cols = [f"surf_area_le_{threshold}" for threshold in SURF_THRESHOLDS if f"surf_area_le_{threshold}" in frame.columns]
    ratio_cols = [f"surf_ratio_le_{threshold}" for threshold in SURF_THRESHOLDS if f"surf_ratio_le_{threshold}" in frame.columns]
    if not area_cols or "surface" not in frame.columns:
        return frame

    out = frame.copy()
    areas = out[area_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    surface = pd.to_numeric(out["surface"], errors="coerce").to_numpy(dtype=float)
    valid_surface = np.isfinite(surface) & (surface > 0)

    areas = np.where(np.isfinite(areas), areas, 0.0)
    areas = np.maximum(areas, 0.0)
    if valid_surface.any():
        areas[valid_surface] = np.minimum(areas[valid_surface], surface[valid_surface, None])

    # SURF_THRESHOLDS are ordered warm to cold. Cumulative area at a warmer
    # threshold must include every colder-threshold area.
    for idx in range(len(area_cols) - 2, -1, -1):
        areas[:, idx] = np.maximum(areas[:, idx], areas[:, idx + 1])
    if valid_surface.any():
        areas[valid_surface] = np.minimum(areas[valid_surface], surface[valid_surface, None])

    out.loc[:, area_cols] = areas
    for area_col, ratio_col in zip(area_cols, ratio_cols):
        ratio = np.zeros(len(out), dtype=float)
        ratio[valid_surface] = areas[valid_surface, area_cols.index(area_col)] / surface[valid_surface]
        out.loc[:, ratio_col] = np.clip(ratio, 0.0, 1.0)
    return out


def _enforce_pixel_stat_consistency(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    stat_order = ["min", "p10", "p25", "median", "p75", "p90", "max"]
    prefixes = [f"pixel_{channel}" for channel in BT_CHANNELS] + [f"pixel_{diff}" for diff in BT_DIFFS]
    for prefix in prefixes:
        ordered_cols = [f"{prefix}_{stat}" for stat in stat_order if f"{prefix}_{stat}" in out.columns]
        if len(ordered_cols) >= 2:
            values = out[ordered_cols].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
            values = np.sort(values, axis=1)
            out.loc[:, ordered_cols] = values

        min_col = f"{prefix}_min"
        max_col = f"{prefix}_max"
        mean_col = f"{prefix}_mean"
        if min_col in out.columns and max_col in out.columns and mean_col in out.columns:
            min_values = pd.to_numeric(out[min_col], errors="coerce")
            max_values = pd.to_numeric(out[max_col], errors="coerce")
            mean_values = pd.to_numeric(out[mean_col], errors="coerce")
            out.loc[:, mean_col] = mean_values.clip(lower=min_values, upper=max_values)
        p10_col = f"{prefix}_p10"
        p90_col = f"{prefix}_p90"
        if p10_col in out.columns and p90_col in out.columns and mean_col in out.columns:
            p10_values = pd.to_numeric(out[p10_col], errors="coerce")
            p90_values = pd.to_numeric(out[p90_col], errors="coerce")
            mean_values = pd.to_numeric(out[mean_col], errors="coerce")
            out.loc[:, mean_col] = mean_values.clip(lower=p10_values, upper=p90_values)

        std_col = f"{prefix}_std"
        if std_col in out.columns:
            out.loc[:, std_col] = pd.to_numeric(out[std_col], errors="coerce").clip(lower=0.0)
    return out


def repair_training_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return _enforce_pixel_stat_consistency(_enforce_surf_consistency(frame))


def repair_training_matrix(X: np.ndarray, feature_names: list[str]) -> np.ndarray:
    frame = pd.DataFrame(np.asarray(X, dtype=float), columns=feature_names)
    repaired = repair_training_frame(frame)
    return repaired[feature_names].to_numpy(dtype=float)


def impute_primary_dataset(processed_dir: Path, output_path: Path | None = None) -> dict[str, Path]:
    processed_dir = _resolve_path(processed_dir)
    output_path = _resolve_path(output_path or (processed_dir / "dataset_final.parquet"))
    dataset = _read_table(processed_dir / "dataset_primary.parquet")
    feature_names = _primary_feature_names(processed_dir)
    missing_features = [feature for feature in feature_names if dataset[feature].isna().any()]

    imputed = dataset.copy()
    cleaned = dataset.copy()
    report_rows = []
    added_indicators: list[str] = []
    indicator_data: dict[str, pd.Series] = {}
    n_rows = len(imputed)
    for feature in feature_names:
        raw_series = pd.to_numeric(imputed[feature], errors="coerce")
        raw_missing_count = int(raw_series.isna().sum())
        series, invalid_mask = _clean_training_feature_series(feature, raw_series, imputed)
        invalid_count = int(invalid_mask.sum())
        missing_mask = series.isna()
        missing_count = int(missing_mask.sum())
        missing_rate = float(missing_count / n_rows) if n_rows else 0.0
        if _is_structural_surf_feature(feature):
            fill_value = 0.0
            cleaned[feature] = _postprocess_training_feature_values(feature, series.fillna(fill_value), cleaned)
            imputed[feature] = _postprocess_training_feature_values(feature, series.fillna(fill_value), imputed)
            strategy = "zero_structural_no_cold_area"
            indicator = ""
        elif missing_count:
            fill_value = float(series.median(skipna=True))
            if math.isnan(fill_value):
                fill_value = 0.0
            cleaned[feature] = _postprocess_training_feature_values(feature, series, cleaned)
            imputed[feature] = _postprocess_training_feature_values(feature, series.fillna(fill_value), imputed)
            indicator = f"missing__{feature}"
            indicator_data[indicator] = missing_mask.astype(np.int8)
            added_indicators.append(indicator)
            strategy = "median_plus_missing_indicator"
        else:
            fill_value = np.nan
            cleaned[feature] = _postprocess_training_feature_values(feature, series, cleaned)
            imputed[feature] = _postprocess_training_feature_values(feature, series, imputed)
            strategy = "none_no_missing"
            indicator = ""
        report_rows.append(
            {
                "feature": feature,
                "raw_missing_count": raw_missing_count,
                "invalid_count": invalid_count,
                "missing_count": missing_count,
                "missing_rate": missing_rate,
                "strategy": strategy,
                "fill_value": fill_value,
                "indicator_added": indicator,
            }
        )

    if indicator_data:
        imputed = pd.concat([imputed, pd.DataFrame(indicator_data, index=imputed.index)], axis=1)

    cleaned = _enforce_surf_consistency(cleaned)
    imputed = _enforce_surf_consistency(imputed)
    imputed = _enforce_pixel_stat_consistency(imputed)

    final_feature_names = feature_names + added_indicators
    meta_cols = [col for col in imputed.columns if col not in feature_names and not col.startswith("missing__")]
    clean_df = cleaned[meta_cols + feature_names]
    final_df = imputed[meta_cols + final_feature_names]
    compact_df = imputed[meta_cols + feature_names]
    outputs = {
        "dataset_clean_85": _write_table(clean_df, processed_dir / "dataset_clean_85.parquet"),
        "dataset_final": _write_table(final_df, output_path),
        "dataset_final_85": _write_table(compact_df, processed_dir / "dataset_final_85.parquet"),
        "imputation_report": processed_dir / "imputation_report.csv",
        "clean_features_85": processed_dir / "clean_feature_list_85.csv",
        "final_features": processed_dir / "final_feature_list.csv",
        "final_features_85": processed_dir / "final_feature_list_85.csv",
    }
    pd.DataFrame(report_rows).to_csv(outputs["imputation_report"], index=False)
    pd.DataFrame({"feature": feature_names, "kind": ["clean_pre_fold_imputation"] * len(feature_names)}).to_csv(
        outputs["clean_features_85"],
        index=False,
    )
    pd.DataFrame(
        {
            "feature": final_feature_names,
            "kind": ["original"] * len(feature_names) + ["missing_indicator"] * len(added_indicators),
        }
    ).to_csv(outputs["final_features"], index=False)
    pd.DataFrame({"feature": feature_names, "kind": ["original_imputed"] * len(feature_names)}).to_csv(
        outputs["final_features_85"],
        index=False,
    )
    return outputs


def _collect_stat_files(raw_dir: Path) -> list[Path]:
    return sorted(raw_dir.rglob("*.stat"))


def discover_public_stat_files(public_url: str = DEFAULT_PUBLIC_URL) -> list[dict[str, object]]:
    root = _fetch_json(_public_api_url(public_url))
    stack = [item["path"] for item in root.get("_embedded", {}).get("items", []) if item.get("type") == "dir"]
    files: list[dict[str, object]] = []
    while stack:
        folder = str(stack.pop())
        offset = 0
        while True:
            data = _fetch_json(_public_api_url(public_url, path=folder, limit=1000, offset=offset))
            embedded = data.get("_embedded", {})
            items = embedded.get("items", [])
            for item in items:
                if item.get("type") == "dir":
                    stack.append(str(item["path"]))
                elif item.get("type") == "file" and str(item.get("name", "")).endswith(".stat"):
                    files.append(item)
            offset += len(items)
            if offset >= int(embedded.get("total", len(items))):
                break
    return sorted(files, key=lambda item: str(item.get("path", "")))


def download_dataset(public_url: str, raw_dir: Path, manifest_path: Path | None = None) -> pd.DataFrame:
    raw_dir = _resolve_path(raw_dir)
    manifest_path = _resolve_path(manifest_path or raw_dir / "download_manifest.csv")
    records = []
    files = discover_public_stat_files(public_url)
    for item in files:
        public_path = str(item["path"])
        date_folder = Path(public_path).parent.name
        local_path = raw_dir / date_folder / str(item["name"])
        expected_size = int(item.get("size") or 0)
        status = "downloaded"
        if local_path.exists() and expected_size and local_path.stat().st_size == expected_size:
            status = "skipped_existing"
        else:
            data = _fetch_json(_public_api_url(public_url, path=public_path, download=True))
            _download_url(str(data["href"]), local_path)
        actual_size = local_path.stat().st_size if local_path.exists() else 0
        sha = _sha256(local_path) if local_path.exists() else ""
        records.append(
            {
                "public_path": public_path,
                "local_path": str(local_path),
                "name": item.get("name"),
                "size_expected": expected_size,
                "size_actual": actual_size,
                "modified": item.get("modified"),
                "sha256": sha,
                "status": status if actual_size == expected_size or not expected_size else "size_mismatch",
            }
        )
        pd.DataFrame(records).to_csv(manifest_path, index=False)
    return pd.DataFrame(records)


def build_dataset(
    raw_dir: Path,
    labels_path: Path,
    processed_dir: Path,
    *,
    log_file: str | Path | None = None,
    complex_chunk_size: int = 5000,
) -> dict[str, Path]:
    log, log_handle = _make_logger(log_file)
    started = time.time()
    raw_dir = _resolve_path(raw_dir)
    processed_dir = _resolve_path(processed_dir)
    labels_path = _resolve_path(labels_path) if not Path(labels_path).is_absolute() else Path(labels_path)
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        stat_files = _collect_stat_files(raw_dir)
        if not stat_files:
            raise FileNotFoundError(f"No .stat files found under {raw_dir}")
        log(f"build start raw_dir={raw_dir} files={len(stat_files)} processed_dir={processed_dir}")

        frames = []
        rows_read = 0
        for idx, path in enumerate(stat_files, start=1):
            frame = read_stat_file(path)
            rows_read += len(frame)
            frames.append(frame)
            if idx == 1 or idx % 100 == 0 or idx == len(stat_files):
                log(f"read stat files {idx}/{len(stat_files)} rows={rows_read}")
        cells = pd.concat(frames, ignore_index=True)
        del frames
        log(f"concatenated cells rows={len(cells)} cols={cells.shape[1]}")

        labels = _read_labels(labels_path)
        cells, label_report = _attach_labels(cells, labels)
        log(f"attached labels positives={int(cells['y_exact'].sum())} labels={len(labels)}")

        outputs: dict[str, Path] = {}
        outputs["cells_raw"] = _write_table(cells, processed_dir / "cells_raw.parquet")
        label_report_path = processed_dir / "label_match_report.csv"
        label_report.to_csv(label_report_path, index=False)
        outputs["label_match_report"] = label_report_path
        log(f"wrote cells_raw={outputs['cells_raw']} label_report={label_report_path}")

        complex_features = build_complex_features(cells, chunk_size=complex_chunk_size, log=log)
        outputs["features_complex_agg"] = _write_table(complex_features, processed_dir / "features_complex_agg.parquet")
        log(f"wrote complex features shape={complex_features.shape} path={outputs['features_complex_agg']}")

        log("running feature audit")
        audit = audit_feature_columns(cells, complex_features)
        scalar_features = _numeric_feature_frame(cells, audit.scalar_clean)
        complex_clean = _numeric_feature_frame(complex_features, audit.complex_clean)
        all_no_coords = _numeric_feature_frame(cells, audit.all_no_coords)
        log(
            "selected feature sets "
            f"scalar_clean={len(audit.scalar_clean)} complex_clean={len(audit.complex_clean)} "
            f"primary={len(audit.scalar_clean) + len(audit.complex_clean)} all_no_coords={len(audit.all_no_coords)}"
        )

        meta_cols = [
            col
            for col in list(ID_TIME_COLS) + list(COORD_COLS) + list(BUILD_META_COLS) + list(LABEL_META_COLS)
            if col in cells.columns
        ]
        dataset_primary = pd.concat([cells[meta_cols + ["y_exact"]].reset_index(drop=True), scalar_features, complex_clean], axis=1)

        feature_sets = {
            "scalar_clean": list(scalar_features.columns),
            "complex_only": list(complex_clean.columns),
            "primary": list(scalar_features.columns) + list(complex_clean.columns),
            "all_no_coords": list(all_no_coords.columns),
        }

        outputs["features_scalar"] = _write_table(scalar_features, processed_dir / "features_scalar.parquet")
        outputs["dataset_primary"] = _write_table(dataset_primary, processed_dir / "dataset_primary.parquet")
        outputs["feature_report"] = processed_dir / "feature_report.csv"
        outputs["feature_corr_report"] = processed_dir / "feature_corr_report.csv"
        outputs["imbalance_report"] = processed_dir / "imbalance_report.csv"
        outputs["feature_groups_ablation"] = processed_dir / "feature_groups_ablation.csv"
        audit.report.to_csv(outputs["feature_report"], index=False)
        audit.corr_report.to_csv(outputs["feature_corr_report"], index=False)
        _imbalance_report(cells, label_report).to_csv(outputs["imbalance_report"], index=False)
        _feature_groups_ablation(feature_sets).to_csv(outputs["feature_groups_ablation"], index=False)
        log(f"build done elapsed_s={time.time() - started:.1f}")
        return outputs
    finally:
        if log_handle is not None:
            log_handle.close()


def audit_processed(processed_dir: Path) -> dict[str, Path]:
    processed_dir = _resolve_path(processed_dir)
    cells = _read_table(processed_dir / "cells_raw.parquet")
    complex_features = _read_table(processed_dir / "features_complex_agg.parquet")
    label_report = pd.read_csv(processed_dir / "label_match_report.csv")
    audit = audit_feature_columns(cells, complex_features)
    outputs = {
        "feature_report": processed_dir / "feature_report.csv",
        "feature_corr_report": processed_dir / "feature_corr_report.csv",
        "imbalance_report": processed_dir / "imbalance_report.csv",
    }
    audit.report.to_csv(outputs["feature_report"], index=False)
    audit.corr_report.to_csv(outputs["feature_corr_report"], index=False)
    _imbalance_report(cells, label_report).to_csv(outputs["imbalance_report"], index=False)
    return outputs


def load_rdt_waterspout(
    processed_dir: str | Path = DEFAULT_PROCESSED_DIR,
    *,
    feature_set: str = "clean_85",
    return_feature_names: bool = False,
    return_groups: bool = False,
    group_by: str = "date_folder",
) -> tuple:
    processed_dir = _resolve_path(processed_dir)
    if feature_set == "primary":
        df = _read_table(processed_dir / "dataset_primary.parquet")
        feature_cols = [col for col in df.columns if col not in EXCLUDE_FROM_TRAINING]
    elif feature_set == "final":
        df = _read_table(processed_dir / "dataset_final.parquet")
        features_path = processed_dir / "final_feature_list.csv"
        if features_path.exists():
            feature_cols = pd.read_csv(features_path)["feature"].tolist()
        else:
            feature_cols = [col for col in df.columns if col not in EXCLUDE_FROM_TRAINING]
    elif feature_set in {"final_85", "final_compact"}:
        df = _read_table(processed_dir / "dataset_final_85.parquet")
        features_path = processed_dir / "final_feature_list_85.csv"
        if features_path.exists():
            feature_cols = pd.read_csv(features_path)["feature"].tolist()
        else:
            feature_cols = [col for col in df.columns if col not in EXCLUDE_FROM_TRAINING]
    elif feature_set in {"clean_85", "clean_preimpute"}:
        df = _read_table(processed_dir / "dataset_clean_85.parquet")
        features_path = processed_dir / "clean_feature_list_85.csv"
        if features_path.exists():
            feature_cols = pd.read_csv(features_path)["feature"].tolist()
        else:
            feature_cols = [col for col in df.columns if col not in EXCLUDE_FROM_TRAINING]
    elif feature_set == "scalar_clean":
        df = _read_table(processed_dir / "dataset_primary.parquet")
        report = pd.read_csv(processed_dir / "feature_report.csv")
        feature_cols = report.loc[(report["group"] == "scalar") & (report["drop_reason"].fillna("") == ""), "feature"].tolist()
    elif feature_set == "complex_only":
        primary = _read_table(processed_dir / "dataset_primary.parquet")
        df = primary
        report = pd.read_csv(processed_dir / "feature_report.csv")
        feature_cols = report.loc[(report["group"] == "complex") & (report["drop_reason"].fillna("") == ""), "feature"].tolist()
    elif feature_set == "all_no_coords":
        cells = _read_table(processed_dir / "cells_raw.parquet")
        complex_features = _read_table(processed_dir / "features_complex_agg.parquet")
        audit = audit_feature_columns(cells, complex_features)
        df = cells
        feature_cols = audit.all_no_coords
    else:
        raise ValueError(
            "feature_set must be one of: primary, final, final_85, final_compact, "
            "clean_85, clean_preimpute, scalar_clean, complex_only, all_no_coords"
        )
    if "y_exact" not in df.columns:
        raise KeyError("Processed dataset does not include y_exact")
    X_df = _numeric_feature_frame(df, feature_cols)
    if not feature_cols:
        raise ValueError(f"No features selected for feature_set={feature_set!r}")
    X = X_df.to_numpy(dtype=float)
    y = df["y_exact"].to_numpy(dtype=int)
    if return_groups:
        if group_by not in df.columns:
            raise KeyError(f"group_by={group_by!r} is not present in processed dataset")
        groups = df[group_by].astype(str).to_numpy()
    else:
        groups = None
    if return_feature_names:
        if return_groups:
            return X.astype(np.float64), y.astype(int), groups, feature_cols
        return X.astype(np.float64), y.astype(int), feature_cols
    if return_groups:
        return X.astype(np.float64), y.astype(int), groups
    return X.astype(np.float64), y.astype(int)


def _copy_smoke_inputs(smoke_raw_dir: Path, stat_files: list[Path]) -> None:
    for src in stat_files:
        if not src.exists():
            continue
        file_dt = _stat_timestamp_from_name(src.name)
        date_folder = file_dt[:8] if file_dt else src.parent.name
        dst = smoke_raw_dir / date_folder / src.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists() or dst.stat().st_size != src.stat().st_size:
            dst.write_bytes(src.read_bytes())


def run_smoke(raw_dir: Path, processed_dir: Path, labels_path: Path, stat_files: list[Path] | None = None) -> None:
    raw_dir = _resolve_path(raw_dir)
    processed_dir = _resolve_path(processed_dir)
    labels_path = Path(labels_path)
    if stat_files:
        _copy_smoke_inputs(raw_dir, [Path(p) for p in stat_files])
    outputs = build_dataset(raw_dir, labels_path, processed_dir)
    cells = _read_table(outputs["cells_raw"])
    primary = _read_table(outputs["dataset_primary"])
    assert len(cells) > 0, "empty cells_raw"
    assert "y_exact" in primary.columns, "missing target"
    target = cells[(cells["dt"].astype("Int64") == 201405310630) & (cells["num"].astype("Int64") == 74)]
    assert not target.empty, "label dt=201405310630 num=74 not found in smoke data"
    assert int(target["y_exact"].iloc[0]) == 1, "smoke label did not map to y=1"
    forbidden_exact = {"dt", "num", "fdt", "bdt", "pos_lat", "pos_lon"}
    feature_cols = [col for col in primary.columns if col not in EXCLUDE_FROM_TRAINING]
    bad = [
        col
        for col in feature_cols
        if col in forbidden_exact or col.lower().endswith("_lat") or col.lower().endswith("_lon")
    ]
    assert not bad, f"training features contain forbidden columns: {bad}"
    X, y, feature_names = load_rdt_waterspout(processed_dir, return_feature_names=True)
    assert X.shape[0] == primary.shape[0], "X row count mismatch"
    assert X.shape[1] == len(feature_names), "feature name count mismatch"
    assert np.isfinite(X[~np.isnan(X)]).all(), "X contains non-finite values other than NaN"
    assert int(y.sum()) >= 1, "no positives in smoke y"
    from reducers import HSICSelector, PCAReducer, PLSReducer

    q = min(2, X.shape[1], max(1, X.shape[0] - 1))
    X_fit = pd.DataFrame(X, columns=feature_names).fillna(pd.DataFrame(X, columns=feature_names).median()).to_numpy(dtype=float)
    X_fit = repair_training_matrix(X_fit, feature_names)
    PCAReducer(q).fit_transform(X_fit, y)
    if len(np.unique(y)) > 1:
        PLSReducer(q).fit_transform(X_fit, y)
        HSICSelector(q).fit_transform(X_fit, y)
    print(f"smoke ok: rows={X.shape[0]} features={X.shape[1]} positives={int(y.sum())}")


def _cmd_download(args: argparse.Namespace) -> int:
    manifest = download_dataset(args.public_url, Path(args.raw_dir), Path(args.manifest) if args.manifest else None)
    print(f"download manifest rows={len(manifest)}")
    return 0


def _cmd_build(args: argparse.Namespace) -> int:
    outputs = build_dataset(
        Path(args.raw_dir),
        Path(args.labels),
        Path(args.processed_dir),
        log_file=args.log_file,
        complex_chunk_size=args.complex_chunk_size,
    )
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


def _cmd_audit(args: argparse.Namespace) -> int:
    outputs = audit_processed(Path(args.processed_dir))
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


def _cmd_finalize(args: argparse.Namespace) -> int:
    outputs = impute_primary_dataset(Path(args.processed_dir), Path(args.output) if args.output else None)
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


def _cmd_smoke(args: argparse.Namespace) -> int:
    run_smoke(
        Path(args.raw_dir),
        Path(args.processed_dir),
        Path(args.labels),
        stat_files=[Path(p) for p in args.stat_file],
    )
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare SAFNWC RDT waterspout dataset.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_download = sub.add_parser("download", help="Download all .stat files from a public Yandex Disk folder.")
    p_download.add_argument("--public-url", default=DEFAULT_PUBLIC_URL)
    p_download.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    p_download.add_argument("--manifest", default=None)
    p_download.set_defaults(func=_cmd_download)

    p_build = sub.add_parser("build", help="Build processed parquet/csv artifacts from raw .stat files.")
    p_build.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    p_build.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR))
    p_build.add_argument("--labels", default=str(DEFAULT_LABELS))
    p_build.add_argument("--log-file", default=str(DEFAULT_PROCESSED_DIR / "build.log"))
    p_build.add_argument("--complex-chunk-size", type=int, default=5000)
    p_build.set_defaults(func=_cmd_build)

    p_audit = sub.add_parser("audit", help="Recompute audit reports for processed artifacts.")
    p_audit.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR))
    p_audit.set_defaults(func=_cmd_audit)

    p_finalize = sub.add_parser("finalize", help="Create final imputed training dataset from processed primary features.")
    p_finalize.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR))
    p_finalize.add_argument("--output", default=None)
    p_finalize.set_defaults(func=_cmd_finalize)

    p_smoke = sub.add_parser("smoke", help="Run local small checks on selected .stat files.")
    p_smoke.add_argument("--raw-dir", default=str(Path("data") / "rdt_smoke_raw"))
    p_smoke.add_argument("--processed-dir", default=str(Path("data") / "rdt_smoke_processed"))
    p_smoke.add_argument("--labels", default=str(DEFAULT_LABELS))
    p_smoke.add_argument("--stat-file", action="append", default=[])
    p_smoke.set_defaults(func=_cmd_smoke)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
