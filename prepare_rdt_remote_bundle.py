from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "dist" / "rdt_remote_bundle"

FILES_TO_COPY = [
    "datasets.py",
    "evaluation.py",
    "nirs_core.py",
    "pipelines.py",
    "reducers.py",
    "reporting.py",
    "unary.py",
    "rdt_dataset.py",
    "run_rdt_search_ablation_q20.py",
    "run_rdt_search_ablation_q20_remote.sh",
    "run_multi_seed_RDT_q20_remote.py",
    "run_multi_seed_RDT_q20_remote.sh",
    "data/rdt_processed/dataset_clean_85.parquet",
    "data/rdt_processed/clean_feature_list_85.csv",
    "data/rdt_processed/feature_groups_ablation.csv",
    "data/rdt_processed/feature_report.csv",
    "data/rdt_processed/imbalance_report.csv",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, object]] = []
    for rel in FILES_TO_COPY:
        src = ROOT / rel
        if not src.exists():
            raise FileNotFoundError(src)
        dst = OUT_DIR / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        manifest.append(
            {
                "path": rel,
                "bytes": src.stat().st_size,
                "sha256": _sha256(src),
            }
        )

    with open(OUT_DIR / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    with open(OUT_DIR / "README.txt", "w", encoding="utf-8") as f:
        f.write(
            "RDT remote bundle prepared locally.\n"
            "Upload this directory to the server without raw .stat files.\n"
            "Runtime entrypoints:\n"
            "  - run_rdt_search_ablation_q20.py\n"
            "  - run_rdt_search_ablation_q20_remote.sh\n"
            "  - run_multi_seed_RDT_q20_remote.py\n"
            "  - run_multi_seed_RDT_q20_remote.sh\n"
        )

    print(f"prepared bundle: {OUT_DIR}")
    print(f"files: {len(manifest)}")


if __name__ == "__main__":
    main()
