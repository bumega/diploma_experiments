from __future__ import annotations

import importlib


def main() -> None:
    mods = ["torch", "pandas", "numpy", "sklearn", "umap", "pyHSICLasso", "ucimlrepo", "shap"]
    for name in mods:
        try:
            mod = importlib.import_module(name)
            version = getattr(mod, "__version__", "unknown")
            print(f"{name}: OK {version}")
        except Exception as exc:
            print(f"{name}: MISSING {exc!r}")

    try:
        import torch

        print(f"cuda_available: {torch.cuda.is_available()}")
        print(f"device_count: {torch.cuda.device_count()}")
        if torch.cuda.is_available():
            print(f"device_name: {torch.cuda.get_device_name(0)}")
    except Exception as exc:
        print(f"torch_cuda_check_failed: {exc!r}")

    try:
        from ucimlrepo import fetch_ucirepo

        ds = fetch_ucirepo(id=327)
        print(f"B_real_features_shape: {ds.data.features.shape}")
        print(f"B_real_targets_shape: {ds.data.targets.shape}")
        print("dataset_fetch: OK")
    except Exception as exc:
        print(f"dataset_fetch: FAILED {exc!r}")


if __name__ == "__main__":
    main()
