from pipelines import run_all_multi_seed


COMMON_MLP = {
    "d_hidden": 32,
    "n_hidden_layers": 1,
    "num_epochs": 50,
    "batch_size": 128,
    "checkpoint_every": 5,
    "output": False,
}


run_all_multi_seed(
    seeds=[41, 42, 43, 44, 45],
    datasets=("A", "A_real"),
    results_root="results_multi_seed_A",
    qgrids={"A": [2, 3, 5], "A_real": [2, 3, 5]},
    mlp_reducer_params=COMMON_MLP,
    unary_params=dict(COMMON_MLP),
)
