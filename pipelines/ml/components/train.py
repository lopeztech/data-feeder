"""Train: K-Means clustering with hyperparameter tuning on k."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1", "joblib==1.4.2",
        "pyarrow==18.1.0", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def train(
    dataset: dsl.Input[dsl.Dataset],
    feature_columns: dsl.Input[dsl.Artifact],
    model: dsl.Output[dsl.Model],
    metrics: dsl.Output[dsl.Artifact],
    min_k: int = 3,
    max_k: int = 10,
) -> int:
    """Train K-Means, sweep k, pick best silhouette score."""
    import json
    import pandas as pd
    import joblib
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    df = pd.read_parquet(dataset.path)

    with open(feature_columns.path) as f:
        feature_cols = json.load(f)

    X = df[feature_cols].values
    print(f"Training on {X.shape[0]} samples, {X.shape[1]} features")

    best_k = min_k
    best_score = -1
    results = []

    for k in range(min_k, max_k + 1):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        inertia = km.inertia_
        results.append({"k": k, "silhouette": round(score, 4), "inertia": round(inertia, 2)})
        print(f"  k={k}: silhouette={score:.4f}, inertia={inertia:.2f}")

        if score > best_score:
            best_score = score
            best_k = k

    # Retrain with best k
    final_model = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    final_model.fit(X)

    joblib.dump({"model": final_model, "feature_columns": feature_cols}, model.path)

    metrics_data = {
        "best_k": best_k,
        "best_silhouette": round(best_score, 4),
        "sweep_results": results,
    }
    with open(metrics.path, "w") as f:
        json.dump(metrics_data, f, indent=2)

    print(f"Best model: k={best_k}, silhouette={best_score:.4f}")
    return best_k
