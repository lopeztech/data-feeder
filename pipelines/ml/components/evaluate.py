"""Evaluate: analyze clusters and write assignments to BigQuery."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas", "scikit-learn", "joblib", "pyarrow",
        "google-cloud-bigquery", "db-dtypes",
    ],
)
def evaluate(
    dataset_path: dsl.InputPath("Dataset"),
    model_path: dsl.InputPath("Model"),
    metrics_path: dsl.InputPath("Metrics"),
    feature_columns_path: dsl.InputPath("JsonArray"),
    project_id: str,
    bq_dataset: str,
    report_path: dsl.OutputPath("Report"),
):
    """Assign clusters, analyze, write player_clusters to BigQuery."""
    import json
    import pandas as pd
    import joblib
    import numpy as np
    from google.cloud import bigquery

    df = pd.read_parquet(dataset_path)
    bundle = joblib.load(model_path)
    model = bundle["model"]
    feature_cols = bundle["feature_columns"]

    with open(metrics_path) as f:
        metrics = json.load(f)

    X = df[feature_cols].values
    df["cluster_id"] = model.predict(X)

    # Cluster analysis
    n_clusters = model.n_clusters
    analysis = []
    for cid in range(n_clusters):
        mask = df["cluster_id"] == cid
        cluster_df = df[mask][feature_cols]
        size = int(mask.sum())

        # Top distinguishing features (highest mean in this cluster vs global)
        global_means = df[feature_cols].mean()
        cluster_means = cluster_df.mean()
        diff = (cluster_means - global_means).abs().sort_values(ascending=False)
        top_features = {col: round(float(cluster_means[col]), 3) for col in diff.head(5).index}

        analysis.append({
            "cluster_id": cid,
            "size": size,
            "pct": round(100 * size / len(df), 1),
            "top_features": top_features,
        })
        print(f"Cluster {cid}: {size} players ({100*size/len(df):.1f}%), top features: {list(top_features.keys())}")

    # Impact score: players who outperform their cluster centroid
    centroids = model.cluster_centers_
    distances = np.linalg.norm(X - centroids[df["cluster_id"].values], axis=1)
    df["impact_score"] = round(1 / (1 + distances), 4)

    # Write to BigQuery
    output_df = df[["player_id", "cluster_id", "impact_score"]].copy()
    output_df["cluster_id"] = output_df["cluster_id"].astype(int)

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{bq_dataset}.player_clusters"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("player_id", "STRING"),
            bigquery.SchemaField("cluster_id", "INT64"),
            bigquery.SchemaField("impact_score", "FLOAT64"),
        ],
    )
    client.load_table_from_dataframe(output_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(output_df)} rows to {table_id}")

    # Report
    report = {
        "n_clusters": n_clusters,
        "best_silhouette": metrics["best_silhouette"],
        "total_players": len(df),
        "clusters": analysis,
        "sweep_results": metrics["sweep_results"],
    }
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
