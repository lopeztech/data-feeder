"""Evaluate team clusters: analyze archetypes and write assignments to BigQuery."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1", "joblib==1.4.2",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def evaluate_team_clusters(
    dataset: dsl.Input[dsl.Dataset],
    raw_dataset: dsl.Input[dsl.Dataset],
    model: dsl.Input[dsl.Model],
    metrics: dsl.Input[dsl.Artifact],
    feature_columns: dsl.Input[dsl.Artifact],
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Assign cluster archetypes, analyze team styles, write team_archetypes to BigQuery."""
    import json
    import pandas as pd
    import joblib
    import numpy as np
    from google.cloud import bigquery

    df = pd.read_parquet(dataset.path)
    raw_df = pd.read_parquet(raw_dataset.path)
    bundle = joblib.load(model.path)
    km = bundle["model"]
    style_cols = bundle["feature_columns"]

    with open(metrics.path) as f:
        metrics_data = json.load(f)

    # Assign clusters using normalized dataset
    X = df[style_cols].values
    df["cluster_id"] = km.predict(X)

    # Carry cluster assignment to raw data
    raw_df["cluster_id"] = df["cluster_id"].values

    # Cluster analysis
    n_clusters = km.n_clusters
    analysis = []
    global_means = df[style_cols].mean()

    for cid in range(n_clusters):
        mask = df["cluster_id"] == cid
        cluster_df = df[mask][style_cols]
        raw_cluster = raw_df[mask]
        size = int(mask.sum())

        # Mean win_loss_perc from raw data
        avg_win_pct = float(raw_cluster["win_loss_perc"].mean()) if "win_loss_perc" in raw_cluster.columns else 0.0

        # Top distinguishing features (highest mean in this cluster vs global)
        cluster_means = cluster_df.mean()
        diff = (cluster_means - global_means).abs().sort_values(ascending=False)
        top_features = {col: round(float(cluster_means[col]), 3) for col in diff.head(5).index}

        # Auto-generate archetype label: top 2 features where cluster mean > global mean
        above_global = cluster_means - global_means
        above_sorted = above_global.sort_values(ascending=False)
        label_features = [col for col in above_sorted.head(2).index if above_sorted[col] > 0]
        if len(label_features) == 0:
            archetype_label = f"cluster_{cid}"
        else:
            archetype_label = "-".join(label_features[:2])

        analysis.append({
            "cluster_id": cid,
            "size": size,
            "pct": round(100 * size / len(df), 1),
            "avg_win_pct": round(avg_win_pct, 4),
            "archetype_label": archetype_label,
            "top_features": top_features,
        })
        print(f"Cluster {cid} ({archetype_label}): {size} teams ({100*size/len(df):.1f}%), "
              f"avg win%={avg_win_pct:.3f}, top features: {list(top_features.keys())}")

    # Impact score: 1/(1+distance) from centroid
    centroids = km.cluster_centers_
    distances = np.linalg.norm(X - centroids[df["cluster_id"].values], axis=1)
    impact_scores = np.round(1 / (1 + distances), 4)

    # Build archetype label lookup
    label_lookup = {a["cluster_id"]: a["archetype_label"] for a in analysis}
    win_pct_lookup = {a["cluster_id"]: a["avg_win_pct"] for a in analysis}

    # Write to BigQuery
    output_df = pd.DataFrame({
        "year": raw_df["year"].values,
        "team": raw_df["team"].astype(str).values,
        "archetype_id": df["cluster_id"].astype(int).values,
        "archetype_label": [label_lookup[cid] for cid in df["cluster_id"].values],
        "avg_win_pct": [win_pct_lookup[cid] for cid in df["cluster_id"].values],
        "impact_score": impact_scores,
    })

    bq_client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.team_archetypes"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("archetype_id", "INT64"),
            bigquery.SchemaField("archetype_label", "STRING"),
            bigquery.SchemaField("avg_win_pct", "FLOAT64"),
            bigquery.SchemaField("impact_score", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(output_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(output_df)} rows to {table_id}")

    # Report
    report_data = {
        "n_clusters": n_clusters,
        "best_silhouette": metrics_data["best_silhouette"],
        "total_teams": len(df),
        "clusters": analysis,
        "sweep_results": metrics_data["sweep_results"],
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
