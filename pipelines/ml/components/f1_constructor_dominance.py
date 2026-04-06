"""Compute F1 constructor era dominance rankings with K-Means archetype clustering."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def f1_constructor_dominance(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read constructor features, compute dominance rankings, cluster into archetypes.

    Normalizes key metrics (win_rate, podium_rate, points_per_race, reliability_pct,
    avg_finish_position) to 0-1 scale, computes weighted composite score, then
    K-Means clusters (k=3..5, best silhouette) to identify constructor archetypes.
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from google.cloud import bigquery

    # Read raw data from the feature view
    client = bigquery.Client(project=project_id)
    df = client.query(
        f"SELECT * FROM `{project_id}.{bq_dataset}.f1_constructor_features_v`"
    ).to_dataframe()

    print(f"Loaded {len(df)} constructor-season rows")

    # Aggregate across all seasons per constructor
    agg = df.groupby("constructor").agg(
        nationality=("nationality", "first"),
        mean_points_per_race=("points_per_race", "mean"),
        mean_win_rate=("win_rate", "mean"),
        mean_podium_rate=("podium_rate", "mean"),
        mean_reliability=("reliability_pct", "mean"),
        mean_avg_finish=("avg_finish_position", "mean"),
        total_wins=("wins", "sum"),
        seasons_active=("year", "nunique"),
    ).reset_index()

    # Peak season: year with highest points_per_race
    peak = df.loc[df.groupby("constructor")["points_per_race"].idxmax()][["constructor", "year"]]
    peak = peak.rename(columns={"year": "peak_season"})
    agg = agg.merge(peak, on="constructor", how="left")

    # Normalize metrics to 0-1
    metrics = ["mean_win_rate", "mean_podium_rate", "mean_points_per_race",
               "mean_reliability", "mean_avg_finish"]
    norm = pd.DataFrame()
    for col in metrics:
        col_min = agg[col].min()
        col_max = agg[col].max()
        if col_max > col_min:
            norm[col] = (agg[col] - col_min) / (col_max - col_min)
        else:
            norm[col] = 0.0

    # Invert avg_finish_position (lower is better)
    norm["mean_avg_finish"] = 1.0 - norm["mean_avg_finish"]

    # Weighted composite score
    weights = {
        "mean_win_rate": 0.30,
        "mean_podium_rate": 0.25,
        "mean_points_per_race": 0.20,
        "mean_reliability": 0.15,
        "mean_avg_finish": 0.10,
    }
    agg["composite_score"] = sum(norm[col] * w for col, w in weights.items())
    agg["composite_score"] = agg["composite_score"].round(4)

    # K-Means clustering on normalized metrics (k=3..5, best silhouette)
    X_cluster = norm.values
    best_k, best_score, best_labels = 3, -1, None

    for k in range(3, 6):
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(X_cluster)
        if len(set(labels)) > 1:
            score = silhouette_score(X_cluster, labels)
            print(f"  k={k}: silhouette={score:.4f}")
            if score > best_score:
                best_k, best_score, best_labels = k, score, labels

    print(f"Best k={best_k}, silhouette={best_score:.4f}")
    agg["archetype_id"] = best_labels

    # Label archetypes by mean composite score
    archetype_means = agg.groupby("archetype_id")["composite_score"].mean().sort_values(ascending=False)
    label_map = {}
    labels_list = ["dominant", "competitive", "midfield", "backmarker", "occasional"]
    for i, (arch_id, _) in enumerate(archetype_means.items()):
        label_map[arch_id] = labels_list[min(i, len(labels_list) - 1)]
    agg["archetype_label"] = agg["archetype_id"].map(label_map)

    # Rank by composite score
    agg = agg.sort_values("composite_score", ascending=False).reset_index(drop=True)
    agg["rank"] = range(1, len(agg) + 1)

    # Write rankings to BigQuery
    bq_client = bigquery.Client(project=project_id, location=region)

    rankings_df = agg[["rank", "constructor", "nationality", "composite_score",
                        "mean_win_rate", "mean_podium_rate", "mean_points_per_race",
                        "mean_reliability", "mean_avg_finish", "total_wins",
                        "seasons_active", "peak_season", "archetype_id", "archetype_label"]].copy()
    rankings_df["total_wins"] = rankings_df["total_wins"].astype(int)
    rankings_df["seasons_active"] = rankings_df["seasons_active"].astype(int)
    rankings_df["peak_season"] = rankings_df["peak_season"].astype(int)

    rank_table = f"{project_id}.{bq_dataset}.f1_constructor_rankings"
    rank_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("constructor", "STRING"),
            bigquery.SchemaField("nationality", "STRING"),
            bigquery.SchemaField("composite_score", "FLOAT64"),
            bigquery.SchemaField("mean_win_rate", "FLOAT64"),
            bigquery.SchemaField("mean_podium_rate", "FLOAT64"),
            bigquery.SchemaField("mean_points_per_race", "FLOAT64"),
            bigquery.SchemaField("mean_reliability", "FLOAT64"),
            bigquery.SchemaField("mean_avg_finish", "FLOAT64"),
            bigquery.SchemaField("total_wins", "INT64"),
            bigquery.SchemaField("seasons_active", "INT64"),
            bigquery.SchemaField("peak_season", "INT64"),
            bigquery.SchemaField("archetype_id", "INT64"),
            bigquery.SchemaField("archetype_label", "STRING"),
        ],
    )
    bq_client.load_table_from_dataframe(rankings_df, rank_table, job_config=rank_config).result()
    print(f"Wrote {len(rankings_df)} rows to {rank_table}")

    # Write per-season data to BigQuery
    seasons_df = df[["year", "constructor", "total_points", "wins", "podiums",
                      "reliability_pct", "avg_finish_position", "points_per_race",
                      "win_rate"]].copy()
    seasons_df["total_points"] = seasons_df["total_points"].astype(int)
    seasons_df["wins"] = seasons_df["wins"].astype(int)
    seasons_df["podiums"] = seasons_df["podiums"].astype(int)

    season_table = f"{project_id}.{bq_dataset}.f1_constructor_seasons"
    season_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("constructor", "STRING"),
            bigquery.SchemaField("total_points", "INT64"),
            bigquery.SchemaField("wins", "INT64"),
            bigquery.SchemaField("podiums", "INT64"),
            bigquery.SchemaField("reliability_pct", "FLOAT64"),
            bigquery.SchemaField("avg_finish_position", "FLOAT64"),
            bigquery.SchemaField("points_per_race", "FLOAT64"),
            bigquery.SchemaField("win_rate", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(seasons_df, season_table, job_config=season_config).result()
    print(f"Wrote {len(seasons_df)} rows to {season_table}")

    # Report
    top_10 = rankings_df.head(10)[["rank", "constructor", "composite_score",
                                    "archetype_label"]].to_dict("records")
    cluster_summary = agg.groupby("archetype_label").agg(
        count=("constructor", "count"),
        avg_composite=("composite_score", "mean"),
    ).round(4).to_dict("index")

    report_data = {
        "total_constructors": len(rankings_df),
        "best_k": best_k,
        "silhouette_score": round(best_score, 4),
        "top_10": top_10,
        "cluster_summary": cluster_summary,
        "seasons_in_data": int(df["year"].nunique()),
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
