"""Compute NRL team all-time dominance rankings with K-Means archetype clustering."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def nrl_team_dominance(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read team season features, compute all-time dominance rankings, cluster archetypes.

    Normalizes key metrics (win_rate, avg_margin, away_win_rate, close_game_win_rate,
    blowout_rate, avg_points_against) to 0-1, computes weighted composite score, then
    K-Means clusters (k=3..6, best silhouette) to label team archetypes.
    Also computes era-adjusted modern dominance with exponential recency weighting.
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(
        f"SELECT * FROM `{project_id}.{bq_dataset}.nrl_team_features_v`"
    ).to_dataframe()

    print(f"Loaded {len(df)} team-season rows")

    # --- All-time aggregate per team ---
    agg = df.groupby("team").agg(
        mean_win_rate=("win_rate", "mean"),
        mean_avg_margin=("avg_margin", "mean"),
        mean_home_win_rate=("home_win_rate", "mean"),
        mean_away_win_rate=("away_win_rate", "mean"),
        mean_blowout_rate=("blowout_rate", "mean"),
        mean_close_game_win_rate=("close_game_win_rate", "mean"),
        mean_avg_points_for=("avg_points_for", "mean"),
        mean_avg_points_against=("avg_points_against", "mean"),
        total_wins=("wins", "sum"),
        total_games=("games_played", "sum"),
        seasons_active=("year", "nunique"),
    ).reset_index()

    # Defensive strength: lower points against is better, invert later
    agg["defensive_strength"] = agg["mean_avg_points_against"]

    # Peak season: year with best win rate
    peak = df.loc[df.groupby("team")["win_rate"].idxmax()][["team", "year"]]
    peak = peak.rename(columns={"year": "peak_season"})
    agg = agg.merge(peak, on="team", how="left")

    # --- Normalize metrics 0-1 ---
    metrics = ["mean_win_rate", "mean_avg_margin", "mean_away_win_rate",
               "mean_close_game_win_rate", "mean_blowout_rate", "defensive_strength"]
    norm = pd.DataFrame()
    for col in metrics:
        col_min = agg[col].min()
        col_max = agg[col].max()
        if col_max > col_min:
            norm[col] = (agg[col] - col_min) / (col_max - col_min)
        else:
            norm[col] = 0.0

    # Invert defensive_strength (lower points against = better)
    norm["defensive_strength"] = 1.0 - norm["defensive_strength"]

    # --- Weighted composite score ---
    weights = {
        "mean_win_rate": 0.25,
        "mean_avg_margin": 0.20,
        "mean_away_win_rate": 0.15,
        "mean_close_game_win_rate": 0.15,
        "mean_blowout_rate": 0.10,
        "defensive_strength": 0.10,
    }
    # Remaining 0.05 for longevity
    longevity_norm = (agg["seasons_active"] - agg["seasons_active"].min()) / \
        max(agg["seasons_active"].max() - agg["seasons_active"].min(), 1)
    agg["composite_score"] = sum(norm[col] * w for col, w in weights.items()) + \
        longevity_norm * 0.05
    agg["composite_score"] = agg["composite_score"].round(4)

    # --- Era-adjusted modern dominance (exponential decay, half-life ~10 years) ---
    max_year = df["year"].max()
    df["recency_weight"] = np.exp(-0.07 * (max_year - df["year"]))
    weighted = df.groupby("team").apply(
        lambda g: pd.Series({
            "modern_win_rate": np.average(g["win_rate"], weights=g["recency_weight"]),
            "modern_avg_margin": np.average(g["avg_margin"], weights=g["recency_weight"]),
        })
    ).reset_index()
    agg = agg.merge(weighted, on="team", how="left")
    agg["modern_dominance"] = (
        agg["modern_win_rate"] * 0.6 + (agg["modern_avg_margin"].clip(lower=0) / 30) * 0.4
    ).round(4)

    # --- K-Means clustering (k=3..6, best silhouette) ---
    X_cluster = norm.values
    best_k, best_score, best_labels = 3, -1, None

    for k in range(3, 7):
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
    labels_list = ["dynasty", "contender", "competitive", "rebuilding", "expansion", "cellar"]
    label_map = {}
    for i, (arch_id, _) in enumerate(archetype_means.items()):
        label_map[arch_id] = labels_list[min(i, len(labels_list) - 1)]
    agg["archetype_label"] = agg["archetype_id"].map(label_map)

    # Rank by composite score
    agg = agg.sort_values("composite_score", ascending=False).reset_index(drop=True)
    agg["rank"] = range(1, len(agg) + 1)

    # --- Write rankings to BigQuery ---
    bq_client = bigquery.Client(project=project_id, location=region)

    rankings_df = agg[["rank", "team", "composite_score", "modern_dominance",
                        "mean_win_rate", "mean_avg_margin", "mean_home_win_rate",
                        "mean_away_win_rate", "mean_close_game_win_rate",
                        "mean_blowout_rate", "mean_avg_points_for",
                        "mean_avg_points_against", "total_wins", "total_games",
                        "seasons_active", "peak_season",
                        "archetype_id", "archetype_label"]].copy()
    rankings_df["total_wins"] = rankings_df["total_wins"].astype(int)
    rankings_df["total_games"] = rankings_df["total_games"].astype(int)
    rankings_df["seasons_active"] = rankings_df["seasons_active"].astype(int)
    rankings_df["peak_season"] = rankings_df["peak_season"].astype(int)

    rank_table = f"{project_id}.{bq_dataset}.nrl_team_rankings"
    rank_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("composite_score", "FLOAT64"),
            bigquery.SchemaField("modern_dominance", "FLOAT64"),
            bigquery.SchemaField("mean_win_rate", "FLOAT64"),
            bigquery.SchemaField("mean_avg_margin", "FLOAT64"),
            bigquery.SchemaField("mean_home_win_rate", "FLOAT64"),
            bigquery.SchemaField("mean_away_win_rate", "FLOAT64"),
            bigquery.SchemaField("mean_close_game_win_rate", "FLOAT64"),
            bigquery.SchemaField("mean_blowout_rate", "FLOAT64"),
            bigquery.SchemaField("mean_avg_points_for", "FLOAT64"),
            bigquery.SchemaField("mean_avg_points_against", "FLOAT64"),
            bigquery.SchemaField("total_wins", "INT64"),
            bigquery.SchemaField("total_games", "INT64"),
            bigquery.SchemaField("seasons_active", "INT64"),
            bigquery.SchemaField("peak_season", "INT64"),
            bigquery.SchemaField("archetype_id", "INT64"),
            bigquery.SchemaField("archetype_label", "STRING"),
        ],
    )
    bq_client.load_table_from_dataframe(rankings_df, rank_table, job_config=rank_config).result()
    print(f"Wrote {len(rankings_df)} rows to {rank_table}")

    # --- Write per-season data to BigQuery ---
    seasons_df = df[["year", "team", "games_played", "wins", "draws", "losses",
                      "win_rate", "avg_points_for", "avg_points_against",
                      "avg_margin", "home_win_rate", "away_win_rate",
                      "blowout_rate", "close_game_win_rate",
                      "points_differential"]].copy()
    seasons_df["games_played"] = seasons_df["games_played"].astype(int)
    seasons_df["wins"] = seasons_df["wins"].astype(int)
    seasons_df["draws"] = seasons_df["draws"].astype(int)
    seasons_df["losses"] = seasons_df["losses"].astype(int)
    seasons_df["points_differential"] = seasons_df["points_differential"].astype(int)

    season_table = f"{project_id}.{bq_dataset}.nrl_team_seasons"
    season_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("games_played", "INT64"),
            bigquery.SchemaField("wins", "INT64"),
            bigquery.SchemaField("draws", "INT64"),
            bigquery.SchemaField("losses", "INT64"),
            bigquery.SchemaField("win_rate", "FLOAT64"),
            bigquery.SchemaField("avg_points_for", "FLOAT64"),
            bigquery.SchemaField("avg_points_against", "FLOAT64"),
            bigquery.SchemaField("avg_margin", "FLOAT64"),
            bigquery.SchemaField("home_win_rate", "FLOAT64"),
            bigquery.SchemaField("away_win_rate", "FLOAT64"),
            bigquery.SchemaField("blowout_rate", "FLOAT64"),
            bigquery.SchemaField("close_game_win_rate", "FLOAT64"),
            bigquery.SchemaField("points_differential", "INT64"),
        ],
    )
    bq_client.load_table_from_dataframe(seasons_df, season_table, job_config=season_config).result()
    print(f"Wrote {len(seasons_df)} rows to {season_table}")

    # --- Report ---
    top_10 = rankings_df.head(10)[["rank", "team", "composite_score",
                                    "modern_dominance", "archetype_label"]].to_dict("records")
    cluster_summary = agg.groupby("archetype_label").agg(
        count=("team", "count"),
        avg_composite=("composite_score", "mean"),
    ).round(4).to_dict("index")

    report_data = {
        "total_teams": len(rankings_df),
        "best_k": best_k,
        "silhouette_score": round(best_score, 4),
        "top_10": top_10,
        "cluster_summary": cluster_summary,
        "seasons_in_data": int(df["year"].nunique()),
        "year_range": f"{int(df['year'].min())}-{int(df['year'].max())}",
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
