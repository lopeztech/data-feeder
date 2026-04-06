"""Compute best NFL team analysis: composite dominance scoring with feature-level explanations."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def compute_best_team(
    project_id: str,
    region: str,
    bq_view: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Identify the best NFL team and explain why using composite dominance scoring.

    Methodology:
    1. Compute per-season z-scores for key performance pillars:
       - Winning: win_loss_perc
       - Offence: points_per_game, yards_per_game, pass_completion_pct, rush_yds_per_game
       - Defence: points_opp_per_game (inverted), turnover_pct, interception_pct (inverted)
       - Efficiency: yds_per_play_offense, score_pct, penalty_yds_per_game (inverted)
    2. Aggregate pillar z-scores into a weighted dominance score per team-season
    3. Rank teams by all-time mean dominance (sustained excellence) and peak season
    4. Use GradientBoosting feature importance to explain which stats most separate
       elite teams from the rest
    5. Write team_dominance_rankings and team_dominance_drivers to BigQuery
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    print(f"Loaded {len(df)} team-seasons ({df['year'].min()}-{df['year'].max()})")

    # --- 1. Define performance pillars and their constituent features ---
    # Features where higher is better get weight +1; lower-is-better get -1
    pillars = {
        "winning": [("win_loss_perc", 1)],
        "offence": [
            ("points_per_game", 1),
            ("yards_per_game", 1),
            ("pass_completion_pct", 1),
            ("rush_yds_per_game", 1),
        ],
        "defence": [
            ("points_opp_per_game", -1),  # lower is better
            ("turnover_pct", 1),          # higher turnovers forced is better
        ],
        "efficiency": [
            ("yds_per_play_offense", 1),
            ("score_pct", 1),
            ("penalty_yds_per_game", -1),  # fewer penalty yards is better
        ],
    }

    pillar_weights = {
        "winning": 0.35,
        "offence": 0.25,
        "defence": 0.25,
        "efficiency": 0.15,
    }

    # --- 2. Compute per-season z-scores and pillar scores ---
    all_features = []
    for feats in pillars.values():
        all_features.extend([f for f, _ in feats])

    # Z-score within each season to account for era differences
    pillar_scores = {p: [] for p in pillars}
    for _, season_df in df.groupby("year"):
        idx = season_df.index
        for pillar_name, feats in pillars.items():
            z_sum = np.zeros(len(season_df))
            for col, direction in feats:
                vals = pd.to_numeric(season_df[col], errors="coerce").fillna(0).values
                std = vals.std()
                if std > 0:
                    z = (vals - vals.mean()) / std * direction
                else:
                    z = np.zeros(len(vals))
                z_sum += z
            # Average z-score across features in the pillar
            pillar_scores[pillar_name].append(
                pd.Series(z_sum / len(feats), index=idx)
            )

    for p in pillars:
        df[f"z_{p}"] = pd.concat(pillar_scores[p])

    # Weighted dominance score
    df["dominance_score"] = sum(
        df[f"z_{p}"] * w for p, w in pillar_weights.items()
    )

    # --- 3. Rank teams by sustained excellence ---
    team_agg = (
        df.groupby("team")
        .agg(
            mean_dominance=("dominance_score", "mean"),
            peak_dominance=("dominance_score", "max"),
            seasons_top_8=("dominance_score", lambda x: (x >= x.quantile(0.75)).sum()),
            total_wins=("wins", "sum"),
            total_losses=("losses", "sum"),
            avg_win_pct=("win_loss_perc", "mean"),
            avg_points_per_game=("points_per_game", "mean"),
            avg_points_opp_per_game=("points_opp_per_game", "mean"),
            best_season_year=("dominance_score", "idxmax"),
        )
        .reset_index()
    )

    # Resolve best_season_year from index back to actual year
    team_agg["best_season_year"] = team_agg["best_season_year"].map(
        lambda idx: int(df.loc[idx, "year"])
    )

    # Composite rank: 60% sustained, 40% peak
    team_agg["composite_score"] = (
        0.6 * team_agg["mean_dominance"] + 0.4 * team_agg["peak_dominance"]
    )
    team_agg = team_agg.sort_values("composite_score", ascending=False).reset_index(drop=True)
    team_agg["rank"] = team_agg.index + 1

    # Round floats
    float_cols = [
        "mean_dominance", "peak_dominance", "composite_score",
        "avg_win_pct", "avg_points_per_game", "avg_points_opp_per_game",
    ]
    for col in float_cols:
        team_agg[col] = team_agg[col].round(4)

    print(f"\nTop 5 teams by composite dominance:")
    for _, row in team_agg.head(5).iterrows():
        print(
            f"  {row['rank']}. {row['team']} — composite={row['composite_score']:.3f}, "
            f"win%={row['avg_win_pct']:.3f}, best_year={row['best_season_year']}"
        )

    # --- 4. Feature importance: what separates elite teams? ---
    # Label top 25% dominance seasons as "elite"
    threshold = df["dominance_score"].quantile(0.75)
    df["is_elite"] = (df["dominance_score"] >= threshold).astype(int)

    id_cols = {"year", "team", "is_playoff", "is_elite", "dominance_score",
               "z_winning", "z_offence", "z_defence", "z_efficiency"}
    feature_cols = [
        c for c in df.columns
        if c not in id_cols and pd.api.types.is_numeric_dtype(df[c])
    ]

    X = df[feature_cols].fillna(0).astype(float)
    y = df["is_elite"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    gb = GradientBoostingClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.1, random_state=42,
    )
    gb.fit(X_scaled, y)
    accuracy = round(float(gb.score(X_scaled, y)), 4)

    importances = sorted(
        zip(feature_cols, gb.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )

    driver_rows = []
    for rank_i, (feat, imp) in enumerate(importances, 1):
        elite_mean = round(float(df.loc[df["is_elite"] == 1, feat].mean()), 4)
        league_mean = round(float(df[feat].mean()), 4)
        driver_rows.append({
            "feature_name": feat,
            "importance": round(float(imp), 6),
            "rank": rank_i,
            "elite_mean": elite_mean,
            "league_mean": league_mean,
            "elite_advantage": round(elite_mean - league_mean, 4),
        })

    print(f"\nTop 5 dominance drivers (classifier accuracy={accuracy}):")
    for d in driver_rows[:5]:
        print(f"  {d['rank']}. {d['feature_name']} (imp={d['importance']:.4f}, "
              f"elite_advantage={d['elite_advantage']:+.2f})")

    # --- 5. Write per-season dominance to BigQuery ---
    season_rows = df[
        ["year", "team", "dominance_score", "z_winning", "z_offence",
         "z_defence", "z_efficiency", "win_loss_perc", "is_elite"]
    ].copy()
    season_rows = season_rows.rename(columns={
        "z_winning": "pillar_winning",
        "z_offence": "pillar_offence",
        "z_defence": "pillar_defence",
        "z_efficiency": "pillar_efficiency",
    })
    for col in ["dominance_score", "pillar_winning", "pillar_offence",
                "pillar_defence", "pillar_efficiency", "win_loss_perc"]:
        season_rows[col] = season_rows[col].round(4)

    bq_client = bigquery.Client(project=project_id, location=region)

    # Write season-level dominance scores
    season_table = f"{project_id}.{bq_dataset}.team_dominance_seasons"
    bq_client.load_table_from_dataframe(
        season_rows, season_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("year", "INT64"),
                bigquery.SchemaField("team", "STRING"),
                bigquery.SchemaField("dominance_score", "FLOAT64"),
                bigquery.SchemaField("pillar_winning", "FLOAT64"),
                bigquery.SchemaField("pillar_offence", "FLOAT64"),
                bigquery.SchemaField("pillar_defence", "FLOAT64"),
                bigquery.SchemaField("pillar_efficiency", "FLOAT64"),
                bigquery.SchemaField("win_loss_perc", "FLOAT64"),
                bigquery.SchemaField("is_elite", "INT64"),
            ],
        ),
    ).result()
    print(f"\nWrote {len(season_rows)} rows to {season_table}")

    # Write all-time rankings
    ranking_table = f"{project_id}.{bq_dataset}.team_dominance_rankings"
    ranking_cols = [
        "rank", "team", "composite_score", "mean_dominance", "peak_dominance",
        "seasons_top_8", "total_wins", "total_losses", "avg_win_pct",
        "avg_points_per_game", "avg_points_opp_per_game", "best_season_year",
    ]
    bq_client.load_table_from_dataframe(
        team_agg[ranking_cols], ranking_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("rank", "INT64"),
                bigquery.SchemaField("team", "STRING"),
                bigquery.SchemaField("composite_score", "FLOAT64"),
                bigquery.SchemaField("mean_dominance", "FLOAT64"),
                bigquery.SchemaField("peak_dominance", "FLOAT64"),
                bigquery.SchemaField("seasons_top_8", "INT64"),
                bigquery.SchemaField("total_wins", "INT64"),
                bigquery.SchemaField("total_losses", "INT64"),
                bigquery.SchemaField("avg_win_pct", "FLOAT64"),
                bigquery.SchemaField("avg_points_per_game", "FLOAT64"),
                bigquery.SchemaField("avg_points_opp_per_game", "FLOAT64"),
                bigquery.SchemaField("best_season_year", "INT64"),
            ],
        ),
    ).result()
    print(f"Wrote {len(team_agg)} rows to {ranking_table}")

    # Write dominance drivers
    drivers_table = f"{project_id}.{bq_dataset}.team_dominance_drivers"
    drivers_df = pd.DataFrame(driver_rows)
    bq_client.load_table_from_dataframe(
        drivers_df, drivers_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("feature_name", "STRING"),
                bigquery.SchemaField("importance", "FLOAT64"),
                bigquery.SchemaField("rank", "INT64"),
                bigquery.SchemaField("elite_mean", "FLOAT64"),
                bigquery.SchemaField("league_mean", "FLOAT64"),
                bigquery.SchemaField("elite_advantage", "FLOAT64"),
            ],
        ),
    ).result()
    print(f"Wrote {len(drivers_df)} rows to {drivers_table}")

    # --- 6. Build report ---
    best = team_agg.iloc[0]
    best_team = str(best["team"])

    # Get best team's pillar strengths
    best_seasons = df[df["team"] == best_team]
    pillar_means = {
        p: round(float(best_seasons[f"z_{p}"].mean()), 4)
        for p in pillars
    }
    strongest_pillar = max(pillar_means, key=pillar_means.get)

    report_data = {
        "best_team": best_team,
        "best_team_composite_score": float(best["composite_score"]),
        "best_team_avg_win_pct": float(best["avg_win_pct"]),
        "best_team_best_season_year": int(best["best_season_year"]),
        "best_team_peak_dominance": float(best["peak_dominance"]),
        "best_team_strongest_pillar": strongest_pillar,
        "best_team_pillar_scores": pillar_means,
        "total_teams_analysed": len(team_agg),
        "total_seasons": int(df["year"].nunique()),
        "classifier_accuracy": accuracy,
        "top_5_teams": [
            {
                "rank": int(row["rank"]),
                "team": str(row["team"]),
                "composite_score": float(row["composite_score"]),
                "avg_win_pct": float(row["avg_win_pct"]),
            }
            for _, row in team_agg.head(5).iterrows()
        ],
        "top_5_drivers": [
            {
                "feature": d["feature_name"],
                "importance": d["importance"],
                "elite_advantage": d["elite_advantage"],
            }
            for d in driver_rows[:5]
        ],
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)

    print(f"\nAnalysis complete. Best team: {best_team} "
          f"(composite={best['composite_score']:.3f}, strongest pillar: {strongest_pillar})")
