"""NRL coaching insights: tactical clustering, SWOT profiles, rivalry matrix, trend analysis."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def nrl_coaching_analyzer(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Analyze NRL teams for actionable coaching insights.

    1. K-Means on tactical features to identify playing styles
    2. Per-team SWOT: strengths (>75th pctile) and weaknesses (<25th pctile)
    3. Head-to-head rivalry matrix from raw fixtures
    4. Rolling 5-year trend analysis (improving/declining/stable)
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    # Load coaching features (per team per season)
    df = client.query(
        f"SELECT * FROM `{project_id}.{bq_dataset}.nrl_coaching_features_v`"
    ).to_dataframe()
    print(f"Loaded {len(df)} team-season rows")

    # Load raw fixtures for rivalry matrix. Alias the PascalCase schema columns
    # to the lowercase names the downstream rivalry code expects.
    fixtures = client.query(
        f"""
        SELECT
          HomeTeam  AS home_team,
          AwayTeam  AS away_team,
          HomeScore AS home_score,
          AwayScore AS away_score,
          Season    AS season,
          DATE(KickOffTime) AS match_date
        FROM `{project_id}.{bq_dataset}.nrl_fixtures_1990_2025`
        """
    ).to_dataframe()
    print(f"Loaded {len(fixtures)} fixture rows")

    # ========== 1. TEAM PLAYING STYLE CLUSTERING ==========

    # Aggregate all-time tactical profile per team
    tactical_cols = ["win_rate", "avg_margin", "consistency_score", "home_dependency",
                     "bounce_back_rate", "streak_maintenance_rate", "close_game_win_rate",
                     "blowout_loss_rate", "attack_defense_ratio",
                     "early_season_win_rate", "late_season_win_rate"]

    team_profile = df.groupby("team")[tactical_cols].mean().reset_index()

    # Normalize for clustering
    scaler = StandardScaler()
    X_cluster = scaler.fit_transform(team_profile[tactical_cols].fillna(0))

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
    team_profile["style_id"] = best_labels

    # Label styles by characteristic
    style_chars = {}
    for sid in team_profile["style_id"].unique():
        cluster = team_profile[team_profile["style_id"] == sid]
        means = cluster[tactical_cols].mean()
        overall = team_profile[tactical_cols].mean()
        diff = means - overall

        # Pick label based on most distinguishing feature
        top_feat = diff.abs().idxmax()
        if top_feat == "attack_defense_ratio" and diff[top_feat] > 0:
            label = "high-scoring attackers"
        elif top_feat == "consistency_score" and diff[top_feat] < 0:
            label = "consistent grinders"
        elif top_feat == "home_dependency" and diff[top_feat] > 0:
            label = "home specialists"
        elif top_feat in ("bounce_back_rate", "close_game_win_rate") and diff[top_feat] > 0:
            label = "clutch performers"
        elif top_feat == "late_season_win_rate" and diff[top_feat] > 0:
            label = "late-season surgers"
        elif diff["win_rate"] < -0.05:
            label = "rebuilding"
        else:
            label = "balanced"
        style_chars[sid] = label

    team_profile["playing_style"] = team_profile["style_id"].map(style_chars)

    # ========== 2. SWOT: STRENGTHS & WEAKNESSES ==========

    percentiles_75 = team_profile[tactical_cols].quantile(0.75)
    percentiles_25 = team_profile[tactical_cols].quantile(0.25)

    friendly_names = {
        "win_rate": "Win Rate", "avg_margin": "Avg Margin",
        "consistency_score": "Consistency", "home_dependency": "Home Dependency",
        "bounce_back_rate": "Bounce-back", "streak_maintenance_rate": "Streak Maintenance",
        "close_game_win_rate": "Close Game Clutch", "blowout_loss_rate": "Blowout Vulnerability",
        "attack_defense_ratio": "Attack/Defense", "early_season_win_rate": "Early Season",
        "late_season_win_rate": "Late Season",
    }
    # For these cols, lower is better
    invert_cols = {"consistency_score", "home_dependency", "blowout_loss_rate"}

    strengths_list = []
    weaknesses_list = []
    for _, row in team_profile.iterrows():
        s, w = [], []
        for col in tactical_cols:
            name = friendly_names.get(col, col)
            if col in invert_cols:
                if row[col] <= percentiles_25[col]:
                    s.append(name)
                elif row[col] >= percentiles_75[col]:
                    w.append(name)
            else:
                if row[col] >= percentiles_75[col]:
                    s.append(name)
                elif row[col] <= percentiles_25[col]:
                    w.append(name)
        strengths_list.append(", ".join(s[:3]) if s else "None")
        weaknesses_list.append(", ".join(w[:3]) if w else "None")

    team_profile["strengths"] = strengths_list
    team_profile["weaknesses"] = weaknesses_list

    # Round tactical cols
    for col in tactical_cols:
        team_profile[col] = team_profile[col].round(4)

    # ========== WRITE TEAM PROFILES ==========
    bq_client = bigquery.Client(project=project_id, location=region)

    profile_df = team_profile[["team", "playing_style", "style_id",
                                "strengths", "weaknesses"] + tactical_cols].copy()

    profile_table = f"{project_id}.{bq_dataset}.nrl_team_profiles"
    profile_schema = [
        bigquery.SchemaField("team", "STRING"),
        bigquery.SchemaField("playing_style", "STRING"),
        bigquery.SchemaField("style_id", "INT64"),
        bigquery.SchemaField("strengths", "STRING"),
        bigquery.SchemaField("weaknesses", "STRING"),
    ] + [bigquery.SchemaField(c, "FLOAT64") for c in tactical_cols]

    bq_client.load_table_from_dataframe(
        profile_df, profile_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=profile_schema),
    ).result()
    print(f"Wrote {len(profile_df)} rows to {profile_table}")

    # ========== 3. RIVALRY MATRIX ==========

    # Build head-to-head records from raw fixtures
    h2h_rows = []
    for _, row in fixtures.iterrows():
        hs = row.get("home_score")
        aws = row.get("away_score")
        if pd.isna(hs) or pd.isna(aws):
            continue
        ht, at = row["home_team"], row["away_team"]
        hs, aws = int(hs), int(aws)

        # Record from both perspectives
        h2h_rows.append({"team": ht, "opponent": at, "pf": hs, "pa": aws,
                          "win": 1 if hs > aws else 0, "is_home": 1})
        h2h_rows.append({"team": at, "opponent": ht, "pf": aws, "pa": hs,
                          "win": 1 if aws > hs else 0, "is_home": 0})

    h2h_df = pd.DataFrame(h2h_rows)

    rivalry = h2h_df.groupby(["team", "opponent"]).agg(
        total_matches=("win", "count"),
        wins=("win", "sum"),
        avg_margin=("pf", lambda x: round((x - h2h_df.loc[x.index, "pa"]).mean(), 2)),
        home_wins=("is_home", lambda x: h2h_df.loc[x.index, "win"][h2h_df.loc[x.index, "is_home"] == 1].sum()),
        home_matches=("is_home", "sum"),
    ).reset_index()

    rivalry["win_rate"] = (rivalry["wins"] / rivalry["total_matches"]).round(3)
    rivalry["home_win_rate"] = np.where(
        rivalry["home_matches"] > 0,
        (rivalry["home_wins"] / rivalry["home_matches"]).round(3),
        np.nan,
    )
    rivalry["losses"] = rivalry["total_matches"] - rivalry["wins"]
    # Flag statistical dominance (>60% win rate with 10+ meetings)
    rivalry["is_dominant"] = ((rivalry["win_rate"] > 0.6) & (rivalry["total_matches"] >= 10)).astype(int)

    rivalry_out = rivalry[["team", "opponent", "total_matches", "wins", "losses",
                            "win_rate", "avg_margin", "home_win_rate", "is_dominant"]].copy()
    rivalry_out["wins"] = rivalry_out["wins"].astype(int)
    rivalry_out["losses"] = rivalry_out["losses"].astype(int)
    rivalry_out["total_matches"] = rivalry_out["total_matches"].astype(int)

    rivalry_table = f"{project_id}.{bq_dataset}.nrl_rivalry_matrix"
    rivalry_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("opponent", "STRING"),
            bigquery.SchemaField("total_matches", "INT64"),
            bigquery.SchemaField("wins", "INT64"),
            bigquery.SchemaField("losses", "INT64"),
            bigquery.SchemaField("win_rate", "FLOAT64"),
            bigquery.SchemaField("avg_margin", "FLOAT64"),
            bigquery.SchemaField("home_win_rate", "FLOAT64"),
            bigquery.SchemaField("is_dominant", "INT64"),
        ],
    )
    bq_client.load_table_from_dataframe(rivalry_out, rivalry_table, job_config=rivalry_config).result()
    print(f"Wrote {len(rivalry_out)} rows to {rivalry_table}")

    # ========== 4. ROLLING 5-YEAR TREND ANALYSIS ==========

    # Compute rolling 5-year windows per team
    years = sorted(df["year"].unique())
    trend_rows = []
    for team in df["team"].unique():
        team_data = df[df["team"] == team].sort_values("year")
        for i in range(len(years) - 4):
            window_years = [y for y in years[i:i+5]]
            window = team_data[team_data["year"].isin(window_years)]
            if len(window) < 3:
                continue
            trend_rows.append({
                "team": team,
                "window_start": int(window_years[0]),
                "window_end": int(window_years[-1]),
                "seasons_in_window": len(window),
                "avg_win_rate": round(window["win_rate"].mean(), 3),
                "avg_margin": round(window["avg_margin"].mean(), 2),
                "avg_close_game_wr": round(window["close_game_win_rate"].mean(), 3),
                "avg_bounce_back": round(window["bounce_back_rate"].mean(), 3),
            })

    trends_df = pd.DataFrame(trend_rows)

    # Compute trajectory: slope of win_rate across windows
    def compute_trajectory(group):
        if len(group) < 3:
            return "stable"
        x = np.arange(len(group))
        y = group["avg_win_rate"].values
        slope = np.polyfit(x, y, 1)[0]
        if slope > 0.015:
            return "improving"
        elif slope < -0.015:
            return "declining"
        return "stable"

    trajectories = trends_df.groupby("team").apply(compute_trajectory).reset_index()
    trajectories.columns = ["team", "trajectory"]
    trends_df = trends_df.merge(trajectories, on="team", how="left")

    trends_df["seasons_in_window"] = trends_df["seasons_in_window"].astype(int)

    trend_table = f"{project_id}.{bq_dataset}.nrl_team_trends"
    trend_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("window_start", "INT64"),
            bigquery.SchemaField("window_end", "INT64"),
            bigquery.SchemaField("seasons_in_window", "INT64"),
            bigquery.SchemaField("avg_win_rate", "FLOAT64"),
            bigquery.SchemaField("avg_margin", "FLOAT64"),
            bigquery.SchemaField("avg_close_game_wr", "FLOAT64"),
            bigquery.SchemaField("avg_bounce_back", "FLOAT64"),
            bigquery.SchemaField("trajectory", "STRING"),
        ],
    )
    bq_client.load_table_from_dataframe(trends_df, trend_table, job_config=trend_config).result()
    print(f"Wrote {len(trends_df)} rows to {trend_table}")

    # ========== REPORT ==========
    style_summary = team_profile.groupby("playing_style").agg(
        count=("team", "count"),
        teams=("team", lambda x: ", ".join(sorted(x))),
    ).to_dict("index")

    top_rivalries = rivalry_out[rivalry_out["is_dominant"] == 1].sort_values(
        "total_matches", ascending=False
    ).head(10)[["team", "opponent", "wins", "losses", "win_rate"]].to_dict("records")

    improving = trajectories[trajectories["trajectory"] == "improving"]["team"].tolist()
    declining = trajectories[trajectories["trajectory"] == "declining"]["team"].tolist()

    report_data = {
        "total_teams": len(team_profile),
        "best_k": best_k,
        "silhouette_score": round(best_score, 4),
        "style_summary": style_summary,
        "top_dominant_rivalries": top_rivalries,
        "trajectory_summary": {
            "improving": improving,
            "declining": declining,
            "stable_count": len(trajectories) - len(improving) - len(declining),
        },
        "total_rivalry_pairs": len(rivalry_out),
        "total_trend_windows": len(trends_df),
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2, default=str)
