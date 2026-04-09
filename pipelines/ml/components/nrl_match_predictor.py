"""Predict NRL match margins using GradientBoosting regression on rolling features."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def nrl_match_predictor(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read match features, train GradientBoosting on margin, identify overperformers.

    Uses rolling form, head-to-head, and contextual features to predict match margin
    (home_score - away_score). Residual analysis reveals which teams consistently
    beat or underperform model expectations.
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import KFold, cross_val_predict, cross_val_score
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(
        f"SELECT * FROM `{project_id}.{bq_dataset}.nrl_match_features_v`"
    ).to_dataframe()

    print(f"Loaded {len(df)} match rows")

    # Drop rows where rolling features are NULL (first few games per team)
    df = df.dropna(subset=["home_last5_win_rate", "away_last5_win_rate"]).reset_index(drop=True)
    print(f"After dropping NULL rolling features: {len(df)} rows")

    target_col = "margin"
    feature_cols = [
        "home_last5_win_rate", "home_last5_avg_margin", "home_last5_avg_pf", "home_last5_avg_pa",
        "home_season_win_rate", "home_season_home_win_rate",
        "away_last5_win_rate", "away_last5_avg_margin", "away_last5_avg_pf", "away_last5_avg_pa",
        "away_season_win_rate",
        "h2h_home_win_rate", "form_differential", "season_form_differential",
        "year",
    ]

    X_raw = df[feature_cols].fillna(0).astype(float)
    scaler = StandardScaler()
    X = scaler.fit_transform(X_raw)
    y = df[target_col].astype(float).values

    print(f"Training on {len(df)} records, {len(feature_cols)} features, "
          f"target range: {y.min():.0f} to {y.max():.0f}")

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42,
    )

    # 5-fold CV predictions
    kfold = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_predictions = cross_val_predict(model, X, y, cv=kfold)

    cv_scores = cross_val_score(model, X, y, cv=kfold, scoring="r2")
    mae_scores = -cross_val_score(model, X, y, cv=kfold, scoring="neg_mean_absolute_error")

    # Fit final model for feature importances
    model.fit(X, y)
    importances = dict(zip(feature_cols, [round(float(v), 4) for v in model.feature_importances_]))
    sorted_importances = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True))

    residuals = np.round(cv_predictions - y, 4)
    predicted = np.round(cv_predictions, 4)

    r2 = round(float(cv_scores.mean()), 4)
    mae = round(float(mae_scores.mean()), 4)
    rmse = round(float(np.sqrt(np.mean(residuals ** 2))), 4)

    print(f"R2={r2}, MAE={mae}, RMSE={rmse}")
    print(f"Top features: {list(sorted_importances.keys())[:5]}")

    # --- Write match predictions to BigQuery ---
    bq_client = bigquery.Client(project=project_id, location=region)

    output_df = pd.DataFrame({
        "match_date": df["match_date"].astype(str),
        "year": df["year"].astype(int),
        "round": df["round"].astype(str),
        "home_team": df["home_team"].astype(str),
        "away_team": df["away_team"].astype(str),
        "home_score": df["home_score"].astype(int),
        "away_score": df["away_score"].astype(int),
        "actual_margin": df["margin"].astype(int),
        "predicted_margin": predicted,
        "residual": residuals,
    })

    pred_table = f"{project_id}.{bq_dataset}.nrl_match_predictions"
    pred_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("match_date", "STRING"),
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("round", "STRING"),
            bigquery.SchemaField("home_team", "STRING"),
            bigquery.SchemaField("away_team", "STRING"),
            bigquery.SchemaField("home_score", "INT64"),
            bigquery.SchemaField("away_score", "INT64"),
            bigquery.SchemaField("actual_margin", "INT64"),
            bigquery.SchemaField("predicted_margin", "FLOAT64"),
            bigquery.SchemaField("residual", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(output_df, pred_table, job_config=pred_config).result()
    print(f"Wrote {len(output_df)} rows to {pred_table}")

    # --- Write feature importances ---
    imp_rows = []
    for rank, (feat, imp) in enumerate(sorted_importances.items(), start=1):
        imp_rows.append({"feature_name": feat, "importance": imp, "rank": rank})
    importances_df = pd.DataFrame(imp_rows)

    imp_table = f"{project_id}.{bq_dataset}.nrl_match_feature_importances"
    imp_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("feature_name", "STRING"),
            bigquery.SchemaField("importance", "FLOAT64"),
            bigquery.SchemaField("rank", "INT64"),
        ],
    )
    bq_client.load_table_from_dataframe(importances_df, imp_table, job_config=imp_config).result()
    print(f"Wrote {len(importances_df)} rows to {imp_table}")

    # --- Team overperformance: avg residual when team is home ---
    # Positive residual = model predicted higher margin than actual (team underperformed)
    # Negative residual = team outperformed prediction
    home_perf = output_df.groupby("home_team").agg(
        home_avg_residual=("residual", "mean"),
        home_matches=("residual", "count"),
    ).reset_index().rename(columns={"home_team": "team"})

    away_perf = output_df.groupby("away_team").agg(
        away_avg_residual=("residual", "mean"),
        away_matches=("residual", "count"),
    ).reset_index().rename(columns={"away_team": "team"})

    # For away team, margin is from home perspective, so negative residual means
    # home team underperformed = away team overperformed
    away_perf["away_avg_residual"] = -away_perf["away_avg_residual"]

    team_perf = home_perf.merge(away_perf, on="team", how="outer").fillna(0)
    team_perf["total_matches"] = team_perf["home_matches"].astype(int) + team_perf["away_matches"].astype(int)
    team_perf["avg_overperformance"] = np.round(
        (team_perf["home_avg_residual"] * team_perf["home_matches"] -
         team_perf["away_avg_residual"] * team_perf["away_matches"]) /
        team_perf["total_matches"].replace(0, 1), 4
    )
    # Negative overall residual = team consistently beats expectations
    team_perf = team_perf.sort_values("avg_overperformance").reset_index(drop=True)
    team_perf["overperformance_rank"] = range(1, len(team_perf) + 1)

    overperf_df = team_perf[["team", "avg_overperformance", "total_matches",
                              "overperformance_rank"]].copy()
    overperf_df["total_matches"] = overperf_df["total_matches"].astype(int)

    overperf_table = f"{project_id}.{bq_dataset}.nrl_team_overperformance"
    overperf_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("avg_overperformance", "FLOAT64"),
            bigquery.SchemaField("total_matches", "INT64"),
            bigquery.SchemaField("overperformance_rank", "INT64"),
        ],
    )
    bq_client.load_table_from_dataframe(overperf_df, overperf_table, job_config=overperf_config).result()
    print(f"Wrote {len(overperf_df)} rows to {overperf_table}")

    # --- Report ---
    top_overperformers = overperf_df.head(5).to_dict("records")

    report_data = {
        "total_matches": len(output_df),
        "r2": r2,
        "mae": mae,
        "rmse": rmse,
        "cv_r2_std": round(float(cv_scores.std()), 4),
        "feature_importances": sorted_importances,
        "top_overperformers": top_overperformers,
        "residual_stats": {
            "mean": round(float(residuals.mean()), 4),
            "std": round(float(residuals.std()), 4),
            "min": round(float(residuals.min()), 4),
            "max": round(float(residuals.max()), 4),
        },
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
