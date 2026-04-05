"""Compute optimal team profile: identify elite team feature ranges and ridge coefficients."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def compute_optimal_profile(
    project_id: str,
    region: str,
    bq_view: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Compute the optimal team profile by analyzing elite teams (top 25% win_loss_pct per year).

    Steps:
    1. Define elite teams as top 25% win_loss_pct per year
    2. Compute elite percentiles (p25, median, p75) and league median per feature
    3. Fit RidgeCV on elite subset to get feature coefficients
    4. Write profile to BigQuery
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.linear_model import RidgeCV
    from sklearn.preprocessing import StandardScaler
    from google.cloud import bigquery

    # Read data
    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    # Drop identifiers
    id_cols = ["year", "team"]
    target_col = "win_loss_perc"

    # Define elite: top 25% win_loss_pct per year
    thresholds = df.groupby("year")[target_col].quantile(0.75)
    df["elite_threshold"] = df["year"].map(thresholds)
    elite_mask = df[target_col] >= df["elite_threshold"]
    elite_df = df[elite_mask].copy()

    print(f"Total teams: {len(df)}, Elite teams: {len(elite_df)} ({100*len(elite_df)/len(df):.1f}%)")

    # Numeric feature columns (exclude identifiers, target, and helper)
    exclude = set(id_cols + ["is_playoff", "elite_threshold"])
    numeric_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]

    # Compute statistics per feature
    profile_rows = []
    for col in numeric_cols:
        elite_vals = elite_df[col].dropna()
        league_vals = df[col].dropna()
        elite_p25 = round(float(elite_vals.quantile(0.25)), 4)
        elite_median = round(float(elite_vals.median()), 4)
        elite_p75 = round(float(elite_vals.quantile(0.75)), 4)
        league_median = round(float(league_vals.median()), 4)
        gap = round(elite_median - league_median, 4)
        profile_rows.append({
            "feature_name": col,
            "elite_p25": elite_p25,
            "elite_median": elite_median,
            "elite_p75": elite_p75,
            "league_median": league_median,
            "gap": gap,
        })

    # Fit RidgeCV on elite subset: features -> win_loss_pct
    feature_cols = [c for c in numeric_cols if c != target_col]
    X_elite = elite_df[feature_cols].fillna(0).astype(float)
    y_elite = elite_df[target_col].fillna(0).astype(float).values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_elite)

    ridge = RidgeCV(alphas=[0.01, 0.1, 1.0, 10.0, 100.0], cv=5)
    ridge.fit(X_scaled, y_elite)
    r2 = round(float(ridge.score(X_scaled, y_elite)), 4)

    coef_map = dict(zip(feature_cols, [round(float(c), 4) for c in ridge.coef_]))

    # Add ridge coefficients to profile rows
    for row in profile_rows:
        row["ridge_coefficient"] = coef_map.get(row["feature_name"], 0.0)

    # Write to BigQuery
    profile_df = pd.DataFrame(profile_rows)
    bq_client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.team_optimal_profile"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("feature_name", "STRING"),
            bigquery.SchemaField("elite_p25", "FLOAT64"),
            bigquery.SchemaField("elite_median", "FLOAT64"),
            bigquery.SchemaField("elite_p75", "FLOAT64"),
            bigquery.SchemaField("league_median", "FLOAT64"),
            bigquery.SchemaField("ridge_coefficient", "FLOAT64"),
            bigquery.SchemaField("gap", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(profile_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(profile_df)} rows to {table_id}")

    # Report
    sorted_by_gap = sorted(profile_rows, key=lambda x: abs(x["gap"]), reverse=True)
    report_data = {
        "total_teams": len(df),
        "elite_teams": len(elite_df),
        "ridge_r2": r2,
        "ridge_alpha": float(ridge.alpha_),
        "top_features_by_gap": [
            {"feature": r["feature_name"], "gap": r["gap"], "ridge_coef": r["ridge_coefficient"]}
            for r in sorted_by_gap[:10]
        ],
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
