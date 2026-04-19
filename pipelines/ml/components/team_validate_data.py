"""Validate team data: run quality checks on the team feature view before training."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.30.0", "pandas==2.2.3", "db-dtypes==1.3.1",
        "pyarrow==18.1.0", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def validate_team_data(
    project_id: str,
    bq_view: str,
    min_rows: int = 100,
    max_null_pct: float = 0.5,
    max_wins: int = 17,
    max_losses: int = 17,
) -> int:
    """Run data quality checks on the team feature view. Fails the pipeline if checks don't pass.

    Checks:
    1. Minimum row count (expect ~672 for 32 teams x 21 seasons)
    2. No columns are entirely NULL
    3. No column exceeds max_null_pct NULL rate
    4. wins in range 0..max_wins, losses in range 0..max_losses (defaults sized for NFL;
       NRL/F1 callers should pass larger bounds)
    5. win_loss_pct in range 0-1
    6. No duplicate (year, team) pairs
    7. Numeric features have non-zero variance

    Returns the row count on success.
    """
    import pandas as pd
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    errors: list[str] = []

    # 1. Minimum row count
    if len(df) < min_rows:
        errors.append(f"Row count {len(df)} is below minimum threshold {min_rows}")

    # 2-3. NULL checks per column
    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct == 1.0:
            errors.append(f"Column '{col}' is 100% NULL")
        elif null_pct > max_null_pct:
            errors.append(f"Column '{col}' is {null_pct:.1%} NULL (threshold: {max_null_pct:.0%})")

    # 4. wins and losses range
    if "wins" in df.columns:
        wins = pd.to_numeric(df["wins"], errors="coerce")
        out_of_range = ((wins < 0) | (wins > max_wins)).sum()
        if out_of_range > 0:
            errors.append(f"{out_of_range} wins values outside 0-{max_wins} range")

    if "losses" in df.columns:
        losses = pd.to_numeric(df["losses"], errors="coerce")
        out_of_range = ((losses < 0) | (losses > max_losses)).sum()
        if out_of_range > 0:
            errors.append(f"{out_of_range} losses values outside 0-{max_losses} range")

    # 5. win_loss_pct range
    if "win_loss_perc" in df.columns:
        wlp = pd.to_numeric(df["win_loss_perc"], errors="coerce")
        out_of_range = ((wlp < 0) | (wlp > 1)).sum()
        if out_of_range > 0:
            errors.append(f"{out_of_range} win_loss_perc values outside 0-1 range")

    # 6. Duplicate (year, team) pairs
    if "year" in df.columns and "team" in df.columns:
        dupes = df.duplicated(subset=["year", "team"]).sum()
        if dupes > 0:
            errors.append(f"{dupes} duplicate (year, team) pairs")

    # 7. Zero-variance numeric columns
    drop_cols = {"year", "team"}
    numeric_cols = [c for c in df.columns if c not in drop_cols and pd.api.types.is_numeric_dtype(df[c])]
    for col in numeric_cols:
        if df[col].std() == 0:
            errors.append(f"Column '{col}' has zero variance (all values identical)")

    if errors:
        msg = "Data validation FAILED:\n" + "\n".join(f"  - {e}" for e in errors)
        print(msg)
        raise ValueError(msg)

    print(f"Data validation PASSED: {len(df)} rows, {len(df.columns)} columns, all checks OK")
    return len(df)
