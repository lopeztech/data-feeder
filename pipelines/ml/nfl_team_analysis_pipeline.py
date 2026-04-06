"""Vertex AI Pipeline: NFL Best Team Analysis with dominance scoring."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.compute_best_team import compute_best_team
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "nfl-team-analysis"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Identify the best NFL team using composite dominance scoring and explain why",
)
def nfl_team_analysis_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    analysis_task = compute_best_team(
        project_id=project_id,
        region=region,
        bq_view=bq_view,
        bq_dataset=bq_dataset,
    )
    analysis_task.after(validate_task)

    log_metrics(
        metrics_artifact=analysis_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=nfl_team_analysis_pipeline,
        package_path="nfl_team_analysis_pipeline.json",
    )
    print("Pipeline compiled to nfl_team_analysis_pipeline.json")
