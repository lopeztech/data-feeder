"""Vertex AI Pipeline: Optimal Team Profile for Championship Contention."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.compute_optimal_profile import compute_optimal_profile
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "team-optimal-profile"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Compute the ideal statistical profile for a championship-contending NFL team",
)
def team_optimal_profile_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    profile_task = compute_optimal_profile(
        project_id=project_id,
        region=region,
        bq_view=bq_view,
        bq_dataset=bq_dataset,
    )
    profile_task.after(validate_task)

    log_metrics(
        metrics_artifact=profile_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=team_optimal_profile_pipeline,
        package_path="team_optimal_profile_pipeline.json",
    )
    print("Pipeline compiled to team_optimal_profile_pipeline.json")
