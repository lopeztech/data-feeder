"""Vertex AI Pipeline: Team-to-Player Bridge for Positional Value Mapping."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.compute_positional_weights import compute_positional_weights
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "team-player-bridge"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Map team-level win drivers to player positions for roster optimization",
)
def team_player_bridge_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    bridge_task = compute_positional_weights(
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )
    bridge_task.after(validate_task)

    log_metrics(
        metrics_artifact=bridge_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=team_player_bridge_pipeline,
        package_path="team_player_bridge_pipeline.json",
    )
    print("Pipeline compiled to team_player_bridge_pipeline.json")
