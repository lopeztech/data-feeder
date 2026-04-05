"""Vertex AI Pipeline: GradientBoosting Win Prediction for NFL Teams."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.train_win_predictor import train_win_predictor
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "team-win-predictor"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Predict NFL team win percentage and identify which stats drive winning",
)
def team_win_predictor_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    predict_task = train_win_predictor(
        project_id=project_id,
        region=region,
        bq_view=bq_view,
        bq_dataset=bq_dataset,
    )
    predict_task.after(validate_task)

    log_metrics(
        metrics_artifact=predict_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=team_win_predictor_pipeline,
        package_path="team_win_predictor_pipeline.json",
    )
    print("Pipeline compiled to team_win_predictor_pipeline.json")
