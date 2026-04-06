"""Vertex AI Pipeline: Predict F1 Race Finish Positions."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.f1_driver_predictor import f1_driver_predictor
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.f1_driver_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "f1-driver-predictor"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Predict F1 race finish positions and identify driver overperformers",
)
def f1_driver_predictor_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
        min_rows=1000,
    )

    predict_task = f1_driver_predictor(
        project_id=project_id,
        region=region,
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
        pipeline_func=f1_driver_predictor_pipeline,
        package_path="f1_driver_predictor_pipeline.json",
    )
    print("Pipeline compiled to f1_driver_predictor_pipeline.json")
