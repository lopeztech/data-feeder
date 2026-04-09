"""Vertex AI Pipeline: NRL Match Outcome Predictor."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.nrl_match_predictor import nrl_match_predictor
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.nrl_match_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "nrl-match-predictor"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Predict NRL match margins and identify team overperformers",
)
def nrl_match_predictor_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
        min_rows=500,
    )

    predict_task = nrl_match_predictor(
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
        pipeline_func=nrl_match_predictor_pipeline,
        package_path="nrl_match_predictor_pipeline.json",
    )
    print("Pipeline compiled to nrl_match_predictor_pipeline.json")
