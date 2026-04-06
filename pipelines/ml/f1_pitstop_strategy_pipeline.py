"""Vertex AI Pipeline: F1 Pit Stop Strategy Impact Analysis."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.f1_pitstop_analyzer import f1_pitstop_analyzer
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.f1_pitstop_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "f1-pitstop-strategy"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Analyze F1 pit stop strategy impact on race positions",
)
def f1_pitstop_strategy_pipeline(
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

    pitstop_task = f1_pitstop_analyzer(
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )
    pitstop_task.after(validate_task)

    log_metrics(
        metrics_artifact=pitstop_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=f1_pitstop_strategy_pipeline,
        package_path="f1_pitstop_strategy_pipeline.json",
    )
    print("Pipeline compiled to f1_pitstop_strategy_pipeline.json")
