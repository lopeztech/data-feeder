"""Vertex AI Pipeline: F1 Constructor Era Dominance Rankings."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.f1_constructor_dominance import f1_constructor_dominance
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.f1_constructor_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "f1-constructor-dominance"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Rank F1 constructors by composite dominance score and identify archetypes",
)
def f1_constructor_dominance_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
        min_rows=50,
    )

    dominance_task = f1_constructor_dominance(
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )
    dominance_task.after(validate_task)

    log_metrics(
        metrics_artifact=dominance_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=f1_constructor_dominance_pipeline,
        package_path="f1_constructor_dominance_pipeline.json",
    )
    print("Pipeline compiled to f1_constructor_dominance_pipeline.json")
