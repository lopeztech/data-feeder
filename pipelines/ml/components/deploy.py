"""Deploy: upload model to Vertex AI Model Registry."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-aiplatform==1.82.0", "joblib==1.4.2", "scikit-learn==1.6.1",
        "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def deploy_model(
    model: dsl.Input[dsl.Model],
    metrics: dsl.Input[dsl.Artifact],
    project_id: str,
    region: str,
    gcs_bucket: str,
    model_display_name: str = "player-role-kmeans",
) -> str:
    """Upload model to Vertex AI Model Registry."""
    import json
    import os
    import joblib
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region, staging_bucket=gcs_bucket)

    with open(metrics.path) as f:
        metrics_data = json.load(f)

    # Prepare model artifacts — extract bare sklearn model
    artifact_dir = "/tmp/model_artifacts"
    os.makedirs(artifact_dir, exist_ok=True)
    bundle = joblib.load(model.path)
    joblib.dump(bundle["model"], os.path.join(artifact_dir, "model.joblib"))

    # Upload to Model Registry
    registered = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=artifact_dir,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest",
        labels={
            "pipeline": "player-role-clustering",
            "k": str(metrics_data["best_k"]),
            "silhouette": str(metrics_data["best_silhouette"]).replace(".", "_"),
        },
    )
    print(f"Model registered: {registered.resource_name}")
    return registered.resource_name
