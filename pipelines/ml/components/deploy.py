"""Deploy: upload model to Vertex AI Model Registry and deploy to endpoint."""

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
    endpoint_display_name: str = "player-role-endpoint",
) -> str:
    """Upload model to registry and deploy to endpoint."""
    import json
    import os
    import shutil
    import joblib
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region, staging_bucket=gcs_bucket)

    with open(metrics.path) as f:
        metrics_data = json.load(f)

    # Prepare model artifacts — extract bare sklearn model for pre-built container
    artifact_dir = "/tmp/model_artifacts"
    os.makedirs(artifact_dir, exist_ok=True)
    bundle = joblib.load(model.path)
    joblib.dump(bundle["model"], os.path.join(artifact_dir, "model.joblib"))

    # Upload model
    model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=artifact_dir,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest",
        labels={
            "pipeline": "player-role-clustering",
            "k": str(metrics_data["best_k"]),
            "silhouette": str(metrics_data["best_silhouette"]).replace(".", "_"),
        },
    )
    print(f"Model uploaded: {model.resource_name}")

    # Get or create endpoint
    endpoints = aiplatform.Endpoint.list(
        filter=f'display_name="{endpoint_display_name}"',
    )

    if endpoints:
        endpoint = endpoints[0]
        print(f"Using existing endpoint: {endpoint.resource_name}")
    else:
        endpoint = aiplatform.Endpoint.create(
            display_name=endpoint_display_name,
        )
        print(f"Created endpoint: {endpoint.resource_name}")

    # Deploy (undeploy existing models first for clean swap)
    if endpoint.traffic_split:
        for deployed_id in list(endpoint.traffic_split.keys()):
            endpoint.undeploy(deployed_model_id=deployed_id)

    model.deploy(
        endpoint=endpoint,
        machine_type="n1-standard-2",
        min_replica_count=1,
        max_replica_count=1,
    )

    print(f"Model deployed to endpoint: {endpoint.resource_name}")
    return endpoint.resource_name
