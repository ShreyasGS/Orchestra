import os
import dlt
import github_pipeline


def run_resource(resource_name: str):
    source = github_pipeline.github_source.with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_remote_demo_{resource_name}",
        destination="bigquery",
        dataset_name="demo_remote_github",
        progress="log",
    )

    info = pipeline.run(source)
    print(f"{resource_name} -> {info}")
    return info


def main():
    resource_name = os.environ["RESOURCE_NAME"]  # comes from matrix
    return run_resource(resource_name)


if __name__ == "__main__":
    main()
