import dlt
import github_pipeline


def run_resource(resource_name: str):
    base_source = github_pipeline.github_source

    # keep only the selected resource from the source
    selected_source = base_source.with_resources(resource_name)

    # create a separate pipeline per resource
    pipeline = dlt.pipeline(
        pipeline_name=f"github_orc_demo_{resource_name}",
        destination="bigquery",
        dataset_name="demo_orc_github",
        progress="log",
    )

    info = pipeline.run(selected_source)
    print(f"{resource_name} -> {info}")
    return info


def main():
    a = run_resource("repos")
    b = run_resource("contributors")
    c = run_resource("releases")
    d = run_resource("issues")
    e = run_resource("forks")

    return a, b, c, d


if __name__ == "__main__":
    main()
