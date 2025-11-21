import os
import dlt
import github_pipeline


def run_resource(
    resource_name: str,
    incremental_date: str | None = None,
):
    base_source = github_pipeline.github_source
    

    # Apply incremental ONLY if explicitly passed
    if incremental_date is not None:
        # Dynamically get the resource: base_source.issues, base_source.forks, etc.
        resource = getattr(base_source, resource_name)
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                "updated_at",
                initial_value=incremental_date,
            )
        )

    selected_source = base_source.with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_inc_orc_demo_{resource_name}",
        destination="bigquery",   
        dataset_name="demo_inc_orc_github",
        progress="log",
    )

    info = pipeline.run(selected_source)
    print(f"{resource_name} -> {info}")
    return info


def main():
    # Orchestra can inject this env var — or it can be empty
    incremental_date = os.environ.get("INCREMENTAL_DATE")

    a = run_resource("repos")
    b = run_resource("contributors")
    c = run_resource("releases")
    #incremental only for issues
    d = run_resource("issues", incremental_date=incremental_date)

    return a, b, c, d


if __name__ == "__main__":
    main()
