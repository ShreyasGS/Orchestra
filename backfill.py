import os
import dlt
import github_pipeline


def run_resource(
    resource_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    base_source = github_pipeline.github_source

    # Apply backfill only if both start and end are provided
    if start_date is not None and end_date is not None:
        # get the underlying resource: base_source.forks, base_source.issues, etc.
        resource = getattr(base_source, resource_name)
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                "created_at",
                initial_value=start_date,
                end_value=end_date,
                row_order="asc",
            )
        )

    # Now slice to just that resource (with hints already applied if any)
    selected_source = base_source.with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_backfill_orc_demo_{resource_name}",
        destination="bigquery",
        dataset_name="demo_backfill_orc_github",
        progress="log",
    )

    info = pipeline.run(selected_source)
    print(f"{resource_name} -> {info}")
    return info


def main():
    # For backfill we usually care only about one resource (e.g. forks)
    start_date = os.environ.get("START_DATE")
    end_date = os.environ.get("END_DATE")

    # full loads – no backfill
    a = run_resource("repos")
    b = run_resource("contributors")
    c = run_resource("releases")
    # backfill only for forks (because we pass dates only here)
    d = run_resource("forks", start_date=start_date, end_date=end_date)

    return a, b, c, d


if __name__ == "__main__":
    main()
