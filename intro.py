import dlt
import github_pipeline


def main():
    # Load the source from your github_pipeline module
    source = github_pipeline.github_source.with_resources('releases')

    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="bigquery",
        dataset_name="demo_github",
        progress="log"
    )

    # Run the pipeline
    info = pipeline.run(source)

    print("Pipeline finished:")
    print(info)

    return info


if __name__ == "__main__":
    main()
