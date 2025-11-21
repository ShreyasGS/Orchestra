import dlt
import github_pipeline


def main():
    # Load the source from your github_pipeline module
    source = github_pipeline.github_source

    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_orchestra_test2",
        destination="bigquery",
        dataset_name="github_orc_data_test2",
        progress="log"
    )

    # Run the pipeline
    info = pipeline.run(source)

    print("Pipeline finished:")
    print(info)

    return info


if __name__ == "__main__":
    main()
