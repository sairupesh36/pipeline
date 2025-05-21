# examples/process_local_wet_files.py

import argparse

from datatrove.executor.ray import RayPipelineExecutor
from datatrove.pipeline.filters import LanguageFilter
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter

def main():
    parser = argparse.ArgumentParser(description="Process local WET files using a Ray pipeline.")
    parser.add_argument("input_dir", type=str, help="Path to the local directory containing WET files.")
    parser.add_argument("output_dir", type=str, help="Path to the local directory for JSONL output.")
    parser.add_argument("language", type=str, nargs="+", default=["en"], help="Target language code(s) for filtering (e.g., en es fr). Default: ['en']")
    parser.add_argument("--glob_pattern", type=str, default="*.wet.gz", help="Glob pattern for WET files (e.g., '*.wet.gz', '*.wet'). Default: '*.wet.gz'")
    parser.add_argument("--ray_tasks", type=int, default=1, help="Number of Ray tasks to use. Default: 1")
    parser.add_argument("--logging_dir", type=str, default="./pipeline_logs", help="Directory to store logs. Default: ./pipeline_logs")

    args = parser.parse_args()

    # Define pipeline components using parsed args
    warc_reader = WarcReader(
        data_folder=args.input_dir,
        glob_pattern=args.glob_pattern,
        default_metadata={"source_script": "process_local_wet_files"} # Example metadata
    )

    # Handle single or multiple languages for output path
    lang_str = "_".join(args.language)

    language_filter = LanguageFilter(
        languages=args.language, # Pass the list of languages
        exclusion_writer=JsonlWriter(
            output_folder=f"{args.output_dir}/non_{lang_str}/"
        )
    )

    jsonl_writer = JsonlWriter(
        output_folder=f"{args.output_dir}/filtered_{lang_str}/"
    )

    pipeline = [
        warc_reader,
        language_filter,
        jsonl_writer,
    ]

    executor = RayPipelineExecutor(
        pipeline=pipeline,
        tasks=args.ray_tasks,
        logging_dir=f"{args.logging_dir}/ray_logs",
        skip_completed=True,
    )
    executor.run()

if __name__ == "__main__":
    main()
