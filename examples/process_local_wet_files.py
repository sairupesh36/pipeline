# examples/process_local_wet_files.py

from datatrove.executor.ray import RayPipelineExecutor
from datatrove.pipeline.filters import LanguageFilter
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter

# Configuration variables
INPUT_DIR = "data/wet_files/"  # TODO: Update this path
OUTPUT_DIR = "output/processed_data/"  # TODO: Update this path
LANGUAGES = ["en", "es"]  # TODO: Update with desired languages
GLOB_PATTERN = "*.wet.gz"
RAY_TASKS = 2
LOGGING_DIR = "pipeline_logs/"

def main():
    # Define pipeline components using global config variables
    warc_reader = WarcReader(
        data_folder=INPUT_DIR,
        glob_pattern=GLOB_PATTERN,
        default_metadata={"source_script": "process_local_wet_files"}
    )

    lang_str = "_".join(LANGUAGES)

    language_filter = LanguageFilter(
        languages=LANGUAGES,
        exclusion_writer=JsonlWriter(
            output_folder=f"{OUTPUT_DIR}/non_{lang_str}/"
        )
    )

    jsonl_writer = JsonlWriter(
        output_folder=f"{OUTPUT_DIR}/filtered_{lang_str}/"
    )

    pipeline = [
        warc_reader,
        language_filter,
        jsonl_writer,
    ]

    executor = RayPipelineExecutor(
        pipeline=pipeline,
        tasks=RAY_TASKS,
        logging_dir=f"{LOGGING_DIR}/ray_logs",
        skip_completed=True,
    )
    executor.run()

if __name__ == "__main__":
    main()
