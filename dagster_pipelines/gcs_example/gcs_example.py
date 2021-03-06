from collections import Counter

from dagster import (
    InputDefinition,
    ModeDefinition,
    PresetDefinition,
    default_executors,
    file_relative_path,
    pipeline,
    
    solid,
)
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_celery_k8s import celery_k8s_job_executor


@solid(input_defs=[InputDefinition("word", str)], config_schema={"factor": int})
def multiply_the_word(context, word):
    return word * context.solid_config["factor"]


@solid(input_defs=[InputDefinition("word")])
def count_letters(_context, word):
    return dict(Counter(word))


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="default",
            resource_defs={"gcs": gcs_resource, "io_manager": gcs_pickle_io_manager},
            executor_defs=default_executors + [celery_k8s_job_executor],
        ),
        ModeDefinition(name="test", executor_defs=default_executors + [celery_k8s_job_executor],),
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "celery_k8s",
            config_files=[
                file_relative_path(__file__, "celery_k8s.yaml"),
                file_relative_path(__file__, "pipeline.yaml"),
            ],
            mode="default",
        ),
        PresetDefinition.from_files(
            "default",
            config_files=[
                file_relative_path(__file__, "pipeline.yaml"),
            ],
            mode="default",
        ),
    ],
)
def example_pipe():
    count_letters(multiply_the_word())
