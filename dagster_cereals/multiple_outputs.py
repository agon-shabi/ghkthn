from dagster import solid, pipeline
from dagster import Field, Bool, OutputDefinition, Output

from input import load_cereals


@solid(
    config_schema={
        "process_hot": Field(Bool, is_required=False, default_value=True),
        "process_cold": Field(Bool, is_required=False, default_value=True),
    },
    output_defs=[
        OutputDefinition(name="hot_cereals", is_required=False),
        OutputDefinition(name="cold_cereals", is_required=False),
    ],
)
def split_cereals(context, cereals):
    if context.solid_config["process_hot"]:
        hot_cereals = [cereal for cereal in cereals if cereal["type"] == "H"]
        yield Output(hot_cereals, "hot_cereals")
    if context.solid_config["process_cold"]:
        cold_cereals = [cereal for cereal in cereals if cereal["type"] == "C"]
        yield Output(cold_cereals, "cold_cereals")


@solid
def sort_hot_cereals_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    context.log.info(
        "Least caloric hot cereal: {least_caloric}".format(
            least_caloric=sorted_cereals[0]["name"]
        )
    )


@solid
def sort_cold_cereals_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    context.log.info(
        "Least caloric cold cereal: {least_caloric}".format(
            least_caloric=sorted_cereals[0]["name"]
        )
    )


@pipeline
def multiple_outputs_pipeline():
    hot_cereals, cold_cereals = split_cereals(load_cereals())
    sort_hot_cereals_by_calories(hot_cereals)
    sort_cold_cereals_by_calories(cold_cereals)

