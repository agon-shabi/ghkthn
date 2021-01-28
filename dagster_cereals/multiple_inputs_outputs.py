import pandas as pd

from dagster import solid, pipeline, execute_pipeline, AssetMaterialization, EventMetadataEntry
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
    least_caloric = sorted_cereals[0]["name"]
    context.log.info(f"Least caloric hot cereal: {least_caloric}")
    yield Output(least_caloric)


@solid
def sort_cold_cereals_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    least_caloric = sorted_cereals[0]["name"]
    context.log.info(f"Least caloric cold cereal: {least_caloric}")

    yield Output(least_caloric)


@solid
def compare_calories(context, cereals, least_hot, least_cold):
    cereals_df = pd.DataFrame(cereals)

    def get_calories(name):
        return cereals_df[cereals_df["name"] == name]["calories"].iloc[0]

    cereal_choice = (
        least_hot if get_calories(least_hot) > get_calories(least_cold) else least_cold
    )
    context.log.info(
        f"Compare the calories of hot and cold cereals: {cereal_choice} is healthier"
    )
    yield AssetMaterialization(
        asset_key="cereal_choice",
        description="Which cereal is healthiest",
        metadata_entries=[
            EventMetadataEntry.text(cereal_choice, "Cereal Choice")
        ],
    )
    yield Output(cereal_choice)


@pipeline
def multiple_input_outputs_pipeline():
    cereals = load_cereals()

    hot_cereals, cold_cereals = split_cereals(cereals)
    hot_cereal = sort_hot_cereals_by_calories(hot_cereals)
    cold_cereal = sort_cold_cereals_by_calories(cold_cereals)

    compare_calories(cereals, hot_cereal, cold_cereal)


def main():
    """Specifying Config for Pipeline Execution"""

    run_config = {
        "solids": {
            "load_cereals": {"inputs": {"csv_path": {"value": "cereal.csv"}}},
            "split_cereals": {"config": {"process_hot": True, "process_cold": True}},
        }
    }

    result = execute_pipeline(multiple_input_outputs_pipeline, run_config)


if __name__ == "__main__":
    main()
