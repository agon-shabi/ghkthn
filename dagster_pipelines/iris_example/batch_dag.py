"""
Creates a DAG that:
(1) loads data from 3 source - this can be done in parallel
(2) joins on feature creation
(3) trains 3 types of models - this can be done in parallel
(4) joins on creating an ensemble model
(5) performs an out-of-sample test of the ensemble model
"""

import pandas as pd
from dagster import pipeline, solid, execute_pipeline, IOManager, io_manager, OutputDefinition, ModeDefinition
from sklearn.base import ClassifierMixin
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC
from collections import Counter
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
import os


class FSIOManager(IOManager):

    @staticmethod
    def __get_dir(context):
        return os.path.join(context.pipeline_name, context.run_id)

    @staticmethod
    def __get_file_name(context):
        return f'{context.step_key}_df.csv'

    def handle_output(self, context, df):
        dir_path = self.__get_dir(context)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        df.to_csv(os.path.join(dir_path, self.__get_file_name(context)), index=False)

    def load_input(self, context):
        ctx = context.upstream_output
        return pd.read_csv(os.path.join(self.__get_dir(ctx), self.__get_file_name(ctx)))


@io_manager
def fs_io_manager(init_context):
    return FSIOManager()


@solid(output_defs=[OutputDefinition(io_manager_key="fs_io_manager_key")])
def load_0(context, csv_path):
    """
    Bespoke logic for loading type 0 data.
    """
    return pd.read_csv(csv_path)


@solid(output_defs=[OutputDefinition(io_manager_key="fs_io_manager_key")])
def load_1(context, csv_path):
    """
    Bespoke logic for loading type 1 data.
    """
    return pd.read_csv(csv_path)


@solid(output_defs=[OutputDefinition(io_manager_key="fs_io_manager_key")])
def load_2(context, csv_path):
    """
    Bespoke logic for loading type 2 data.
    """
    return pd.read_csv(csv_path)


@solid
def load_test(context, csv_path):
    """
    Bespoke logic for loading test data.
    """
    return pd.read_csv(csv_path)


@solid
def create_features(context, type_0_df, type_1_df, type_2_df):
    """
    Bespoke logic for create features out of n types of data.
    """
    return pd.concat([type_0_df, type_1_df, type_2_df])


def _split_df(df):
    x = df[set(df.columns).difference('target')]
    y = df['target']

    return x, y


def _train_model(clf, df):
    return clf().fit(*_split_df(df))


@solid
def train_knn_model(context, df):
    return _train_model(KNeighborsClassifier, df)


@solid
def train_nn_model(context, df):
    return _train_model(MLPClassifier, df)


@solid
def train_svc_model(context, df):
    return _train_model(SVC, df)


class EnsembleClassifier(ClassifierMixin):

    def __init__(self, models):
        self.__models = models

    def predict(self, x):
        predictions = zip(*[model.predict(x) for model in self.__models])
        return [Counter(preds).most_common()[0][0] for preds in predictions]


@solid
def create_ensemble_model(context, knn_model, nn_model, svc_model):
    return EnsembleClassifier([knn_model, nn_model, svc_model])


@solid
def test_model(context, model, test_df):
    score = model.score(*_split_df(test_df))
    context.log.info(f'Model score = {score}')
    return score


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="default",
            resource_defs={
                "gcs": gcs_resource,
                "io_manager": gcs_pickle_io_manager,
                "fs_io_manager_key": fs_io_manager,
            },
        ),
    ]
)
def full_run():
    features = create_features(
        load_0(),
        load_1(),
        load_2()
    )

    ensemble_model = create_ensemble_model(train_knn_model(features),
                                           train_nn_model(features),
                                           train_svc_model(features))

    test_model(ensemble_model, load_test())


run_config = {
    "solids": {
        "load_0": {"inputs": {"csv_path": {"value": "train_0_df.csv"}}},
        "load_1": {"inputs": {"csv_path": {"value": "train_1_df.csv"}}},
        "load_2": {"inputs": {"csv_path": {"value": "train_2_df.csv"}}},
        "load_test": {"inputs": {"csv_path": {"value": "test_df.csv"}}},
    }
}

"""
Dagit version:

solids:
  load_0:
    inputs:
      csv_path: {"value": "train_0_df.csv"}
  load_1:
    inputs:
      csv_path: {"value": "train_1_df.csv"}
  load_2:
    inputs:
      csv_path: {"value": "train_2_df.csv"}
  load_test:
    inputs:
      csv_path: {"value": "test_df.csv"}
"""

if __name__ == '__main__':
    result = execute_pipeline(full_run, run_config=run_config)
