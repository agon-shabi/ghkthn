from sklearn import datasets, model_selection
import pandas as pd


def _fix_column(col):
    return col.replace(" (cm)", "").replace(" ", "_")


def create_input_files():
    iris = datasets.load_iris()
    columns = [_fix_column(col) for col in iris.feature_names]
    df = pd.DataFrame(iris.data, columns=columns)
    df['target'] = pd.Series(iris.target)
    train_df, test_df = model_selection.train_test_split(df, test_size=0.5)

    test_df.to_csv('./test_df.csv', index=False)

    for target, target_df in train_df.groupby('target'):
        target_df.to_csv(f'./train_{target}_df.csv', index=False)


if __name__ == '__main__':
    create_input_files()
