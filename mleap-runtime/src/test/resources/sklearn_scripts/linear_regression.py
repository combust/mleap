import argparse
import sys

import numpy as np
import pandas as pd
from sklearn.datasets import make_regression
from sklearn.model_selection import train_test_split

from mleap.sklearn.linear_model import LinearRegression
from mleap.sklearn.pipeline import Pipeline
from mleap.sklearn.preprocessing.data import FeatureExtractor


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bundle-path",
        type=str,
        required=True
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        required=True
    )
    return parser.parse_args()


def main():
    args = parse_args()

    X, y = make_regression()
    y = np.reshape(y, (-1, 1))

    feature_names = ["feature_{}".format(i) for i in range(X.shape[1])]
    df = pd.DataFrame(np.hstack((X, y)), columns=(feature_names + ['label']))
    train_df, test_df = train_test_split(df)

    feature_extractor = FeatureExtractor(input_scalars=feature_names, output_vector='features')

    linear_regression = LinearRegression()
    linear_regression.mlinit(input_features='features', prediction_column='prediction')

    pipeline = Pipeline([('feature_extractor', feature_extractor), ('linear_regression', linear_regression)])
    pipeline.mlinit()

    pipeline.fit(train_df, train_df['label'])
    pipeline.serialize_to_bundle(args.bundle_path, "logistic_regression_bundle", init=True)

    test_df['prediction'] = pipeline.predict(test_df)
    test_df.to_csv(args.csv_path, index=False)


if __name__ == "__main__":
    sys.exit(main())
