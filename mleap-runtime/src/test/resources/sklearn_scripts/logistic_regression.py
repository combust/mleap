import argparse
import sys

import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split

from mleap.sklearn.linear_model import LogisticRegressionCV, LogisticRegression
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
    parser.add_argument(
        "--multinomial",
        type=bool,
        required=True
    )
    parser.add_argument(
        "--cv",
        type=bool,
        required=True
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.multinomial:
        X, y = make_classification(n_features=100, n_classes=2)
    else:
        X, y = make_classification(n_features=100, n_classes=3, n_informative=3)
    y = np.reshape(y, (-1, 1))

    feature_names = ["feature_{}".format(i) for i in range(X.shape[1])]
    df = pd.DataFrame(np.hstack((X, y)), columns=(feature_names + ['label']))
    train_df, test_df = train_test_split(df)

    feature_extractor = FeatureExtractor(input_scalars=feature_names, output_vector='features')

    if args.cv:
        logistic_regression = LogisticRegressionCV()
    else:
        logistic_regression = LogisticRegression()
    logistic_regression.mlinit(input_features='features', prediction_column='prediction')

    pipeline = Pipeline([('feature_extractor', feature_extractor), ('logistic_regression', logistic_regression)])
    pipeline.mlinit()

    pipeline.fit(train_df, train_df['label'])
    pipeline.serialize_to_bundle(args.bundle_path, "logistic_regression_bundle", init=True)

    test_df['prediction'] = pipeline.predict(test_df)
    test_df['probability'] = pipeline.predict(test_df)
    test_df.to_csv(args.csv_path, index=False)


if __name__ == "__main__":
    sys.exit(main())
