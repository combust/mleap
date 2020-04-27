import argparse
import sys

import numpy as np
import pandas as pd

from mleap.sklearn.feature_union import FeatureUnion
from mleap.sklearn.pipeline import Pipeline
from mleap.sklearn.preprocessing.data import FeatureExtractor, NDArrayToDataFrame, LabelEncoder, ReshapeArrayToN1
from mleap.sklearn.preprocessing.data import OneHotEncoder


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bundle-dir",
        type=str,
        required=True
    )
    parser.add_argument(
        "--original-csv-path",
        type=str,
        required=True
    )
    parser.add_argument(
        "--transformed-csv-path",
        type=str,
        required=True
    )
    return parser.parse_args()


def main():
    args = parse_args()

    df = pd.DataFrame(np.array([
        ['Alice', 'Smith'],
        ['Jack', 'Johnson'],
        ['Bob', 'Robertson'],
        ['Bob', 'Smith'],
        ['Alice', 'Johnson'],
    ]), columns=['first', 'last'])

    first_extractor = FeatureExtractor(input_scalars=['first'], output_vector='first_extracted')
    first_label_encoder = LabelEncoder(input_features=['first'], output_features='first_label_encoded')
    first_reshaper = ReshapeArrayToN1()
    first_one_hot_encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')
    first_one_hot_encoder.mlinit(prior_tf=first_label_encoder, output_features='first_one_hot_encoded')
    first_pipeline = Pipeline([
        (first_extractor.name, first_extractor),
        (first_label_encoder.name, first_label_encoder),
        (first_reshaper.name, first_reshaper),
        (first_one_hot_encoder.name, first_one_hot_encoder),
    ])
    first_pipeline.mlinit()

    last_extractor = FeatureExtractor(input_scalars=['last'], output_vector='last_extracted')
    last_label_encoder = LabelEncoder(input_features=['last'], output_features='last_label_encoded')
    last_reshaper = ReshapeArrayToN1()
    last_one_hot_encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')
    last_one_hot_encoder.mlinit(prior_tf=last_label_encoder, output_features='last_one_hot_encoded')
    last_pipeline = Pipeline([
        (last_extractor.name, last_extractor),
        (last_label_encoder.name, last_label_encoder),
        (last_reshaper.name, last_reshaper),
        (last_one_hot_encoder.name, last_one_hot_encoder),
    ])
    last_pipeline.mlinit()

    feature_union = FeatureUnion([(first_pipeline.name, first_pipeline), (last_pipeline.name, last_pipeline)])
    feature_union.mlinit()

    to_df = NDArrayToDataFrame(['first_0', 'first_1', 'first_2', 'last_0', 'last_1', 'last_2'])

    union_pipeline = Pipeline([(feature_union.name, feature_union), (to_df.name, to_df)])
    union_pipeline.fit(df)
    union_pipeline.mlinit()

    union_pipeline.serialize_to_bundle(args.bundle_dir, "one_hot_encoder_bundle", init=True)
    df.to_csv(args.original_csv_path, index=False)
    union_pipeline.transform(df).to_csv(args.transformed_csv_path, index=False)


if __name__ == "__main__":
    sys.exit(main())
