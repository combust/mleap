#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import shutil
import tempfile
import unittest

import numpy as np
import pandas as pd

from mleap.sklearn.preprocessing.data import FeatureExtractor
from mleap.sklearn.preprocessing.data import SimpleImputer


class TestImputer(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame([
            [0.85281608, 1.50669264],
            [-1.04544152, np.NaN],
            [0.41515407, -0.29941475],
            [np.NaN, -0.96775275],
            [np.NaN, -0.85734022]
        ], columns=['a', 'b'])
        self.feature_extractor = FeatureExtractor(input_scalars=['a'], output_vector='a_extracted')
        self.tmp_dir = tempfile.mkdtemp(prefix="mleap.python.tests")

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_imputer_serialization_fails_with_strategy_set_to_most_frequent(self):
        imputer = SimpleImputer(strategy='most_frequent')
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))

        with self.assertRaises(NotImplementedError):
            imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

    def test_imputer_serialization_fails_with_strategy_set_to_constant(self):
        imputer = SimpleImputer(strategy='constant')
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))

        with self.assertRaises(NotImplementedError):
            imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

    def test_imputer_serialization_fails_with_add_indicator_set_to_true(self):
        imputer = SimpleImputer(add_indicator=True)
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))

        with self.assertRaises(NotImplementedError):
            imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

    def test_imputer_serialization_fails_when_fit_on_multiple_features(self):
        imputer = SimpleImputer()
        self.feature_extractor = FeatureExtractor(input_scalars=['a', 'b'], output_vector='ab_extracted')
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='ab_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))

        with self.assertRaises(NotImplementedError):
            imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

    def test_imputer_serialization_succeeds_with_strategy_set_to_mean(self):
        imputer = SimpleImputer(strategy='mean')
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))
        imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

        expected_model = {
            "op": "imputer",
            "attributes": {
                "surrogate_value": {
                    "double": self.df.a.mean()
                },
                "strategy": {
                    "string": "mean"
                }
            }
        }

        with open("{}/{}.node/model.json".format(self.tmp_dir, imputer.name)) as json_data:
            actual_model = json.load(json_data)

        self.assertEqual(expected_model, actual_model)

        with open("{}/{}.node/node.json".format(self.tmp_dir, imputer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(imputer.name, node['name'])
        self.assertEqual("a_extracted", node['shape']['inputs'][0]['name'])
        self.assertEqual("a_imputed", node['shape']['outputs'][0]['name'])

    def test_imputer_serialization_succeeds_with_strategy_set_to_median(self):
        imputer = SimpleImputer(strategy='median')
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(self.df))
        imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

        expected_model = {
            "op": "imputer",
            "attributes": {
                "surrogate_value": {
                    "double": self.df.a.median()
                },
                "strategy": {
                    "string": "median"
                }
            }
        }

        with open("{}/{}.node/model.json".format(self.tmp_dir, imputer.name)) as json_data:
            actual_model = json.load(json_data)

        self.assertEqual(expected_model, actual_model)

        with open("{}/{}.node/node.json".format(self.tmp_dir, imputer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(imputer.name, node['name'])
        self.assertEqual("a_extracted", node['shape']['inputs'][0]['name'])
        self.assertEqual("a_imputed", node['shape']['outputs'][0]['name'])

    def test_imputer_serialization_succeeds_with_missing_values_set_to_zero(self):
        df2 = self.df.fillna(0)

        imputer = SimpleImputer(strategy='mean', missing_values=0.0)
        imputer.mlinit(prior_tf=self.feature_extractor, output_features='a_imputed')

        imputer.fit(self.feature_extractor.transform(df2))
        imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

        expected_model = {
            "op": "imputer",
            "attributes": {
                "surrogate_value": {
                    "double": self.df.a.mean(),
                },
                "strategy": {
                    "string": "mean",
                },
                "missing_value": {
                    "double": 0.0,
                }
            }
        }

        with open("{}/{}.node/model.json".format(self.tmp_dir, imputer.name)) as json_data:
            actual_model = json.load(json_data)

        self.assertEqual(expected_model, actual_model)

        with open("{}/{}.node/node.json".format(self.tmp_dir, imputer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(imputer.name, node['name'])
        self.assertEqual("a_extracted", node['shape']['inputs'][0]['name'])
        self.assertEqual("a_imputed", node['shape']['outputs'][0]['name'])
