
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

import unittest
import pandas as pd
import numpy as np
import os
import shutil
import json
import uuid

from sklearn.linear_model import LinearRegression, LogisticRegression, LogisticRegressionCV
from sklearn.preprocessing import Binarizer
from mleap.sklearn.base import LinearRegression
from mleap.sklearn.logistic import LogisticRegression, LogisticRegressionCV
from mleap.sklearn.preprocessing.data import FeatureExtractor, Binarizer


class TransformerTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        pass

    def test_linear_regression_serializer(self):

        linear_regression = LinearRegression(fit_intercept=True, normalize=False)
        linear_regression.mlinit(input_features='a',
                                 prediction_column='e')

        linear_regression.fit(self.df[['a']], self.df[['e']])

        linear_regression.serialize_to_bundle(self.tmp_dir, linear_regression.name)


        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, linear_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(model['op'], 'linear_regression')
        self.assertTrue(model['attributes']['intercept']['double'] is not None)

    def test_linear_regression_deserializer(self):

        linear_regression = LinearRegression(fit_intercept=True, normalize=False)
        linear_regression.mlinit(input_features='a',
                                 prediction_column='e')

        linear_regression.fit(self.df[['a']], self.df[['e']])

        linear_regression.serialize_to_bundle(self.tmp_dir, linear_regression.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, linear_regression.name)) as json_data:
            model = json.load(json_data)

        # Now deserialize it back
        node_name = "{}.node".format(linear_regression.name)
        linear_regression_tf = LinearRegression()
        linear_regression_tf = linear_regression_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        res_a = linear_regression.predict(self.df[['a']])
        res_b = linear_regression_tf.predict(self.df[['a']])

        self.assertEqual(res_a[0], res_b[0])
        self.assertEqual(res_a[1], res_b[1])
        self.assertEqual(res_a[2], res_b[2])

    def test_logistic_regression_serializer(self):

        logistic_regression = LogisticRegression(fit_intercept=True)
        logistic_regression.mlinit(input_features='a',
                                 prediction_column='e_binary')

        extract_features = ['e']
        feature_extractor = FeatureExtractor(input_scalars=['e'],
                                         output_vector='extracted_e_output',
                                         output_vector_items=["{}_out".format(x) for x in extract_features])

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(prior_tf=feature_extractor,
                         output_features='e_binary')

        Xres = binarizer.fit_transform(self.df[['a']])

        logistic_regression.fit(self.df[['a']], Xres)

        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)


        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(model['op'], 'logistic_regression')
        self.assertTrue(model['attributes']['intercept']['double'] is not None)

    def test_logistic_regression_deserializer(self):

        logistic_regression = LogisticRegression(fit_intercept=True)
        logistic_regression.mlinit(input_features='a',
                                   prediction_column='e_binary')

        extract_features = ['e']
        feature_extractor = FeatureExtractor(input_scalars=['e'],
                                         output_vector='extracted_e_output',
                                         output_vector_items=["{}_out".format(x) for x in extract_features])

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(prior_tf=feature_extractor,
                         output_features='e_binary')

        Xres = binarizer.fit_transform(self.df[['a']])

        logistic_regression.fit(self.df[['a']], Xres)

        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        # Now deserialize it back
        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_tf = LogisticRegression()
        logistic_regression_tf = logistic_regression_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        res_a = logistic_regression.predict(self.df[['a']])
        res_b = logistic_regression_tf.predict(self.df[['a']])

        self.assertEqual(res_a[0], res_b[0])
        self.assertEqual(res_a[1], res_b[1])
        self.assertEqual(res_a[2], res_b[2])

    def test_logistic_regression_cv_serializer(self):

        logistic_regression = LogisticRegressionCV(fit_intercept=True)
        logistic_regression.mlinit(input_features='a',
                                 prediction_column='e_binary')

        extract_features = ['e']
        feature_extractor = FeatureExtractor(input_scalars=['e'],
                                             output_vector='extracted_e_output',
                                             output_vector_items=["{}_out".format(x) for x in extract_features])

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(prior_tf=feature_extractor,
                         output_features='e_binary')

        Xres = binarizer.fit_transform(self.df[['a']])

        logistic_regression.fit(self.df[['a']], Xres)

        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)


        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(model['op'], 'logistic_regression')
        self.assertTrue(model['attributes']['intercept']['double'] is not None)

    def test_logistic_regression_cv_deserializer(self):

        logistic_regression = LogisticRegressionCV(fit_intercept=True)
        logistic_regression.mlinit(input_features='a',
                                   prediction_column='e_binary')

        extract_features = ['e']
        feature_extractor = FeatureExtractor(input_scalars=['e'],
                                             output_vector='extracted_e_output',
                                             output_vector_items=["{}_out".format(x) for x in extract_features])

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(prior_tf=feature_extractor,
                         output_features='e_binary')

        Xres = binarizer.fit_transform(self.df[['a']])

        logistic_regression.fit(self.df[['a']], Xres)

        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        # Now deserialize it back
        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_tf = LogisticRegressionCV()
        logistic_regression_tf = logistic_regression_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        res_a = logistic_regression.predict(self.df[['a']])
        res_b = logistic_regression_tf.predict(self.df[['a']])

        self.assertEqual(res_a[0], res_b[0])
        self.assertEqual(res_a[1], res_b[1])
        self.assertEqual(res_a[2], res_b[2])
