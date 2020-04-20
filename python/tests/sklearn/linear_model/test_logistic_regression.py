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
import os
import shutil
import unittest
import uuid

from numpy.testing import assert_array_equal
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split

from mleap.sklearn.linear_model import LogisticRegression
from mleap.sklearn.linear_model import LogisticRegressionCV


class TestLogisticRegression(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        pass

    def test_logistic_regression_serializer(self):
        logistic_regression = LogisticRegression()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=2)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual(logistic_regression.coef_.shape, (1, 100))
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('logistic_regression', model['op'])
        self.assertEqual(2, model['attributes']['num_classes']['long'])

        # assert coefficient matrix has shape (1,100)
        self.assertEqual(100, len(model['attributes']['coefficients']['double']))
        self.assertEqual(2, len(model['attributes']['coefficients']['shape']['dimensions']))
        self.assertEqual(1, model['attributes']['coefficients']['shape']['dimensions'][0]['size'])
        self.assertEqual(100, model['attributes']['coefficients']['shape']['dimensions'][1]['size'])

        # assert intercept vector has shape (1,)
        self.assertEqual(1, len(model['attributes']['intercept']['double']))
        self.assertEqual(1, len(model['attributes']['intercept']['shape']['dimensions']))
        self.assertEqual(1, model['attributes']['intercept']['shape']['dimensions'][0]['size'])

    def test_logistic_regression_deserializer(self):
        logistic_regression = LogisticRegression()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=2)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual(logistic_regression.coef_.shape, (1, 100))
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_ds = LogisticRegression()
        logistic_regression_ds = logistic_regression_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        expected = logistic_regression.predict(X_test)
        actual = logistic_regression_ds.predict(X_test).flatten()

        expected_proba = logistic_regression.predict_proba(X_test)
        actual_proba = logistic_regression_ds.predict_proba(X_test)

        assert_array_equal(expected, actual)
        assert_array_equal(expected_proba, actual_proba)

    def test_multinomial_logistic_regression_serializer(self):
        logistic_regression = LogisticRegression()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=3, n_informative=3)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual((3, 100), logistic_regression.coef_.shape, )
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('logistic_regression', model['op'])
        self.assertEqual(3, model['attributes']['num_classes']['long'])

        # assert coefficient matrix has shape (3,100)
        self.assertEqual(300, len(model['attributes']['coefficient_matrix']['double']))
        self.assertEqual(2, len(model['attributes']['coefficient_matrix']['shape']['dimensions']))
        self.assertEqual(3, model['attributes']['coefficient_matrix']['shape']['dimensions'][0]['size'])
        self.assertEqual(100, model['attributes']['coefficient_matrix']['shape']['dimensions'][1]['size'])

        # assert intercept vector has shape (3,)
        self.assertEqual(3, len(model['attributes']['intercept_vector']['double']))
        self.assertEqual(1, len(model['attributes']['intercept_vector']['shape']['dimensions']))
        self.assertEqual(3, model['attributes']['intercept_vector']['shape']['dimensions'][0]['size'])

    def test_multinomial_logistic_regression_deserializer(self):
        logistic_regression = LogisticRegression()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=3, n_informative=3)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual((3, 100), logistic_regression.coef_.shape, )
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_ds = LogisticRegression()
        logistic_regression_ds = logistic_regression_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        expected = logistic_regression.predict(X_test)
        actual = logistic_regression_ds.predict(X_test).flatten()

        expected_proba = logistic_regression.predict_proba(X_test)
        actual_proba = logistic_regression_ds.predict_proba(X_test)

        assert_array_equal(expected, actual)
        assert_array_equal(expected_proba, actual_proba)

    def test_logistic_regression_cv_serializer(self):
        logistic_regression = LogisticRegressionCV()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=2)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual(logistic_regression.coef_.shape, (1, 100))
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('logistic_regression', model['op'])
        self.assertEqual(2, model['attributes']['num_classes']['long'])

        # assert coefficient matrix has shape (1,100)
        self.assertEqual(100, len(model['attributes']['coefficients']['double']))
        self.assertEqual(2, len(model['attributes']['coefficients']['shape']['dimensions']))
        self.assertEqual(1, model['attributes']['coefficients']['shape']['dimensions'][0]['size'])
        self.assertEqual(100, model['attributes']['coefficients']['shape']['dimensions'][1]['size'])

        # assert intercept vector has shape (1,)
        self.assertEqual(1, len(model['attributes']['intercept']['double']))
        self.assertEqual(1, len(model['attributes']['intercept']['shape']['dimensions']))
        self.assertEqual(1, model['attributes']['intercept']['shape']['dimensions'][0]['size'])

    def test_logistic_regression_cv_deserializer(self):
        logistic_regression = LogisticRegressionCV()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=2)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual(logistic_regression.coef_.shape, (1, 100))
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_ds = LogisticRegressionCV()
        logistic_regression_ds = logistic_regression_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        expected = logistic_regression.predict(X_test)
        actual = logistic_regression_ds.predict(X_test).flatten()

        expected_proba = logistic_regression.predict_proba(X_test)
        actual_proba = logistic_regression_ds.predict_proba(X_test)

        assert_array_equal(expected, actual)
        assert_array_equal(expected_proba, actual_proba)

    def test_multinomial_logistic_regression_cv_serializer(self):
        logistic_regression = LogisticRegressionCV()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=3, n_informative=3)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual((3, 100), logistic_regression.coef_.shape, )
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, logistic_regression.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('logistic_regression', model['op'])
        self.assertEqual(3, model['attributes']['num_classes']['long'])

        # assert coefficient matrix has shape (3,100)
        self.assertEqual(300, len(model['attributes']['coefficient_matrix']['double']))
        self.assertEqual(2, len(model['attributes']['coefficient_matrix']['shape']['dimensions']))
        self.assertEqual(3, model['attributes']['coefficient_matrix']['shape']['dimensions'][0]['size'])
        self.assertEqual(100, model['attributes']['coefficient_matrix']['shape']['dimensions'][1]['size'])

        # assert intercept vector has shape (3,)
        self.assertEqual(3, len(model['attributes']['intercept_vector']['double']))
        self.assertEqual(1, len(model['attributes']['intercept_vector']['shape']['dimensions']))
        self.assertEqual(3, model['attributes']['intercept_vector']['shape']['dimensions'][0]['size'])

    def test_multinomial_logistic_regression_cv_deserializer(self):
        logistic_regression = LogisticRegressionCV()
        logistic_regression.mlinit(input_features='features', prediction_column='prediction')

        X, y = make_classification(n_features=100, n_classes=3, n_informative=3)
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        logistic_regression.fit(X_train, y_train)

        self.assertEqual((3, 100), logistic_regression.coef_.shape, )
        logistic_regression.serialize_to_bundle(self.tmp_dir, logistic_regression.name)

        node_name = "{}.node".format(logistic_regression.name)
        logistic_regression_ds = LogisticRegressionCV()
        logistic_regression_ds = logistic_regression_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        expected = logistic_regression.predict(X_test)
        actual = logistic_regression_ds.predict(X_test).flatten()

        expected_proba = logistic_regression.predict_proba(X_test)
        actual_proba = logistic_regression_ds.predict_proba(X_test)

        assert_array_equal(expected, actual)
        assert_array_equal(expected_proba, actual_proba)
