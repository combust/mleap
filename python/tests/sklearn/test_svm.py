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

import numpy as np
import mleap.sklearn.svm
from sklearn.datasets import load_breast_cancer
from sklearn.svm import LinearSVC


class TestSVM(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_svc_serializer(self):
        X, y = load_breast_cancer(return_X_y=True)

        svc = LinearSVC()
        svc.fit(X, y)

        svc.mlinit(input_features='features', prediction_column='prediction')
        svc.serialize_to_bundle(self.tmp_dir, svc.name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, svc.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(model['op'], 'svm')
        self.assertEqual(len(model['attributes']['coefficients']['double']), 30)
        self.assertEqual(model['attributes']['num_classes']['long'], 2)
        self.assertTrue(model['attributes']['intercept']['double'] is not None)

    def test_svc_deserializer(self):
        X, y = load_breast_cancer(return_X_y=True)

        svc = LinearSVC()
        svc.fit(X, y)

        svc.mlinit(input_features='features', prediction_column='prediction')
        svc.serialize_to_bundle(self.tmp_dir, svc.name)

        svc_ds = LinearSVC()
        node_name = "{}.node".format(svc.name)
        svc_ds = svc_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        expected = svc.predict(X)
        actual = svc_ds.predict(X)

        np.testing.assert_array_equal(expected, actual)
