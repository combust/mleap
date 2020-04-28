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

from mleap.sklearn.preprocessing.data import LabelEncoder
from mleap.sklearn.preprocessing.data import OneHotEncoder


class TestOneHotEncoder(unittest.TestCase):
    def setUp(self):
        labels = ['a', 'b', 'c', 'a', 'b', 'b']
        self.le = LabelEncoder(input_features=['label'], output_features='label_le_encoded')
        self.oh_data = self.le.fit_transform(labels).reshape(-1, 1)
        self.tmp_dir = tempfile.mkdtemp(prefix="mleap.python.tests")

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_one_hot_encoder_serialization_fails_on_multiple_feature_columns(self):
        self.oh_data = np.hstack((self.oh_data, self.oh_data))  # make two feature columns

        ohe = OneHotEncoder(handle_unknown='error')
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        with self.assertRaises(NotImplementedError):
            ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

    def test_one_hot_encoder_serialization_fails_on_an_invalid_category_range(self):
        self.oh_data[2][0] = 3  # make invalid category range

        ohe = OneHotEncoder(handle_unknown='error')
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        with self.assertRaises(ValueError):
            ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

    def test_one_hot_encoder_serialization_fails_when_using_the_drop_param(self):
        ohe = OneHotEncoder(handle_unknown='error', drop='first')  # try to use `drop` parameter
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        with self.assertRaises(NotImplementedError):
            ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

    def test_one_hot_encoder_serialization_fails_when_using_the_dtype_param(self):
        ohe = OneHotEncoder(handle_unknown='error', dtype=int)  # try to use `dtype` parameter
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        with self.assertRaises(NotImplementedError):
            ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

    def test_one_hot_encoder_serialization_succeeds_when_handle_unknown_is_set_to_error(self):
        ohe = OneHotEncoder(handle_unknown='error')
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)
        with open("{}/{}.node/model.json".format(self.tmp_dir, ohe.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('one_hot_encoder', model['op'])
        self.assertEqual(3, model['attributes']['size']['long'])
        self.assertEqual('error', model['attributes']['handle_invalid']['string'])
        self.assertEqual(False, model['attributes']['drop_last']['boolean'])

    def test_one_hot_encoder_deserialization_succeeds_when_handle_unknown_is_set_to_error(self):
        ohe = OneHotEncoder(handle_unknown='error')
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

        node_name = "{}.node".format(ohe.name)
        ohe_ds = OneHotEncoder()
        ohe_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        self.oh_data[2][0] = 3  # Add an unknown category

        with self.assertRaises(ValueError):
            ohe_ds.transform(self.oh_data)

    def test_one_hot_encoder_serialization_succeeds_when_handle_unknown_is_set_to_ignore(self):
        labels = ['a', 'b', 'c', 'a', 'b', 'b']

        le = LabelEncoder(input_features=['label'], output_features='label_le_encoded')
        oh_data = le.fit_transform(labels).reshape(-1, 1)

        ohe = OneHotEncoder(handle_unknown='ignore')
        ohe.mlinit(prior_tf=le, output_features='{}_one_hot_encoded'.format(le.output_features))
        ohe.fit(oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)
        with open("{}/{}.node/model.json".format(self.tmp_dir, ohe.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('one_hot_encoder', model['op'])
        self.assertEqual(3, model['attributes']['size']['long'])
        self.assertEqual('keep', model['attributes']['handle_invalid']['string'])
        self.assertEqual(True, model['attributes']['drop_last']['boolean'])

    def test_one_hot_encoder_deserialization_succeeds_when_handle_unknown_is_set_to_ignore(self):
        ohe = OneHotEncoder(handle_unknown='ignore')
        ohe.mlinit(prior_tf=self.le, output_features='{}_one_hot_encoded'.format(self.le.output_features))
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

        node_name = "{}.node".format(ohe.name)
        ohe_ds = OneHotEncoder()
        ohe_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        self.oh_data[2][0] = 3  # Add an unknown category

        expected = ohe.transform(self.oh_data).todense()
        actual = ohe_ds.transform(self.oh_data)

        np.testing.assert_array_equal(expected, actual)
