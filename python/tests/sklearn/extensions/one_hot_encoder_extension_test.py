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

from mleap.sklearn.extensions.data import OneHotEncoder
from mleap.sklearn.preprocessing.data import LabelEncoder


class TestOneHotEncoderExtension(unittest.TestCase):
    def setUp(self):
        labels = ['a', 'b', 'c', 'a', 'b', 'b']
        self.le = LabelEncoder(input_features=['label'], output_features='label_le_encoded')
        self.oh_data = self.le.fit_transform(labels).reshape(-1, 1)
        self.tmp_dir = tempfile.mkdtemp(prefix="mleap.python.tests")

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_one_hot_encoder_extension_serialization_fails_when_drop_last_is_set_to_true_and_handle_unknown_is_set_to_ignore(self):
        ohe = OneHotEncoder(input_features=self.le.output_features,
                            output_features='{}_one_hot_encoded'.format(self.le.output_features),
                            drop_last=True,
                            handle_unknown='ignore')
        ohe.fit(self.oh_data)

        with self.assertRaises(NotImplementedError):
            ohe.serialize_to_bundle(self.tmp_dir, ohe.name)

    def test_one_hot_encoder_extension_serialization_succeeds_when_drop_last_is_set_to_false_and_handle_unknown_is_set_to_ignore(self):
        ohe = OneHotEncoder(input_features=self.le.output_features,
                            output_features='{}_one_hot_encoded'.format(self.le.output_features),
                            drop_last=False,
                            handle_unknown='ignore')
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)
        with open("{}/{}.node/model.json".format(self.tmp_dir, ohe.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('one_hot_encoder', model['op'])
        self.assertEqual(3, model['attributes']['size']['long'])
        self.assertEqual('keep', model['attributes']['handle_invalid']['string'])
        self.assertEqual(True, model['attributes']['drop_last']['boolean'])  # `drop_last` has to be true to replicate scikit-learn's `handle_unknown=ignore` behavior

    def test_one_hot_encoder_extension_serialization_succeeds_when_drop_last_is_set_to_true_and_handle_unknown_is_set_to_error(self):
        ohe = OneHotEncoder(input_features=self.le.output_features,
                            output_features='{}_one_hot_encoded'.format(self.le.output_features),
                            drop_last=True,
                            handle_unknown='error')
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)
        with open("{}/{}.node/model.json".format(self.tmp_dir, ohe.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('one_hot_encoder', model['op'])
        self.assertEqual(3, model['attributes']['size']['long'])
        self.assertEqual('error', model['attributes']['handle_invalid']['string'])
        self.assertEqual(True, model['attributes']['drop_last']['boolean'])

    def test_one_hot_encoder_extension_serialization_succeeds_when_drop_last_is_set_to_false_and_handle_unknown_is_set_to_error(self):
        ohe = OneHotEncoder(input_features=self.le.output_features,
                            output_features='{}_one_hot_encoded'.format(self.le.output_features),
                            drop_last=False,
                            handle_unknown='error')
        ohe.fit(self.oh_data)

        ohe.serialize_to_bundle(self.tmp_dir, ohe.name)
        with open("{}/{}.node/model.json".format(self.tmp_dir, ohe.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual('one_hot_encoder', model['op'])
        self.assertEqual(3, model['attributes']['size']['long'])
        self.assertEqual('error', model['attributes']['handle_invalid']['string'])
        self.assertEqual(False, model['attributes']['drop_last']['boolean'])
    