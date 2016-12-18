
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
import uuid
import os
import shutil
import json

import mleap.sklearn.preprocessing.data
from sklearn.preprocessing.data import StandardScaler, MinMaxScaler, LabelEncoder


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

    def test_standard_scaler(self):

        standard_scaler = StandardScaler(with_mean=True, with_std=True)
        standard_scaler.set_input_features(['a'])
        standard_scaler.set_output_features('scaled_cont_features')

        standard_scaler.fit(self.df[['a']])

        standard_scaler.serialize_to_bundle(self.tmp_dir, standard_scaler.name)

        expected_mean = self.df.a.mean()
        expected_std = np.sqrt(np.var(self.df.a))

        expected_model = {
               "attributes": [
                  {
                     "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "name": "mean",
                     "value": [expected_mean]
                  },
                  {
                     "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "name": "std",
                     "value": [expected_std]
                  }
               ],
               "op": "standard_scaler"
            }

        self.assertEqual(expected_mean, standard_scaler.mean_.tolist()[0])
        self.assertEqual(expected_std, np.sqrt(standard_scaler.var_.tolist()[0]))

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(standard_scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes'][0]['type']['tensor']['dimensions'][0], model['attributes'][0]['type']['tensor']['dimensions'][0])
        self.assertEqual([x for x in expected_model['attributes'] if x['name'] == 'mean'][0], [x for x in model['attributes'] if x['name'] == 'mean'][0])
        self.assertEqual([x for x in expected_model['attributes'] if x['name'] == 'std'][0], [x for x in model['attributes'] if x['name'] == 'std'][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(standard_scaler.name, node['name'])
        self.assertEqual(standard_scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(standard_scaler.output_features, node['shape']['outputs'][0]['name'])

    def test_min_max_scaler(self):

        scaler = MinMaxScaler()
        scaler.set_input_features(['a'])
        scaler.set_output_features('scaled_cont_features')

        scaler.fit(self.df[['a']])

        scaler.serialize_to_bundle(self.tmp_dir, scaler.name)

        expected_min = self.df.a.min()
        expected_max = self.df.a.max()

        expected_model = {
               "attributes": [
                  {
                     "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "name": "min",
                     "value": [expected_min]
                  },
                  {
                     "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "name": "max",
                     "value": [expected_max]
                  }
               ],
               "op": "min_max_scaler"
            }

        self.assertEqual(expected_min, scaler.data_min_.tolist()[0])
        self.assertEqual(expected_max, scaler.data_max_.tolist()[0])

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes'][0]['type']['tensor']['dimensions'][0], model['attributes'][0]['type']['tensor']['dimensions'][0])
        self.assertEqual([x for x in expected_model['attributes'] if x['name'] == 'min'][0], [x for x in model['attributes'] if x['name'] == 'min'][0])
        self.assertEqual([x for x in expected_model['attributes'] if x['name'] == 'max'][0], [x for x in model['attributes'] if x['name'] == 'max'][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(scaler.name, node['name'])
        self.assertEqual(scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(scaler.output_features, node['shape']['outputs'][0]['name'])

    def label_encoder_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder()
        le.set_input_features('label_feature')
        le.set_output_features('label_feature_le_encoded')
        le.fit(labels)

        self.assertEqual(labels, le.classes_.tolist())

        le.serialize_to_bundle(self.tmp_dir, le.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, le.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(le.op, model['op'])
        self.assertEqual('labels', model['attributes'][0]['name'])
