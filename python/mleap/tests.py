
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

import mleap.sklearn.preprocessing.data
from mleap.sklearn.preprocessing.data import FeatureExtractor, MathUnary, MathBinary
from sklearn.preprocessing.data import StandardScaler, MinMaxScaler, LabelEncoder, Imputer, Binarizer, PolynomialFeatures


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

        standard_scaler = StandardScaler(with_mean=True,
                                         with_std=True
                                         )

        standard_scaler.mlinit(input_features=['a'],
                               output_features=['a_scaled'])

        standard_scaler.fit(self.df[['a']])

        standard_scaler.serialize_to_bundle(self.tmp_dir, standard_scaler.name)

        expected_mean = self.df.a.mean()
        expected_std = np.sqrt(np.var(self.df.a))

        expected_model = {
            "op": "standard_scaler",
            "attributes": {
                "mean": {
                    "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "value": [expected_mean]
                },
                "std": {
                    "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double",
                           "dimensions": [
                              -1
                           ]
                        }
                     },
                     "value": [expected_std]
                }
            }
        }

        self.assertEqual(expected_mean, standard_scaler.mean_.tolist()[0])
        self.assertEqual(expected_std, np.sqrt(standard_scaler.var_.tolist()[0]))

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(standard_scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['mean']['type']['tensor']['dimensions'][0], model['attributes']['mean']['type']['tensor']['dimensions'][0])
        self.assertEqual(expected_model['attributes']['std']['type']['tensor']['dimensions'][0], model['attributes']['std']['type']['tensor']['dimensions'][0])
        self.assertEqual(expected_model['attributes']['mean']['value'][0], model['attributes']['mean']['value'][0])
        self.assertEqual(expected_model['attributes']['std']['value'][0], model['attributes']['std']['value'][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(standard_scaler.name, node['name'])
        self.assertEqual(standard_scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(standard_scaler.output_features, node['shape']['outputs'][0]['name'])

    def test_min_max_scaler(self):

        scaler = MinMaxScaler()
        scaler.mlinit(input_features=['a'],
                      output_features=['a_scaled'])

        scaler.fit(self.df[['a']])

        scaler.serialize_to_bundle(self.tmp_dir, scaler.name)

        expected_min = self.df.a.min()
        expected_max = self.df.a.max()

        expected_model = {
           "op": "min_max_scaler",
            "attributes": {
                "min": {
                         "type": {
                            "type": "tensor",
                            "tensor": {
                               "base": "double",
                               "dimensions": [
                                  -1
                               ]
                            }
                         },
                         "value": [expected_min]
                      },
                "max": {
                         "type": {
                            "type": "tensor",
                            "tensor": {
                               "base": "double",
                               "dimensions": [
                                  -1
                               ]
                            }
                         },
                         "value": [expected_max]
                      }
            }
        }

        self.assertEqual(expected_min, scaler.data_min_.tolist()[0])
        self.assertEqual(expected_max, scaler.data_max_.tolist()[0])

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['min']['type']['tensor']['dimensions'][0], model['attributes']['min']['type']['tensor']['dimensions'][0])
        self.assertEqual(expected_model['attributes']['min']['value'][0], model['attributes']['min']['value'][0])
        self.assertEqual(expected_model['attributes']['max']['value'][0], model['attributes']['max']['value'][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(scaler.name, node['name'])
        self.assertEqual(scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(scaler.output_features, node['shape']['outputs'][0]['name'])

    def label_encoder_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder()
        le.mlinit(input_features=['label_feature'],
                  output_features=['label_feature_le_encoded'])

        le.fit(labels)

        self.assertEqual(labels, le.classes_.tolist())

        le.serialize_to_bundle(self.tmp_dir, le.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, le.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(le.op, model['op'])
        self.assertEqual('labels', model['attributes'].keys()[0])

    def feature_extractor_test(self):

        extract_features = ['a', 'd']

        feature_extractor = FeatureExtractor(input_features=extract_features,
                                             output_vector='extract_features_output',
                                             output_vector_items=["{}_out".format(x) for x in extract_features])

        res = feature_extractor.fit_transform(self.df)

        self.assertEqual(len(res.columns), 2)

    def imputer_test(self):

        def _set_nulls(df):
            row = df['index']
            if row in [2,5]:
                return np.NaN
            return df.a

        imputer = Imputer(strategy='mean')
        imputer.mlinit(input_features=['a'],
                       output_features=['a_imputed'])

        df2 = self.df
        df2.reset_index(inplace=True)
        df2['a'] = df2.apply(_set_nulls, axis=1)

        imputer.fit(df2[['a']])

        self.assertEqual(imputer.statistics_[0], df2.a.mean())

        imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

        expected_model = {
          "op": "imputer",
          "attributes": {
            "surrogate_value": {
              "type": "double",
              "value": df2.a.mean()
            },
            "strategy": {
              "type": "string",
              "value": "mean"
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, imputer.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['strategy']['value'], model['attributes']['strategy']['value'])
        self.assertEqual(expected_model['attributes']['surrogate_value']['value'], model['attributes']['surrogate_value']['value'])

    def binarizer_test(self):

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(input_features=['a'],
                         output_features=['a_binary'])

        Xres = binarizer.fit_transform(self.df[['a']])

        # Test that the binarizer functions as expected
        self.assertEqual(float(len(self.df[self.df.a >= 0]))/10.0, Xres.mean())

        binarizer.serialize_to_bundle(self.tmp_dir, binarizer.name)

        expected_model = {
          "op": "binarizer",
          "attributes": {
            "threshold": {
              "type": "double",
              "value": 0.0
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, binarizer.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['threshold']['value'], model['attributes']['threshold']['value'])

    def polynomial_expansion_test(self):

        polynomial_exp = PolynomialFeatures(degree=2, include_bias=False)
        polynomial_exp.mlinit(input_features=['a', 'b'],
                              output_features=['a', 'b', 'a_sqd', 'a_mult_b', 'b_sqd'])

        Xres = polynomial_exp.fit_transform(self.df[['a', 'b']])

        self.assertEqual(Xres[0][2], Xres[0][0] * Xres[0][0])

        polynomial_exp.serialize_to_bundle(self.tmp_dir, polynomial_exp.name)

        expected_model = {
          "op": "polynomial_expansion",
          "attributes": {
            "degree": {
              "type": "double",
              "value": 2
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, polynomial_exp.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['degree']['value'], model['attributes']['degree']['value'])

    def math_unary_exp_test(self):

        math_unary_tf = MathUnary(input_features=['a'], output_features=['log_a'], transform_type='exp')

        Xres = math_unary_tf.fit_transform(self.df.a)

        self.assertEqual(np.exp(self.df.a[0]), Xres[0])

        math_unary_tf.serialize_to_bundle(math_unary_tf, self.tmp_dir, math_unary_tf.name)

        expected_model = {
          "op": "math_unary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'exp'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])

    def math_unary_sin_test(self):

        math_unary_tf = MathUnary(input_features=['a'], output_features=['cos_a'], transform_type='sin')

        Xres = math_unary_tf.fit_transform(self.df.a)

        self.assertEqual(np.sin(self.df.a[0]), Xres[0])

        math_unary_tf.serialize_to_bundle(math_unary_tf, self.tmp_dir, math_unary_tf.name)

        expected_model = {
          "op": "math_unary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'sin'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])

    def math_binary_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_plus_b'], transform_type='add')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual( self.df.a[0] + self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(math_binary_tf, self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'add'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])

    def math_binary_subtract_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_less_b'], transform_type='sub')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] - self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(math_binary_tf, self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'sub'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])

    def math_binary_multiply_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_mul_b'], transform_type='mul')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] * self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(math_binary_tf, self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'mul'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])

    def math_binary_divide_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_mul_b'], transform_type='div')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] / self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(math_binary_tf, self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "opperation": {
              "type": "string",
              "value": 'div'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['opperation']['value'], model['attributes']['opperation']['value'])
