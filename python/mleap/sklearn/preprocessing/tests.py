
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
from mleap.sklearn.preprocessing.data import FeatureExtractor, MathUnary, MathBinary, StringMap
from mleap.sklearn.preprocessing.data import StandardScaler, MinMaxScaler, LabelEncoder, Imputer, Binarizer, PolynomialFeatures
from mleap.sklearn.preprocessing.data import OneHotEncoder


class TransformerTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_standard_scaler_serializer(self):

        standard_scaler = StandardScaler(with_mean=True,
                                         with_std=True
                                         )

        standard_scaler.mlinit(input_features='a',
                               output_features='a_scaled')

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
                           "base": "double"
                        }
                     },
                     "value": {
                         "values": [expected_mean],
                         "dimensions": [
                             1
                         ]
                     }
                },
                "std": {
                    "type": {
                        "type": "tensor",
                        "tensor": {
                           "base": "double"
                        }
                     },
                     "value": {
                         "values": [expected_std],
                         "dimensions": [
                             1
                         ]
                     }
                }
            }
        }

        self.assertAlmostEqual(expected_mean, standard_scaler.mean_.tolist()[0], places = 7)
        self.assertAlmostEqual(expected_std, np.sqrt(standard_scaler.var_.tolist()[0]), places = 7)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(standard_scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['mean']['value']['dimensions'][0], model['attributes']['mean']['value']['dimensions'][0])
        self.assertEqual(expected_model['attributes']['std']['value']['dimensions'][0], model['attributes']['std']['value']['dimensions'][0])
        self.assertAlmostEqual(expected_model['attributes']['mean']['value']['values'][0], model['attributes']['mean']['value']['values'][0], places = 7)
        self.assertAlmostEqual(expected_model['attributes']['std']['value']['values'][0], model['attributes']['std']['value']['values'][0], places = 7)

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, standard_scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(standard_scaler.name, node['name'])
        self.assertEqual(standard_scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(standard_scaler.output_features, node['shape']['outputs'][0]['name'])

    def test_standard_scaler_deserializer(self):

        # Serialize a standard scaler to a bundle
        standard_scaler = StandardScaler(with_mean=True,
                                         with_std=True
                                         )

        standard_scaler.mlinit(input_features=['a'],
                               output_features=['a_scaled'])

        standard_scaler.fit(self.df[['a']])

        standard_scaler.serialize_to_bundle(self.tmp_dir, standard_scaler.name)

        # Now deserialize it back

        node_name = "{}.node".format(standard_scaler.name)

        standard_scaler_tf = StandardScaler()

        standard_scaler_tf = standard_scaler_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = standard_scaler.transform(self.df[['a']])
        res_b = standard_scaler_tf.transform(self.df[['a']])

        self.assertEqual(res_a[0], res_b[0])
        self.assertEqual(standard_scaler.name, standard_scaler_tf.name)
        self.assertEqual(standard_scaler.op, standard_scaler_tf.op)
        self.assertEqual(standard_scaler.mean_, standard_scaler_tf.mean_)
        self.assertEqual(standard_scaler.scale_, standard_scaler_tf.scale_)

    def test_standard_scaler_multi_deserializer(self):

        # Serialize a standard scaler to a bundle
        standard_scaler = StandardScaler(with_mean=True,
                                         with_std=True
                                         )

        standard_scaler.mlinit(input_features=['a', 'b'],
                               output_features=['a_scaled', 'b_scaled'])

        standard_scaler.fit(self.df[['a', 'b']])

        standard_scaler.serialize_to_bundle(self.tmp_dir, standard_scaler.name)

        # Now deserialize it back

        node_name = "{}.node".format(standard_scaler.name)

        standard_scaler_tf = StandardScaler()

        standard_scaler_tf = standard_scaler_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = standard_scaler.transform(self.df[['a', 'b']])
        res_b = standard_scaler_tf.transform(self.df[['a', 'b']])

        self.assertEqual(res_a[0][0], res_b[0][0])
        self.assertEqual(res_a[0][1], res_b[0][1])
        self.assertEqual(standard_scaler.name, standard_scaler_tf.name)
        self.assertEqual(standard_scaler.op, standard_scaler_tf.op)
        self.assertEqual(standard_scaler.mean_[0], standard_scaler_tf.mean_[0])
        self.assertEqual(standard_scaler.mean_[1], standard_scaler_tf.mean_[1])
        self.assertEqual(standard_scaler.scale_[0], standard_scaler_tf.scale_[0])
        self.assertEqual(standard_scaler.scale_[1], standard_scaler_tf.scale_[1])

    def test_min_max_scaler_serializer(self):

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

    def test_min_max_scaler_serializer(self):

        scaler = MinMaxScaler()
        scaler.mlinit(input_features='a',
                      output_features='a_scaled')

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
                               "base": "double"
                            }
                         },
                         "value": {
                             "values": [expected_min],
                             "dimensions": [
                                 1
                             ]
                         }
                      },
                "max": {
                         "type": {
                            "type": "tensor",
                            "tensor": {
                               "base": "double"
                            }
                         },
                         "value": {
                             "values": [expected_max],
                             "dimensions": [
                                 1
                             ]
                         }
                      }
            }
        }

        self.assertAlmostEqual(expected_min, scaler.data_min_.tolist()[0], places = 7)
        self.assertAlmostEqual(expected_max, scaler.data_max_.tolist()[0], places = 7)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, scaler.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(scaler.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['min']['value']['dimensions'][0], model['attributes']['min']['value']['dimensions'][0])
        self.assertEqual(expected_model['attributes']['max']['value']['dimensions'][0], model['attributes']['max']['value']['dimensions'][0])
        self.assertAlmostEqual(expected_model['attributes']['min']['value']['values'][0], model['attributes']['min']['value']['values'][0])
        self.assertAlmostEqual(expected_model['attributes']['max']['value']['values'][0], model['attributes']['max']['value']['values'][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, scaler.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(scaler.name, node['name'])
        self.assertEqual(scaler.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(scaler.output_features, node['shape']['outputs'][0]['name'])

    def test_min_max_scaler_deserializer(self):

        scaler = MinMaxScaler()
        scaler.mlinit(input_features=['a'],
                      output_features=['a_scaled'])

        scaler.fit(self.df[['a']])

        scaler.serialize_to_bundle(self.tmp_dir, scaler.name)

        # Deserialize the MinMaxScaler
        node_name = "{}.node".format(scaler.name)
        min_max_scaler_tf = MinMaxScaler()
        min_max_scaler_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = scaler.transform(self.df[['a']])
        res_b = min_max_scaler_tf.transform(self.df[['a']])

        self.assertEqual(res_a[0], res_b[0])

        self.assertEqual(scaler.name, min_max_scaler_tf.name)
        self.assertEqual(scaler.op, min_max_scaler_tf.op)

    def test_min_max_scaler_multi_deserializer(self):

        scaler = MinMaxScaler()
        scaler.mlinit(input_features=['a', 'b'],
                      output_features=['a_scaled', 'b_scaled'])

        scaler.fit(self.df[['a']])

        scaler.serialize_to_bundle(self.tmp_dir, scaler.name)

        # Deserialize the MinMaxScaler
        node_name = "{}.node".format(scaler.name)
        min_max_scaler_tf = MinMaxScaler()
        min_max_scaler_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = scaler.transform(self.df[['a', 'b']])
        res_b = min_max_scaler_tf.transform(self.df[['a', 'b']])

        self.assertEqual(res_a[0][0], res_b[0][0])
        self.assertEqual(res_a[0][1], res_b[0][1])

        self.assertEqual(scaler.name, min_max_scaler_tf.name)
        self.assertEqual(scaler.op, min_max_scaler_tf.op)

    def label_encoder_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder(input_features=['label_feature'],
                  output_features='label_feature_le_encoded')

        le.fit(labels)

        self.assertEqual(labels, le.classes_.tolist())

        le.serialize_to_bundle(self.tmp_dir, le.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, le.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(le.op, model['op'])
        self.assertEqual('labels', model['attributes'].keys()[0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, le.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(le.name, node['name'])
        self.assertEqual(le.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(le.output_features, node['shape']['outputs'][0]['name'])

    def label_encoder_deserializer_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder(input_features=['label_feature'],
                          output_features='label_feature_le_encoded')

        le.fit(labels)

        self.assertEqual(labels, le.classes_.tolist())

        le.serialize_to_bundle(self.tmp_dir, le.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, le.name)) as json_data:
            model = json.load(json_data)

        # Deserialize the LabelEncoder
        node_name = "{}.node".format(le.name)
        label_encoder_tf = LabelEncoder()
        label_encoder_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = le.transform(labels)
        res_b = label_encoder_tf.transform(labels)
        print("le.output_features: {}".format(le.output_features))
        print("label_encoder_tf.output_features: {}".format(label_encoder_tf.output_features))
        self.assertEqual(res_a[0], res_b[0])
        self.assertEqual(res_a[1], res_b[1])
        self.assertEqual(res_a[2], res_b[2])
        self.assertEqual(le.input_features, label_encoder_tf.input_features)
        self.assertEqual(le.output_features, label_encoder_tf.output_features[0])

    def one_hot_encoder_serializer_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder(input_features=['label_feature'],
                          output_features='label_feature_le_encoded')

        oh_data = le.fit_transform(labels).reshape(3, 1)

        one_hot_encoder_tf = OneHotEncoder(sparse=False)
        one_hot_encoder_tf.mlinit(input_features = le.output_features,
                                  output_features = '{}_one_hot_encoded'.format(le.output_features))
        one_hot_encoder_tf.fit(oh_data)

        one_hot_encoder_tf.serialize_to_bundle(self.tmp_dir, one_hot_encoder_tf.name)

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, one_hot_encoder_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(one_hot_encoder_tf.op, model['op'])
        self.assertEqual(3, model['attributes']['size']['value'])
        self.assertEqual(True, model['attributes']['drop_last']['value'])

    def one_hot_encoder_deserializer_test(self):

        labels = ['a', 'b', 'c']

        le = LabelEncoder(input_features=['label_feature'],
                          output_features='label_feature_le_encoded')

        oh_data = le.fit_transform(labels).reshape(3, 1)

        one_hot_encoder_tf = OneHotEncoder(sparse=False)
        one_hot_encoder_tf.mlinit(input_features = le.output_features,
                                  output_features=['{}_one_hot_encoded'.format(le.output_features[0])])
        one_hot_encoder_tf.fit(oh_data)

        one_hot_encoder_tf.serialize_to_bundle(self.tmp_dir, one_hot_encoder_tf.name)

        # Deserialize the OneHotEncoder
        node_name = "{}.node".format(one_hot_encoder_tf.name)
        one_hot_encoder_tf_ds = OneHotEncoder()
        one_hot_encoder_tf_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = one_hot_encoder_tf.transform(oh_data)
        res_b = one_hot_encoder_tf_ds.transform(oh_data)

        self.assertEqual(res_a[0][0], res_b[0][0])
        self.assertEqual(res_a[1][0], res_b[1][0])
        self.assertEqual(res_a[2][0], res_b[2][0])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, one_hot_encoder_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(one_hot_encoder_tf_ds.name, node['name'])
        self.assertEqual(one_hot_encoder_tf_ds.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(one_hot_encoder_tf_ds.output_features, node['shape']['outputs'][0]['name'])

    def feature_extractor_test(self):

        extract_features = ['a', 'd']

        feature_extractor = FeatureExtractor(input_features=extract_features,
                                             output_vector='extract_features_output',
                                             output_vector_items=["{}_out".format(x) for x in extract_features])

        res = feature_extractor.fit_transform(self.df)

        self.assertEqual(len(res.columns), 2)

        feature_extractor.serialize_to_bundle(self.tmp_dir, feature_extractor.name)

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, feature_extractor.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(feature_extractor.name, node['name'])
        self.assertEqual(feature_extractor.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(feature_extractor.input_features[1], node['shape']['inputs'][1]['name'])
        self.assertEqual(feature_extractor.output_vector, node['shape']['outputs'][0]['name'])

    def imputer_test(self):

        def _set_nulls(df):
            row = df['index']
            if row in [2,5]:
                return np.NaN
            return df.a

        imputer = Imputer(strategy='mean')
        imputer.mlinit(input_features='a',
                       output_features='a_imputed')

        df2 = self.df
        df2.reset_index(inplace=True)
        df2['a'] = df2.apply(_set_nulls, axis=1)

        imputer.fit(df2[['a']])

        self.assertAlmostEqual(imputer.statistics_[0], df2.a.mean(), places = 7)

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
        self.assertAlmostEqual(expected_model['attributes']['surrogate_value']['value'], model['attributes']['surrogate_value']['value'], places = 7)

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, imputer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(imputer.name, node['name'])
        self.assertEqual(imputer.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(imputer.output_features, node['shape']['outputs'][0]['name'])

    def binarizer_test(self):

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(input_features='a',
                         output_features='a_binary')

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

        self.assertEqual(expected_model['attributes']['threshold']['value'],
                         model['attributes']['threshold']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, binarizer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(binarizer.name, node['name'])
        self.assertEqual(binarizer.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(binarizer.output_features, node['shape']['outputs'][0]['name'])

    def binarizer_deserializer_test(self):

        binarizer = Binarizer(threshold=0.0)
        binarizer.mlinit(input_features=['a'],
                         output_features=['a_binary'])

        Xres = binarizer.fit_transform(self.df[['a']])

        # Test that the binarizer functions as expected
        self.assertEqual(float(len(self.df[self.df.a >= 0]))/10.0, Xres.mean())

        binarizer.serialize_to_bundle(self.tmp_dir, binarizer.name)

        # Deserialize the Binarizer
        node_name = "{}.node".format(binarizer.name)
        binarizer_tf_ds = Binarizer()
        binarizer_tf_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = binarizer.transform(self.df[['a']])
        res_b = binarizer_tf_ds.transform(self.df[['a']])

        self.assertEqual(res_a[0][0], res_b[0][0])
        self.assertEqual(res_a[1][0], res_b[1][0])
        self.assertEqual(res_a[2][0], res_b[2][0])
        self.assertEqual(res_a[3][0], res_b[3][0])

    def polynomial_expansion_test(self):

        polynomial_exp = PolynomialFeatures(degree=2, include_bias=False)
        polynomial_exp.mlinit(input_features='a',
                              output_features='poly')

        Xres = polynomial_exp.fit_transform(self.df[['a']])

        self.assertEqual(Xres[0][1], Xres[0][0] * Xres[0][0])

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

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, polynomial_exp.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(polynomial_exp.name, node['name'])
        self.assertEqual(polynomial_exp.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(polynomial_exp.output_features, node['shape']['outputs'][0]['name'])

    def polynomial_expansion_deserializer_test(self):

        polynomial_exp = PolynomialFeatures(degree=2, include_bias=False)
        polynomial_exp.mlinit(input_features=['a', 'b'],
                              output_features=['a', 'b', 'a_sqd', 'a_mult_b', 'b_sqd'])

        Xres = polynomial_exp.fit_transform(self.df[['a', 'b']])

        self.assertEqual(Xres[0][2], Xres[0][0] * Xres[0][0])

        polynomial_exp.serialize_to_bundle(self.tmp_dir, polynomial_exp.name)

        # Deserialize the PolynomialExpansion
        node_name = "{}.node".format(polynomial_exp.name)
        poly_tf_ds = PolynomialFeatures()
        poly_tf_ds.deserialize_from_bundle(self.tmp_dir, node_name)

        # Transform some sample data
        res_a = polynomial_exp.transform(self.df[['a', 'b']])
        res_b = poly_tf_ds.transform(self.df[['a', 'b']])

        self.assertEqual(res_a[0][0], res_b[0][0])
        self.assertEqual(res_a[1][0], res_b[1][0])
        self.assertEqual(res_a[2][0], res_b[2][0])
        self.assertEqual(res_a[3][0], res_b[3][0])
        self.assertEqual(res_a[0][1], res_b[0][1])
        self.assertEqual(res_a[1][1], res_b[1][1])
        self.assertEqual(res_a[2][1], res_b[2][1])
        self.assertEqual(res_a[3][1], res_b[3][1])

    def math_unary_exp_test(self):

        math_unary_tf = MathUnary(input_features=['a'], output_features=['log_a'], transform_type='exp')

        Xres = math_unary_tf.fit_transform(self.df.a)

        self.assertEqual(np.exp(self.df.a[0]), Xres[0])

        math_unary_tf.serialize_to_bundle(self.tmp_dir, math_unary_tf.name)

        expected_model = {
          "op": "math_unary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'exp'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_unary_tf.name, node['name'])
        self.assertEqual(math_unary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_unary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def math_unary_deserialize_exp_test(self):

        math_unary_tf = MathUnary(input_features=['a'], output_features=['log_a'], transform_type='exp')

        Xres = math_unary_tf.fit_transform(self.df.a)

        self.assertEqual(np.exp(self.df.a[0]), Xres[0])

        math_unary_tf.serialize_to_bundle(self.tmp_dir, math_unary_tf.name)

        node_name = "{}.node".format(math_unary_tf.name)
        math_unary_ds_tf = MathUnary()
        math_unary_ds_tf = math_unary_ds_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        with open("{}/{}.node/model.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            model = json.load(json_data)

        res_a = math_unary_tf.transform(self.df['a'])
        res_b = math_unary_ds_tf.transform(self.df['a'])

        self.assertEqual(res_a[0], res_b[0])

    def math_unary_sin_test(self):

        math_unary_tf = MathUnary(input_features=['a'], output_features=['sin_a'], transform_type='sin')

        Xres = math_unary_tf.fit_transform(self.df.a)

        self.assertEqual(np.sin(self.df.a[0]), Xres[0])

        math_unary_tf.serialize_to_bundle(self.tmp_dir, math_unary_tf.name)

        expected_model = {
          "op": "math_unary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'sin'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_unary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_unary_tf.name, node['name'])
        self.assertEqual(math_unary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_unary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def math_binary_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_plus_b'], transform_type='add')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual( self.df.a[0] + self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'add'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_binary_tf.name, node['name'])
        self.assertEqual(math_binary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_binary_tf.input_features[1], node['shape']['inputs'][1]['name'])
        self.assertEqual(math_binary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def math_binary_deserialize_exp_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_plus_b'], transform_type='add')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual( self.df.a[0] + self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(self.tmp_dir, math_binary_tf.name)

        node_name = "{}.node".format(math_binary_tf.name)
        math_binary_ds_tf = MathBinary()
        math_binary_ds_tf = math_binary_ds_tf.deserialize_from_bundle(self.tmp_dir, node_name)

        res_a = math_binary_tf.transform(self.df[['a', 'b']])
        res_b = math_binary_ds_tf.transform(self.df[['a', 'b']])

        self.assertEqual(res_a[0], res_b[0])

    def math_binary_subtract_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_less_b'], transform_type='sub')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] - self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'sub'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_binary_tf.name, node['name'])
        self.assertEqual(math_binary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_binary_tf.input_features[1], node['shape']['inputs'][1]['name'])
        self.assertEqual(math_binary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def math_binary_multiply_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_mul_b'], transform_type='mul')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] * self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'mul'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_binary_tf.name, node['name'])
        self.assertEqual(math_binary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_binary_tf.input_features[1], node['shape']['inputs'][1]['name'])
        self.assertEqual(math_binary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def math_binary_divide_test(self):

        math_binary_tf = MathBinary(input_features=['a', 'b'], output_features=['a_mul_b'], transform_type='div')

        Xres = math_binary_tf.fit_transform(self.df[['a', 'b']])

        self.assertEqual(self.df.a[0] / self.df.b[0], Xres[0])

        math_binary_tf.serialize_to_bundle(self.tmp_dir, math_binary_tf.name)

        expected_model = {
          "op": "math_binary",
          "attributes": {
            "operation": {
              "type": "string",
              "value": 'div'
            }
          }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['operation']['value'], model['attributes']['operation']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, math_binary_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(math_binary_tf.name, node['name'])
        self.assertEqual(math_binary_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(math_binary_tf.input_features[1], node['shape']['inputs'][1]['name'])
        self.assertEqual(math_binary_tf.output_features[0], node['shape']['outputs'][0]['name'])

    def string_map_test(self):

        df = pd.DataFrame(['test_one', 'test_two', 'test_one', 'test_one', 'test_two'], columns=['a'])
        string_map_tf = StringMap(input_features=['a'], output_features=['a_mapped'], labels={"test_one":1.0, "test_two": 0.0})

        Xres = string_map_tf.fit_transform(df)
        self.assertEqual(1.0, Xres[0])
        self.assertEqual(0.0, Xres[1])
        self.assertEqual(1.0, Xres[2])
        self.assertEqual(1.0, Xres[3])
        self.assertEqual(0.0, Xres[4])

        string_map_tf.serialize_to_bundle(self.tmp_dir, string_map_tf.name)
        #

        expected_model = {
            "op": "string_map",
            "attributes": {
                "labels": {
                    "type": {
                        "type": "list",
                        "base": "string"
                    },
                    "value": ["test_one", "test_two"]
                },
                "values": {
                    "type": {
                        "type": "list",
                        "base": "double"
                    },
                    "value": [1.0, 0.0]
                }
            }
        }
        #
        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, string_map_tf.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['labels']['value'], model['attributes']['labels']['value'])
        self.assertEqual(expected_model['attributes']['values']['value'], model['attributes']['values']['value'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, string_map_tf.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(string_map_tf.name, node['name'])
        self.assertEqual(string_map_tf.input_features[0], node['shape']['inputs'][0]['name'])
        self.assertEqual(string_map_tf.output_features[0], node['shape']['outputs'][0]['name'])
