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
from sklearn.preprocessing.data import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Imputer
from sklearn.preprocessing.data import OneHotEncoder
from sklearn.preprocessing.label import LabelEncoder
import numpy as np
import pandas as pd
import uuid
import os
import shutil
import json


class ops(object):
    def __init__(self):
        self.STANDARD_SCALER = 'standard_scaler'
        self.MIN_MAX_SCALER = 'min_max_scaler'
        self.LABEL_ENCODER = 'string_indexer'
        self.ONE_HOT_ENCODER = 'one_hot_encoder'
        self.IMPUTER = 'imputer'
        self.NDARRAYTODATAFRAME = 'one_dim_array_to_dataframe'
        self.TODENSE = 'dense_transformer'

ops = ops()

def get_mleap_model(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_model(self)

def get_mleap_node(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_node(self)

def set_input_features(self, input_features):
    serializer = SimpleSparkSerializer()
    return serializer.set_input_features(self, input_features)

def set_output_features(self, output_features):
    serializer = SimpleSparkSerializer()
    return serializer.set_output_features(self, output_features)

def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


setattr(StandardScaler, 'get_mleap_model', get_mleap_model)
setattr(StandardScaler, 'get_mleap_node', get_mleap_node)
setattr(StandardScaler, 'op', ops.STANDARD_SCALER)
setattr(StandardScaler, 'name', "{}_{}".format(ops.STANDARD_SCALER, uuid.uuid1()))
setattr(StandardScaler, 'set_input_features', set_input_features)
setattr(StandardScaler, 'set_output_features', set_output_features)
setattr(StandardScaler, 'serialize_to_bundle', serialize_to_bundle)

setattr(MinMaxScaler, 'get_mleap_model', get_mleap_model)
setattr(MinMaxScaler, 'get_mleap_node', get_mleap_node)
setattr(MinMaxScaler, 'op', ops.MIN_MAX_SCALER)
setattr(MinMaxScaler, 'name', "{}_{}".format(ops.MIN_MAX_SCALER, uuid.uuid1()))
setattr(MinMaxScaler, 'set_input_features', set_input_features)
setattr(MinMaxScaler, 'set_output_features', set_output_features)
setattr(MinMaxScaler, 'serialize_to_bundle', serialize_to_bundle)

setattr(Imputer, 'get_mleap_model', get_mleap_model)
setattr(Imputer, 'get_mleap_node', get_mleap_node)
setattr(Imputer, 'op', ops.IMPUTER)
setattr(Imputer, 'name', "{}_{}".format(ops.IMPUTER, uuid.uuid1()))
setattr(Imputer, 'set_input_features', set_input_features)
setattr(Imputer, 'set_output_features', set_output_features)
setattr(Imputer, 'serialize_to_bundle', serialize_to_bundle)

setattr(LabelEncoder, 'get_mleap_model', get_mleap_model)
setattr(LabelEncoder, 'get_mleap_node', get_mleap_node)
setattr(LabelEncoder, 'op', ops.LABEL_ENCODER)
setattr(LabelEncoder, 'name', "{}_{}".format(ops.LABEL_ENCODER, uuid.uuid1()))
setattr(LabelEncoder, 'set_input_features', set_input_features)
setattr(LabelEncoder, 'set_output_features', set_output_features)
setattr(LabelEncoder, 'serialize_to_bundle', serialize_to_bundle)

setattr(OneHotEncoder, 'get_mleap_model', get_mleap_model)
setattr(OneHotEncoder, 'get_mleap_node', get_mleap_node)
setattr(OneHotEncoder, 'op', ops.ONE_HOT_ENCODER)
setattr(OneHotEncoder, 'name', "{}_{}".format(ops.ONE_HOT_ENCODER, uuid.uuid1()))
setattr(OneHotEncoder, 'set_input_features', set_input_features)
setattr(OneHotEncoder, 'set_output_features', set_output_features)
setattr(OneHotEncoder, 'serialize_to_bundle', serialize_to_bundle)


class FeatureExtractor(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features from a pandas dataframe that are then passed into a subsequent transformer.
    MLeap treats this transformer like a VectorAssembler equivalent in spark.
    """
    def __init__(self, input_features, output_vector, output_vector_items):
        self.input_features = input_features
        self.output_vector_items = output_vector_items
        self.output_vector = output_vector
        self.op = 'vector_assembler'
        self.name = "{}_{}".format(self.op, uuid.uuid1())

    def transform(self, df, **params):
        return df[self.input_features]

    def fit(self, df, y=None, **fit_params):
        return self

    def fit_transform(self, X, y=None, **fit_params):

        self.fit(X)

        df_subset = self.transform(X)
        return df_subset

    def serialize_to_bundle(self, path, model_name):
        # If bundle path already exists, delte it and create a clean directory
        if os.path.exists("{}/{}".format(path, model_name)):
            shutil.rmtree("{}/{}".format(path, model_name))

        model_dir = "{}/{}".format(path, model_name)
        os.mkdir(model_dir)

        # Write bundle file
        with open("{}/{}".format(model_dir, 'bundle.json'), 'w') as outfile:
            json.dump(self.get_mleap_model(), outfile, indent=3)

        # Write node file
        with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
            json.dump(self.get_mleap_node(), outfile, indent=3)

    def get_mleap_model(self):
        js = {
          "op": self.op
        }
        return js

    def get_mleap_node(self):
        js = {
          "name": self.name,
          "shape": {
            "inputs": [{'name': x, 'port': 'input{}'.format(self.input_features.index(x))} for x in self.input_features],
            "outputs": [{
              "name": self.output_vector,
              "port": "output"
            }]
          }
        }
        return js


class SerializeToBundle(object):
    def __init__(self):
        super(SerializeToBundle, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):
        # If bundle path already exists, delete it and create a clean directory
        if os.path.exists("{}/{}.node".format(path, model_name)):
            shutil.rmtree("{}/{}.node".format(path, model_name))

        model_dir = "{}/{}.node".format(path, model_name)
        os.mkdir(model_dir)

        # Write bundle file
        with open("{}/{}".format(model_dir, 'bundle.json'), 'w') as outfile:
            json.dump(self.get_mleap_model(transformer), outfile, indent=3)

        # Write node file
        with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
            json.dump(self.get_mleap_node(transformer), outfile, indent=3)


class NDArrayToDataFrame(BaseEstimator, TransformerMixin):
    def __init__(self, input_features):
        self.input_features = input_features
        self.output_features = input_features
        self.op = 'one_dim_array_to_dataframe'
        self.name = "{}_{}".format(self.op, uuid.uuid1())

    def transform(self, X, **params):
        if isinstance(X, np.ndarray):
            return pd.DataFrame(X, columns=[self.input_features])
        return pd.DataFrame(X.todense(), columns=[self.input_features])

    def fit(self, df, y=None, **fit_params):
        return self

    def fit_transform(self, X, y=None, **fit_params):
        return self.transform(X)

    def get_mleap_model(self):
        js = {
          "op": self.op
        }
        return js

    def get_mleap_node(self):
        js = {
          "name": self.name,
          "shape": {
            "inputs": [{'name': x, 'port': 'input{}'.format(self.input_features.index(x))} for x in self.input_features],
            "outputs": [{
              "name": self.output_features,
              "port": "output"
            }]
          }
        }
        return js

    def _serialize_to_bundle(self, path, model_name):

        # If bundle path already exists, delte it and create a clean directory
        if os.path.exists("{}/{}".format(path, model_name)):
            shutil.rmtree("{}/{}".format(path, model_name))

        model_dir = "{}/{}".format(path, model_name)
        os.mkdir(model_dir)

        # Write bundle file
        with open("{}/{}".format(model_dir, 'bundle.json'), 'w') as outfile:
            json.dump(self.get_mleap_model(), outfile, indent=3)

        # Write node file
        with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
            json.dump(self.get_mleap_node(), outfile, indent=3)


class ToDense(BaseEstimator, TransformerMixin):
    def __init__(self, input_features):
        self.op = 'dense_transformer'
        self.name = "{}_{}".format(self.op, uuid.uuid1())
        self.input_features = input_features
        self.output_features = input_features

    def transform(self, X, **params):
        return X.todense()

    def fit(self, df, y=None, **fit_params):
        return self

    def fit_transform(self, X, y=None, **fit_params):
        return self.transform(X)

    def get_mleap_model(self):
        js = {
          "op": self.op
        }
        return js

    def get_mleap_node(self):
        js = {
          "name": self.name,
          "shape": {
            "inputs": {'name': self.input_features, 'port': 'input0'},
            "outputs": [{
              "name": self.output_features,
              "port": "output"
            }]
          }
        }
        return js

    def _serialize_to_bundle(self, path, model_name):
        # If bundle path already exists, delte it and create a clean directory
        if os.path.exists("{}/{}".format(path, model_name)):
            shutil.rmtree("{}/{}".format(path, model_name))

        model_dir = "{}/{}".format(path, model_name)
        os.mkdir(model_dir)

        # Write bundle file
        with open("{}/{}".format(model_dir, 'bundle.json'), 'w') as outfile:
            json.dump(self.get_mleap_model(), outfile, indent=3)

        # Write node file
        with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
            json.dump(self.get_mleap_node(), outfile, indent=3)


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()

    @staticmethod
    def _choose_serializer(transformer):
        serializer = None
        if transformer.op == ops.STANDARD_SCALER:
            serializer = StandardScalerSerializer()
        elif transformer.op == ops.MIN_MAX_SCALER:
            serializer = MinMaxScalerSerializer()
        elif transformer.op == ops.ONE_HOT_ENCODER:
            serializer = OneHotEncoderSerializer()
        elif transformer.op == ops.LABEL_ENCODER:
            serializer = LabelEncoderSerializer()
        elif transformer.op == ops.IMPUTER:
            serializer = ImputerSerializer()
        return serializer

    def get_mleap_model(self, transformer):
        serializer = self._choose_serializer(transformer)
        js = serializer.get_mleap_model(transformer)
        return js

    def get_mleap_node(self, transformer):
        serializer = self._choose_serializer(transformer)
        js = serializer.get_mleap_node(transformer)
        return js

    def set_input_features(self, transformer, input_features):
        transformer.input_features = input_features

    def set_output_features(self, transformer, output_features):
        transformer.output_features = output_features

    def serialize_to_bundle(self, transformer, path, model_name):
        serializer = self._choose_serializer(transformer)
        serializer.serialize_to_bundle(transformer, path, model_name)


class StandardScalerSerializer(SerializeToBundle):
    def __init__(self):
        super(StandardScalerSerializer, self).__init__()

    def get_mleap_model(self, transformer):

        attributes = []
        if transformer.with_mean is True:
            attributes.append({
                'name': 'mean',
                'type': {
                    'type': 'tensor',
                    'tensor': {
                        'base': 'double',
                        'dimensions': [-1]
                    }
                },
                'value': transformer.mean_.tolist()
            })

        if transformer.with_std is True:
            attributes.append({
                'name': 'std',
                'type': {
                    'type': 'tensor',
                    'tensor': {
                        'base': 'double',
                        'dimensions': [-1]
                    }
                },
                'value': [np.sqrt(x) for x in transformer.var_]
            })

        js = {
          "op": transformer.op,
          "attributes": attributes
        }
        return js

    def get_mleap_node(self, transformer):

        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": transformer.output_features,
              "port": "output"
            }]
          }
        }
        return js


class MinMaxScalerSerializer(SerializeToBundle):
    def __init__(self):
        super(MinMaxScalerSerializer, self).__init__()

    def get_mleap_model(self, transformer):

        attributes = []

        attributes.append({
            'name': 'min',
            'type': {
                'type': 'tensor',
                'tensor': {
                    'base': 'double',
                    'dimensions': [-1]
                }
            },
            'value': transformer.data_min_.tolist()
        })

        attributes.append({
            'name': 'max',
            'type': {
                'type': 'tensor',
                'tensor': {
                    'base': 'double',
                    'dimensions': [-1]
                }
            },
            'value': transformer.data_max_.tolist()
        })

        js = {
          "op": transformer.op,
          "attributes": attributes
        }
        return js

    def get_mleap_node(self, transformer):

        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": transformer.output_features,
              "port": "output"
            }]
          }
        }
        return js



class ImputerSerializer(SerializeToBundle):
    def __init__(self):
        super(ImputerSerializer, self).__init__()
        self.serializable = False

    def get_mleap_model(self, transformer):

        attributes = []

        attributes.append({
            'name': transformer.strategy,
            'type': {
                'type': 'tensor',
                'tensor': {
                    'base': 'double',
                    'dimensions': [-1]
                }
            },
            'value': transformer.statistics_.tolist()
        })

        js = {
          "op": transformer.op,
          "attributes": attributes
        }
        return js

    def get_mleap_node(self, transformer):

        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": transformer.output_features,
              "port": "output"
            }]
          }
        }
        return js


class OneHotEncoderSerializer(SerializeToBundle):
    def __init__(self):
        super(OneHotEncoderSerializer, self).__init__()

    def get_mleap_model(self, transformer):
        js = {
            'op': transformer.op,
            'attributes': [{
                    'name': "size",
                    'type': "long",
                    'value': transformer.n_values
                }]
        }
        return js

    def get_mleap_node(self, transformer):
        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": transformer.output_features,
              "port": "output"
            }]
          }
        }
        return js


class LabelEncoderSerializer(SerializeToBundle):
    def __init__(self):
        super(LabelEncoderSerializer, self).__init__()

    def get_mleap_model(self, transformer):
        js = {
              "op": transformer.op,
              "attributes": [{
                "name": "labels",
                "type": {
                  "type": "list",
                  "base": "string"
                },
                "value": transformer.classes_.tolist()
              }]
            }
        return js

    def get_mleap_node(self, transformer):
        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": transformer.output_features,
              "port": "output"
            }]
          }
        }
        return js