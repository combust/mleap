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
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Imputer, Binarizer, PolynomialFeatures
from sklearn.preprocessing.data import OneHotEncoder
from sklearn.preprocessing.label import LabelEncoder
from mleap.bundle.serialize import MLeapSerializer
from sklearn.utils import column_or_1d
import warnings
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
        self.BINARIZER = 'binarizer'
        self.POLYNOMIALEXPANSION = 'polynomial_expansion'

ops = ops()

def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def mleap_init(self, input_features, output_features):
    self.input_features = input_features
    self.output_features = output_features
    self.name = "{}_{}".format(self.op, uuid.uuid1())


setattr(StandardScaler, 'op', ops.STANDARD_SCALER)
setattr(StandardScaler, 'serialize_to_bundle', serialize_to_bundle)
setattr(StandardScaler, 'mlinit', mleap_init)
setattr(StandardScaler, 'serializable', True)

setattr(MinMaxScaler, 'op', ops.MIN_MAX_SCALER)
setattr(MinMaxScaler, 'mlinit', mleap_init)
setattr(MinMaxScaler, 'serialize_to_bundle', serialize_to_bundle)
setattr(MinMaxScaler, 'serializable', True)

setattr(Imputer, 'op', ops.IMPUTER)
setattr(Imputer, 'mlinit', mleap_init)
setattr(Imputer, 'serialize_to_bundle', serialize_to_bundle)
setattr(Imputer, 'serializable', True)

setattr(LabelEncoder, 'op', ops.LABEL_ENCODER)
setattr(LabelEncoder, 'mlinit', mleap_init)
setattr(LabelEncoder, 'serialize_to_bundle', serialize_to_bundle)
setattr(LabelEncoder, 'serializable', True)

setattr(OneHotEncoder, 'op', ops.ONE_HOT_ENCODER)
setattr(OneHotEncoder, 'mlinit', mleap_init)
setattr(OneHotEncoder, 'serialize_to_bundle', serialize_to_bundle)
setattr(OneHotEncoder, 'serializable', True)

setattr(Binarizer, 'op', ops.BINARIZER)
setattr(Binarizer, 'mlinit', mleap_init)
setattr(Binarizer, 'serialize_to_bundle', serialize_to_bundle)
setattr(Binarizer, 'serializable', True)

setattr(PolynomialFeatures, 'op', ops.POLYNOMIALEXPANSION)
setattr(PolynomialFeatures, 'mlinit', mleap_init)
setattr(PolynomialFeatures, 'serialize_to_bundle', serialize_to_bundle)
setattr(PolynomialFeatures, 'serializable', True)


class SimpleSerializer(object):
    """
    Extends base scikit-learn transformers to include:
        - set_input_features
        - set_output_features
        - serialize_to_bundle
    methods. The set input/output features are required to maintain parity with the Spark/MLeap execution engine.
    """
    def __init__(self):
        super(SimpleSerializer, self).__init__()

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
        elif transformer.op == ops.BINARIZER:
            serializer = BinarizerSerializer()
        elif transformer.op == ops.POLYNOMIALEXPANSION:
            serializer = PolynomialExpansionSerializer()
        return serializer

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    @staticmethod
    def set_output_features(transformer, output_features):
        transformer.output_features = output_features

    def serialize_to_bundle(self, transformer, path, model_name):
        serializer = self._choose_serializer(transformer)
        serializer.serialize_to_bundle(transformer, path, model_name)


def _to_list(x):
    if isinstance(x, list):
        return x
    return list(x)


class FeatureExtractor(BaseEstimator, TransformerMixin, MLeapSerializer):
    def __init__(self, input_features, output_vector, output_vector_items):
        """
        Selects a subset of features from a pandas dataframe that are then passed into a subsequent transformer.
        MLeap treats this transformer like a VectorAssembler equivalent in spark.
        >>> data = pd.DataFrame([['a', 0, 1], ['b', 1, 2], ['c', 3, 4]], columns=['col_a', 'col_b', 'col_c'])
        >>> vector_items = ['col_b', 'col_c']
        >>> feature_extractor2_tf = FeatureExtractor(vector_items, 'continuous_features', vector_items)
        >>> feature_extractor2_tf.fit_transform(data).head(1).values
        >>> array([[0, 1]])
        :param input_features: List of features to extracts from a pandas data frame
        :param output_vector: Name of the output vector, only used for serialization
        :param output_vector_items: List of output feature names
        :return:
        """
        self.input_features = input_features
        self.output_vector_items = output_vector_items
        self.output_vector = output_vector
        self.op = 'vector_assembler'
        self.name = "{}_{}".format(self.op, uuid.uuid1())
        self.dtypes = None
        self.serializable = True
        self.skip_fit_transform = False

    def transform(self, df, **params):
        if not self.skip_fit_transform:
            return df[self.input_features]
        return df

    def fit(self, df, y=None, **fit_params):
        if not self.skip_fit_transform:
            self.dtypes = df[self.input_features].dtypes.to_dict()
            if len([x for x in self.dtypes.values() if x.type == np.object_]) != 0:
                self.serializable = False
        return self

    def fit_transform(self, X, y=None, **fit_params):
        if not self.skip_fit_transform:
            self.fit(X)

        df_subset = self.transform(X)
        return df_subset

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = None

        # define node inputs and outputs
        inputs = [{'name': x, 'port': 'input{}'.format(self.input_features.index(x))} for x in self.input_features]

        outputs = [{
                  "name": self.output_vector,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)


class StandardScalerSerializer(MLeapSerializer):
    """
    Standardizes features by removing the mean and scaling to unit variance using mean and standard deviation from
    training data.

    >>> data = pd.DataFrame([[1, 0], [5, 1], [6, 3], [1, 1]], columns=['col_a', 'col_b'])
    >>> standard_scaler_tf = StandardScaler()
    >>> standard_scaler_tf.minit(input_features=['col_a', 'col_b'], output_features='scaled_cont_features')
    >>> standard_scaler_tf.fit_transform(data)
    >>> array([[-0.98787834, -1.14707867],
    >>>         [ 0.76834982, -0.22941573],
    >>>         [ 1.20740686,  1.60591014],
    >>>         [-0.98787834, -0.22941573]])
    """
    def __init__(self):
        super(StandardScalerSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('mean', transformer.mean_.tolist()))
        attributes.append(('std', [np.sqrt(x) for x in transformer.var_]))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class MinMaxScalerSerializer(MLeapSerializer):
    """
    Scales features by the range of values using calculated min and max from training data.

    >>> data = pd.DataFrame([[1, 0], [5, 1], [6, 3], [1, 1]], columns=['col_a', 'col_b'])
    >>> minmax_scaler_tf = MinMaxScaler()
    >>> minmax_scaler_tf.minit(input_features=['col_a', 'col_b'], output_features='scaled_cont_features')

    >>> minmax_scaler_tf.fit_transform(data)
    >>> array([[ 0.        ,  0.        ],
    >>>      [ 0.8       ,  0.33333333],
    >>>      [ 1.        ,  1.        ],
    >>>      [ 0.        ,  0.33333333]])
    """
    def __init__(self):
        super(MinMaxScalerSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('min', _to_list(transformer.data_min_)))
        attributes.append(('max', _to_list(transformer.data_max_)))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class ImputerSerializer(MLeapSerializer):
    def __init__(self):
        super(ImputerSerializer, self).__init__()
        self.serializable = False

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('strategy', transformer.strategy))
        attributes.append(('surrogate_value', transformer.statistics_.tolist()[0]))
        if transformer.missing_values is not np.NaN:
            attributes.append(('missing_value', transformer.missing_values[0]))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class LabelEncoderSerializer(MLeapSerializer):
    """
    Converts categorical values of a single column into categorical indices. This transformer should be followed by a
    NDArrayToDataFrame transformer to maintain a data structure required by scikit pipelines.

    >>> data = pd.DataFrame([['a', 0], ['b', 1], ['b', 3], ['c', 1]], columns=['col_a', 'col_b'])
    >>> # Label Encoder for x1 Label
    >>> label_encoder_tf = LabelEncoder()
    >>> label_encoder_tf.minit(input_features = ['col_a'] , output_features='col_a_label_le')
    >>> # Convert output of Label Encoder to Data Frame instead of 1d-array
    >>> n_dim_array_to_df_tf = NDArrayToDataFrame('col_a_label_le')
    >>> n_dim_array_to_df_tf.fit_transform(label_encoder_tf.fit_transform(data['col_a']))
    """
    def __init__(self):
        super(LabelEncoderSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('labels', transformer.classes_.tolist()))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class OneHotEncoderSerializer(MLeapSerializer):
    """
    A one-hot encoder maps a single column of categorical indices to a
    column of binary vectors, which can be re-assamble back to a DataFrame using a ToDense transformer.
    """
    def __init__(self):
        super(OneHotEncoderSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('size', transformer.n_values_.tolist()[0]))
        attributes.append(('drop_last', True))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class BinarizerSerializer(MLeapSerializer):
    def __init__(self):
        super(BinarizerSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('threshold', transformer.threshold))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class PolynomialExpansionSerializer(MLeapSerializer):
    def __init__(self):
        super(PolynomialExpansionSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('degree', transformer.degree))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class ReshapeArrayToN1(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.serializable=False
        self.op='reshape_array'
        self.name = "{}_{}".format(self.op, uuid.uuid1())

    def transform(self, X, **params):
        """
        :type X: np.ndarray
        :param X:
        :param params:
        :return:
        """
        return X.reshape(X.size, 1)

    def fit(self, df, y=None, **fit_params):
        return self

    def fit_transform(self, X, y=None, **fit_params):
        return self.transform(X)


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
        with open("{}/{}".format(model_dir, 'model.json'), 'w') as outfile:
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
        with open("{}/{}".format(model_dir, 'model.json'), 'w') as outfile:
            json.dump(self.get_mleap_model(), outfile, indent=3)

        # Write node file
        with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
            json.dump(self.get_mleap_node(), outfile, indent=3)


class MathUnary(BaseEstimator, TransformerMixin, MLeapSerializer):
    """
    Performs basic math operations on a single feature (column of a DataFrame). Supported operations include:
        - log
        - exp
        - sqrt
        - sin
        - cos
        - tan
    Note, currently we only support 1d-arrays.
    Inputs need to be floats.
    """
    def __init__(self, input_features, output_features, transform_type):
        self.valid_transforms = ['log', 'exp', 'sqrt', 'sin', 'cos', 'tan']
        self.op = 'math_unary'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.input_features = input_features
        self.output_features = output_features
        self.transform_type = transform_type
        self.serializable = True

    def fit(self, y):
        """
        Fit Unary Math Operator
        :param y:
        :return:
        """
        y = column_or_1d(y, warn=True)
        if self.transform_type not in self.valid_transforms:
                warnings.warn("Invalid transform type.", stacklevel=2)
        return self

    def transform(self, y):
        """
        Transform features per specified math function.
        :param y:
        :return:
        """
        if self.transform_type == 'log':
            return np.log(y)
        elif self.transform_type == 'exp':
            return np.exp(y)
        elif self.transform_type == 'sqrt':
            return np.sqrt(y)
        elif self.transform_type == 'sin':
            return np.sin(y)
        elif self.transform_type == 'cos':
            return np.cos(y)
        elif self.transform_type == 'tan':
            return np.tan(y)

    def fit_transform(self, X, y=None, **fit_params):
        """
        Transform data per specified math function.
        :param X:
        :param y:
        :param fit_params:
        :return:
        """
        self.fit(X)

        return self.transform(X)

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('operation', self.transform_type))

        # define node inputs and outputs
        inputs = [{
                  "name": self.input_features[0],
                  "port": "input"
                }]

        outputs = [{
                  "name": self.output_features[0],
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)


class MathBinary(BaseEstimator, TransformerMixin, MLeapSerializer):
    """
    Performs basic math operations on two features (columns of a DataFrame). Supported operations include:
        - add: Add x + y
        - sub: Subtract x - y
        - mul: Multiply x * y
        - div: Divide x / y
        - rem: Remainder x % y
        - logn: LogN log(x) / log(y)
        - pow: Power x^y
    These transforms work on 2-dimensional arrays/vectors, where the the first column is x and second column is y.
    Inputs need to be floats.
    """
    def __init__(self, input_features, output_features, transform_type):
        self.valid_transforms = ['add', 'sub', 'mul', 'div', 'rem', 'logn', 'pow']
        self.op = 'math_binary'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.input_features = input_features
        self.output_features = output_features
        self.transform_type = transform_type
        self.serializable = True

    def _return(self, y):
        if type(y, np.ndarray):
            return

    def fit(self, y):
        """
        Fit Unary Math Operator
        :param y:
        :return:
        """
        if self.transform_type not in self.valid_transforms:
                warnings.warn("Invalid transform type.", stacklevel=2)
        return self

    def transform(self, y):
        """
        Transform features per specified math function.
        :param y:
        :return:
        """
        if isinstance(y, pd.DataFrame):
            x = y.ix[:,0]
            y = y.ix[:,1]
        else:
            x = y[:,0]
            y = y[:,1]
        if self.transform_type == 'add':
            return np.add(x, y)
        elif self.transform_type == 'sub':
            return np.subtract(x, y)
        elif self.transform_type == 'mul':
            return np.multiply(x, y)
        elif self.transform_type == 'div':
            return np.divide(x, y)
        elif self.transform_type == 'rem':
            return np.remainder(x, y)
        elif self.transform_type == 'pow':
            return x**y

    def fit_transform(self, X, y=None, **fit_params):
        """
        Transform data per specified math function.
        :param X:
        :param y:
        :param fit_params:
        :return:
        """
        self.fit(X)

        return self.transform(X)

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('operation', self.transform_type))

        # define node inputs and outputs
        inputs = [{
                  "name": self.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": self.output_features,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)