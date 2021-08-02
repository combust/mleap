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
import uuid
import warnings
from collections import OrderedDict

import numpy as np
import pandas as pd
from mleap.bundle.serialize import MLeapSerializer, MLeapDeserializer, Vector
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Binarizer, PolynomialFeatures
from sklearn.preprocessing.data import BaseEstimator, TransformerMixin
from sklearn.preprocessing.data import OneHotEncoder
from sklearn.preprocessing.label import LabelEncoder
from sklearn.utils import column_or_1d
from sklearn.utils.fixes import np_version
from sklearn.utils.validation import check_is_fitted


class ops(object):
    def __init__(self):
        self.STANDARD_SCALER = 'standard_scaler'
        self.MIN_MAX_SCALER = 'min_max_scaler'
        self.LABEL_ENCODER = 'string_indexer'
        self.ONE_HOT_ENCODER = 'one_hot_encoder'
        self.IMPUTER = 'imputer'
        self.NDARRAYTODATAFRAME = 'one_dim_array_to_dataframe'
        self.TODENSE = 'dense_transformer'
        self.BINARIZER = 'sklearn_binarizer'
        self.POLYNOMIALEXPANSION = 'sklearn_polynomial_expansion'

ops = ops()


def _check_numpy_unicode_bug(labels):
    """Check that user is not subject to an old numpy bug

    Fixed in master before 1.7.0:

      https://github.com/numpy/numpy/pull/243

    """
    if np_version[:3] < (1, 7, 0) and labels.dtype.kind == 'U':
        raise RuntimeError("NumPy < 1.7.0 does not implement searchsorted"
                           " on unicode data correctly. Please upgrade"
                           " NumPy to use LabelEncoder with unicode inputs.")


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def deserialize_from_bundle(self, path, node_name):
    serializer = SimpleSerializer()
    return serializer.deserialize_from_bundle(self, path, node_name)


def mleap_init(self, prior_tf, output_features=None):

    self.name = "{}_{}".format(self.op, uuid.uuid1())

    if prior_tf.op == 'vector_assembler':
        self.input_features = prior_tf.output_vector
        output_shape = 'tensor'
        output_size = len(prior_tf.input_features)
        output_feature_name = prior_tf.output_vector

    else:
        self.input_features = prior_tf.output_features
        output_shape = 'scalar'
        output_size = 1
        output_feature_name = prior_tf.output_features

    class_name = self.__class__.__name__

    if output_features is not None:
        self.output_features = output_features
    else:
        self.output_features = "{}_{}".format(output_feature_name, class_name.lower())

    if class_name == 'PolynomialFeatures':
        self.input_size = len(prior_tf.input_features)

    elif output_shape == 'tensor':
        self.input_shapes = {'data_shape': [{'shape':'tensor',
                                                 "tensor_shape": {"dimensions": [{"size": output_size}]}}]}
    else:
        self.input_shapes = {'data_shape': [{'shape': output_shape}]}


setattr(StandardScaler, 'op', ops.STANDARD_SCALER)
setattr(StandardScaler, 'serialize_to_bundle', serialize_to_bundle)
setattr(StandardScaler, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(StandardScaler, 'mlinit', mleap_init)
setattr(StandardScaler, 'serializable', True)

setattr(MinMaxScaler, 'op', ops.MIN_MAX_SCALER)
setattr(MinMaxScaler, 'mlinit', mleap_init)
setattr(MinMaxScaler, 'serialize_to_bundle', serialize_to_bundle)
setattr(MinMaxScaler, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(MinMaxScaler, 'serializable', True)

setattr(SimpleImputer, 'op', ops.IMPUTER)
setattr(SimpleImputer, 'mlinit', mleap_init)
setattr(SimpleImputer, 'serialize_to_bundle', serialize_to_bundle)
setattr(SimpleImputer, 'serializable', True)

setattr(OneHotEncoder, 'op', ops.ONE_HOT_ENCODER)
setattr(OneHotEncoder, 'mlinit', mleap_init)
setattr(OneHotEncoder, 'serialize_to_bundle', serialize_to_bundle)
setattr(OneHotEncoder, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(OneHotEncoder, 'serializable', True)

setattr(Binarizer, 'op', ops.BINARIZER)
setattr(Binarizer, 'mlinit', mleap_init)
setattr(Binarizer, 'serialize_to_bundle', serialize_to_bundle)
setattr(Binarizer, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(Binarizer, 'serializable', True)

setattr(PolynomialFeatures, 'op', ops.POLYNOMIALEXPANSION)
setattr(PolynomialFeatures, 'mlinit', mleap_init)
setattr(PolynomialFeatures, 'serialize_to_bundle', serialize_to_bundle)
setattr(PolynomialFeatures, 'deserialize_from_bundle', deserialize_from_bundle)
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

    def deserialize_from_bundle(self, transformer, path, node_name):
        deserializer = self._choose_serializer(transformer)
        transformer = deserializer.deserialize_from_bundle(transformer, path, node_name)
        return transformer


def _to_list(x):
    if isinstance(x, list):
        return x
    return list(x)


class FeatureExtractor(BaseEstimator, TransformerMixin, MLeapSerializer):
    def __init__(self, input_scalars=None, input_vectors=None, output_vector=None, output_vector_items=None):
        """
        Selects a subset of features from a pandas dataframe that are then passed into a subsequent transformer.
        MLeap treats this transformer like a VectorAssembler equivalent in spark.
        >>> data = pd.DataFrame([['a', 0, 1], ['b', 1, 2], ['c', 3, 4]], columns=['col_a', 'col_b', 'col_c'])
        >>> vector_items = ['col_b', 'col_c']
        >>> input_shapes = {'data_shape': [{'shape':'scalar'}, {'shape':'scalar'}]}
        >>> input_shapes = {'data_shape': [{'shape':'tensor', "tensor_shape": {"dimensions": [{"size": 23}]}}]}
        >>> feature_extractor2_tf = FeatureExtractor(vector_items, 'continuous_features', vector_items, input_shapes)
        >>> feature_extractor2_tf.fit_transform(data).head(1).values
        >>> array([[0, 1]])
        :param input_vectors: List of scalar feature names that are being extracted from a DataFrame
        :param input_vectors: List of FeatureExtractors that were used to generate the input vectors
        :param input_features: List of features to extracts from a pandas data frame
        :param output_vector: Name of the output vector, only used for serialization
        :param output_vector_items: List of output feature names
        :param input_shapes: the shape of each input feature, whether it is scalar or a vector
        if it's a vector, then we include size information of the vector
        :return:
        """
        self.input_scalars = input_scalars
        self.input_vectors = input_vectors
        self.input_features = self.get_input_features(input_scalars, input_vectors)
        self.output_vector_items = output_vector_items
        self.output_vector = output_vector
        self.op = 'vector_assembler'
        self.name = "{}_{}".format(self.op, uuid.uuid1())
        self.dtypes = None
        self.serializable = True
        self.skip_fit_transform = False
        self.input_size = None
        self.input_shapes = None

    def get_input_features(self, input_scalars, input_vectors):
        if input_scalars is not None:
            return input_scalars
        elif input_vectors is not None and isinstance(input_vectors, list) and len(input_vectors)>0:
            features = list()
            for x in input_vectors:
                if 'output_vector' in x.__dict__:
                    features.append(x.output_vector)
                elif 'output_features' in x.__dict__:
                    features.append(x.output_features)
            return features
        else:
            raise BaseException("Unable To Define Input Features")

    def transform(self, df, **params):
        if not self.skip_fit_transform:
            return df[self.input_features]
        return df

    def assign_input_shapes(self, X):
        # Figure out the data shape
        if self.input_scalars is not None and isinstance(X, pd.DataFrame):
            self.input_shapes = {'data_shape': [{'shape': 'scalar'}]*len(self.input_features)}

        elif self.input_vectors is not None:
            self.input_shapes = {'data_shape': []}
            for vector in self.input_vectors:
                if vector.op not in [ops.ONE_HOT_ENCODER, ops.POLYNOMIALEXPANSION]:
                    shape = {'shape': 'tensor', "tensor_shape": {"dimensions": [{"size": len(vector.input_features)}]}}
                    self.input_shapes['data_shape'].append(shape)
                elif vector.op == ops.ONE_HOT_ENCODER:
                    shape = {'shape': 'tensor', "tensor_shape": {"dimensions": [{"size": int(vector.n_values_[0] - 1)}]}}
                    self.input_shapes['data_shape'].append(shape)
        return self

    def fit(self, X, y=None, **fit_params):
        self.assign_input_shapes(X)
        if not self.skip_fit_transform:
            self.dtypes = X[self.input_features].dtypes.to_dict()
            if len([x for x in self.dtypes.values() if x.type == np.object_]) != 0:
                self.serializable = False
        return self

    def fit_transform(self, X, y=None, **fit_params):
        if not self.skip_fit_transform:
            self.fit(X)
        else:
            self.assign_input_shapes(X)

        df_subset = self.transform(X)
        return df_subset

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(("input_shapes", self.input_shapes))

        # define node inputs and outputs
        inputs = [{'name': x, 'port': 'input{}'.format(self.input_features.index(x))} for x in self.input_features]

        outputs = [{
                  "name": self.output_vector,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)


class LabelEncoder(BaseEstimator, TransformerMixin, MLeapSerializer, MLeapDeserializer):
    """
    Copied from sklearn, but enables passing X and Y features, which allows this transformer
    to be used in Pipelines.

    Converts categorical values of a single column into categorical indices. This transformer should be followed by a
    NDArrayToDataFrame transformer to maintain a data structure required by scikit pipelines.

    NOTE: You can only LabelEncode/String Index one feature at a time!!!

    >>> data = pd.DataFrame([['a', 0], ['b', 1], ['b', 3], ['c', 1]], columns=['col_a', 'col_b'])
    >>> # Label Encoder for x1 Label
    >>> label_encoder_tf = LabelEncoder(input_features = ['col_a'] , output_features='col_a_label_le')
    >>> # Convert output of Label Encoder to Data Frame instead of 1d-array
    >>> n_dim_array_to_df_tf = NDArrayToDataFrame('col_a_label_le')
    >>> n_dim_array_to_df_tf.fit_transform(label_encoder_tf.fit_transform(data['col_a']))

    Encode labels with value between 0 and n_classes-1.

    Read more in the :ref:`User Guide <preprocessing_targets>`.

    Attributes
    ----------
    classes_ : array of shape (n_class,)
        Holds the label for each class.

    Examples
    --------
    `LabelEncoder` can be used to normalize labels.

    >>> from sklearn import preprocessing
    >>> le = preprocessing.LabelEncoder()
    >>> le.fit([1, 2, 2, 6])
    LabelEncoder()
    >>> le.classes_
    array([1, 2, 6])
    >>> le.transform([1, 1, 2, 6]) #doctest: +ELLIPSIS
    array([0, 0, 1, 2]...)
    >>> le.inverse_transform([0, 0, 1, 2])
    array([1, 1, 2, 6])

    It can also be used to transform non-numerical labels (as long as they are
    hashable and comparable) to numerical labels.

    >>> le = preprocessing.LabelEncoder()
    >>> le.fit(["paris", "paris", "tokyo", "amsterdam"])
    LabelEncoder()
    >>> list(le.classes_)
    ['amsterdam', 'paris', 'tokyo']
    >>> le.transform(["tokyo", "tokyo", "paris"]) #doctest: +ELLIPSIS
    array([2, 2, 1]...)
    >>> list(le.inverse_transform([2, 2, 1]))
    ['tokyo', 'tokyo', 'paris']

    """
    def __init__(self, input_features=None, output_features=None):
        self.input_features = input_features
        self.output_features = output_features
        self.op = 'string_indexer'
        self.name = "{}_{}".format(self.op, uuid.uuid1())
        self.serializable = True


    def fit(self, X):
        """Fit label encoder

        Parameters
        ----------
        y : array-like of shape (n_samples,)
            Target values.

        Returns
        -------
        self : returns an instance of self.
        """
        X = column_or_1d(X, warn=True)
        _check_numpy_unicode_bug(X)
        self.classes_ = np.unique(X)
        return self

    def fit_transform(self, X, y=None, **fit_params):
        """Fit label encoder and return encoded labels

        Parameters
        ----------
        y : array-like of shape [n_samples]
            Target values.

        Returns
        -------
        y : array-like of shape [n_samples]
        """
        y = column_or_1d(X, warn=True)
        _check_numpy_unicode_bug(X)
        self.classes_, X = np.unique(X, return_inverse=True)
        return X

    def transform(self, y):
        """Transform labels to normalized encoding.

        Parameters
        ----------
        y : array-like of shape [n_samples]
            Target values.

        Returns
        -------
        y : array-like of shape [n_samples]
        """
        check_is_fitted(self, 'classes_')
        y = column_or_1d(y, warn=True)

        classes = np.unique(y)
        _check_numpy_unicode_bug(classes)
        if len(np.intersect1d(classes, self.classes_)) < len(classes):
            diff = np.setdiff1d(classes, self.classes_)
            raise ValueError("y contains new labels: %s" % str(diff))
        return np.searchsorted(self.classes_, y)

    def inverse_transform(self, y):
        """Transform labels back to original encoding.

        Parameters
        ----------
        y : numpy array of shape [n_samples]
            Target values.

        Returns
        -------
        y : numpy array of shape [n_samples]
        """
        check_is_fitted(self, 'classes_')

        diff = np.setdiff1d(y, np.arange(len(self.classes_)))
        if diff:
            raise ValueError("y contains new labels: %s" % str(diff))
        y = np.asarray(y)
        return self.classes_[y]

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('labels', self.classes_.tolist()))
        attributes.append(('nullable_input', False))

        # define node inputs and outputs
        inputs = [{
                  "name": self.input_features[0],
                  "port": "input"
                }]

        outputs = [{
                  "name": self.output_features,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)

    def deserialize_from_bundle(self, node_path, node_name):

        attributes_map = {
            'labels': 'classes_'
        }

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(self, full_node_path, attributes_map)

        return transformer


class MinMaxScalerSerializer(MLeapSerializer, MLeapDeserializer):
    """
    Scales features by the range of values using calculated min and max from training data.

    >>> data = pd.DataFrame([[1], [5], [6], [1]], columns=['col_a'])
    >>> minmax_scaler_tf = MinMaxScaler()
    >>> minmax_scaler_tf.mlinit(input_features='col_a', output_features='scaled_cont_features')

    >>> minmax_scaler_tf.fit_transform(data)
    >>> array([[ 0.],
    >>>      [ 0.8],
    >>>      [ 1.],
    >>>      [ 0.]])
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

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        # Default (and only range supported)
        feature_range = (0, 1)

        attributes_map = {
            'min': 'data_min_',
            'max': 'data_max_'
        }

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path, attributes_map)
        transformer.data_range_ = np.array(transformer.data_max_) - np.array(transformer.data_min_)

        transformer.scale_ = ((feature_range[1] - feature_range[0]) / transformer.data_range_)

        transformer.min_ = feature_range[0] - transformer.data_min_ * transformer.scale_

        return transformer


class ImputerSerializer(MLeapSerializer):
    def __init__(self):
        super(ImputerSerializer, self).__init__()
        self.serializable = False

    def serialize_to_bundle(self, transformer, path, model_name):
        if transformer.strategy == 'most_frequent' or transformer.strategy == 'constant':
            raise NotImplementedError(f"Scikit-learn's Imputer strategy `{transformer.strategy}` is not supported by MLeap")
        if transformer.add_indicator:
            raise NotImplementedError("Scikit-learn's Imputer parameter `add_indicator` is not supported by MLeap")
        if len(transformer.statistics_.tolist()) != 1:
            raise NotImplementedError("MLeap's Imputer only supports imputing a single feature at a time")

        attributes = list()
        attributes.append(('strategy', transformer.strategy))
        attributes.append(('surrogate_value', transformer.statistics_.tolist()[0]))
        if not np.isnan(transformer.missing_values):
            attributes.append(('missing_value', transformer.missing_values))

        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)


class StandardScalerSerializer(MLeapSerializer, MLeapDeserializer):
    """
    Standardizes features by removing the mean and scaling to unit variance using mean and standard deviation from
    training data.
    >>> data = pd.DataFrame([[1], [5], [6], [1]], columns=['col_a'])
    >>> standard_scaler_tf = StandardScaler()
    >>> standard_scaler_tf.mlinit(input_features='col_a', output_features='scaled_cont_features')
    >>> standard_scaler_tf.fit_transform(data)
    >>> array([[-0.98787834],
    >>>         [ 0.76834982],
    >>>         [ 1.20740686],
    >>>         [-0.98787834]])
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

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        attributes_map = {
            'mean': 'mean_',
            'std': 'scale_'
        }

        # Set serialized attributes
        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path, attributes_map)

        # Set Additional Attributes
        if 'mean_' in transformer.__dict__:
            transformer.with_mean = True
        else:
            transformer.with_mean = False

        if 'scale_' in transformer.__dict__:
            transformer.with_std = True
            transformer.var = np.square(transformer.scale_)
        else:
            transformer.with_std = False

        return transformer


class OneHotEncoderSerializer(MLeapSerializer, MLeapDeserializer):
    """
    A one-hot encoder maps a single column of categorical indices to a
    column of binary vectors, which can be re-assamble back to a DataFrame using a ToDense transformer.
    """

    def __init__(self):
        super(OneHotEncoderSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):
        if len(transformer.categories_) != 1:
            raise NotImplementedError("MLeap can only one-hot encode a single feature at a time")
        single_feature_categories = transformer.categories_[0]
        if not np.array_equal(single_feature_categories, np.arange(single_feature_categories.size)):
            raise ValueError(f"Categories {single_feature_categories} do not form a valid index range")
        if transformer.drop is not None:
            raise NotImplementedError("Scikit-learn's OneHotEncoder `drop` parameter is not supported by MLeap")
        if transformer.dtype != np.float64:
            raise NotImplementedError("Scikit-learn's OneHotEncoder `dtype` parameter is not supported by MLeap")

        attributes = list()
        attributes.append(('size', single_feature_categories.size))
        if transformer.handle_unknown == 'ignore':
            # MLeap's OneHotEncoderModel adds an extra column when keeping invalid data
            # so we drop that extra column to match sklearn's ignore behavior
            attributes.append(('handle_invalid', 'keep'))
            attributes.append(('drop_last', True))
        else:
            attributes.append(('handle_invalid', 'error'))
            attributes.append(('drop_last', False))

        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                }]

        outputs = [{
                  "name": transformer.output_features,
                  "port": "output"
                }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        attributes_map = {
            'size': 'categories_',
            'handle_invalid': 'handle_unknown',
        }

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path, attributes_map)

        transformer.categories_ = np.asarray([range(transformer.categories_)])
        if transformer.handle_unknown == 'keep':
            transformer.handle_unknown = 'ignore'
        transformer.drop_idx_ = None

        transformer.sparse = False

        return transformer


class BinarizerSerializer(MLeapSerializer, MLeapDeserializer):
    def __init__(self):
        super(BinarizerSerializer, self).__init__()



    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('threshold', float(transformer.threshold)))
        attributes.append(("input_shapes", transformer.input_shapes))

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

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path)

        return transformer


class PolynomialExpansionSerializer(MLeapSerializer, MLeapDeserializer):
    def __init__(self):
        super(PolynomialExpansionSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('combinations', str(transformer.get_feature_names()).replace("'", "").replace(", ", ",")))

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

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path)
        transformer.include_bias = False

        # Get number of input features
        with open("{}/node.json".format(full_node_path)) as json_data:
            node_j = json.load(json_data)

        # Set powers
        transformer.n_input_features_ = len(node_j['shape']['inputs'][0]['name']) # TODO: Make this dynamic
        transformer.interaction_only = False

        transformer.n_output_features_ = sum(1 for _ in transformer._combinations(transformer.n_input_features_,
                                                                                  transformer.degree,
                                                                                  transformer.interaction_only,
                                                                                  transformer.include_bias))
        return transformer


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


class MathUnary(BaseEstimator, TransformerMixin, MLeapSerializer, MLeapDeserializer):
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
    def __init__(self, input_features=None, output_features=None, transform_type=None):
        self.valid_transforms = ['log', 'exp', 'sqrt', 'sin', 'cos', 'tan', 'abs']
        self.op = 'math_unary'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.input_features = input_features
        self.output_features = output_features
        self.transform_type = transform_type
        self.serializable = True

    def fit(self, X, y=None, **fit_params):
        """
        Fit Unary Math Operator
        :param X:
        :return:
        """
        X = column_or_1d(X, warn=True)
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
        elif self.transform_type == 'abs':
             return np.abs(y)

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
                  "name": self.output_features,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)

    def deserialize_from_bundle(self, node_path, node_name):
        attributes_map = {
            'operation': 'transform_type'
        }
        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(self, full_node_path, attributes_map)
        return transformer


class MathBinary(BaseEstimator, TransformerMixin, MLeapSerializer, MLeapDeserializer):
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
    def __init__(self, input_features=None, output_features=None, transform_type=None):
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

    def fit(self, X, y=None, **fit_params):
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
            return pd.DataFrame(np.add(x, y))
        elif self.transform_type == 'sub':
            return pd.DataFrame(np.subtract(x, y))
        elif self.transform_type == 'mul':
            return pd.DataFrame(np.multiply(x, y))
        elif self.transform_type == 'div':
            return pd.DataFrame(np.divide(x, y))
        elif self.transform_type == 'rem':
            return pd.DataFrame(np.remainder(x, y))
        elif self.transform_type == 'pow':
            return pd.DataFrame(x**y)

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
                  "port": "input_a"
                },
                {
                    "name": self.input_features[1],
                    "port": "input_b"
                }]

        outputs = [{
                  "name": self.output_features,
                  "port": "output"
                }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)

    def deserialize_from_bundle(self, node_path, node_name):
        attributes_map = {
            'operation': 'transform_type'
        }
        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(self, full_node_path, attributes_map)
        return transformer

class StringMap(BaseEstimator, TransformerMixin, MLeapSerializer, MLeapDeserializer):

    def __init__(self, input_features=None, output_features=None, labels=None):
        self.op = 'string_map'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.input_features = input_features
        self.output_features = output_features
        self.serializable = True
        self.labels = labels
        if labels is not None:
            if not isinstance(self.labels, OrderedDict):
                self.labels = OrderedDict(
                    sorted(self.labels.items(), key=lambda x: x[0])
                )
            self.label_keys = self.labels.keys
            self.label_values = self.labels.values

    def fit(self, X, y=None, **fit_params):
        if self.labels is None:
            self.labels = dict(zip(self.label_keys, self.label_values))
        return self

    def transform(self, y):
       return y.applymap(lambda input : self.labels[input]).values

    def fit_transform(self, X, y=None, **fit_params):
        self.fit(X)
        return self.transform(X)

    def serialize_to_bundle(self, path, model_name):
        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(("labels", list(self.labels.keys())))
        attributes.append(("values", Vector(list(self.labels.values()))))

        # define node inputs and outputs
        inputs = [{
            "name": self.input_features[0],
            "port": "input"
        }]

        outputs = [{
            "name": self.output_features,
            "port": "output"
        }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)

    def deserialize_from_bundle(self, node_path, node_name):
        attributes_map = {
            'labels': 'label_keys',
            'values': 'label_values'
        }

        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(self, full_node_path, attributes_map)
        return transformer
