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
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing.data import _transform_selected
from mleap.sklearn.preprocessing.data import MLeapSerializer
import numpy as np
import uuid


class OneHotEncoder(OneHotEncoder, MLeapSerializer):
    def __init__(self, input_features, output_features, drop_last=False, n_values="auto", categorical_features="all",
                 dtype=np.float, sparse=True, handle_unknown='error'):
        self.op = 'one_hot_encoder'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.serializable = True
        self.drop_last = drop_last
        self.n_values = n_values
        self.categorical_features = categorical_features
        self.dtype = dtype
        self.sparse = sparse
        self.handle_unknown = handle_unknown
        self.input_features = input_features
        self.output_features = output_features

    def fit_transform(self, X, y=None):
        res = _transform_selected(X, self._fit_transform, self.categorical_features, copy=True)
        if self.drop_last:
            res = res[:,:-1]

        if self.sparse:
            return res.todense()
        return res

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of mode attributes to serialize
        attributes = list()
        attributes.append(['size', transformer.n_values_.tolist()[0]])
        attributes.append(['drop_last', self.drop_last])

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


class DefineEstimator(BaseEstimator, TransformerMixin):
    def __init__(self, transformer):
        """
        Selects all but the last column of a matrix and passes it as the X variable into the transformer,
        and the last column as the y variable.

        This transformer is useful when we need to run a transformer or a series of transformers on the y-variable
        :type transformer: BaseEstimator
        :param transformer: Estimator (linear regression, random forest, etc)
        :return: Estimator
        """
        self.op = 'define_estimator'
        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.transformer = transformer
        self.serializable=False

    def fit(self, X, y, sample_weight=None):
        return self.transformer.fit(X[:, :-1], X[:, -1:])

    def transform(self, X):
        return self.transformer.predict(X[:,:-1])

    def fit_transform(self, X, y=None, **fit_params):
        return self.transformer.fit_transform(X[:,:-1], X[:,-1:])

    def predict(self, X):
        return self.transformer.predict(X[:,:-1])