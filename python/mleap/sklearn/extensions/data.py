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

import uuid

import numpy as np
import pandas as pd
from mleap.sklearn.preprocessing.data import ImputerSerializer, FeatureExtractor
from sklearn.impute import SimpleImputer as SKLearnImputer
from sklearn.preprocessing.data import BaseEstimator, TransformerMixin


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


"""
Wrapper around the sklearn Imputer so that it can be used in pipelines. 
Delegates fit() and transform() methods to the sklearn transformer and uses the ImputerSerializer to serialize to bundle.

Instead of putting a FeatureExtractor ahead of the Imputer, we add the equivalent of FeatureExtractor's transform() method
in the fit() and transform() methods.

This is because the Imputer both in Spark and MLeap operates on a scalar value and if we were to add a feature extractor in
front of it, then it would serialize as operating on a tensor and thus, fail at scoring time. 
"""
class Imputer(SKLearnImputer):

    def __init__(self, input_features, output_features,
                 missing_values=np.nan, strategy="mean",
                 fill_value=None, verbose=0, copy=True, add_indicator=False):
        self.name = "{}_{}".format(self.op, uuid.uuid1())
        self.input_features = input_features
        self.output_features = output_features
        self.input_shapes = {'data_shape': [{'shape': 'scalar'}]}
        self.feature_extractor = FeatureExtractor(input_scalars=[input_features],
                                                  output_vector='extracted_' + output_features,
                                                  output_vector_items=[output_features])
        SKLearnImputer.__init__(self, missing_values, strategy, fill_value, verbose, copy, add_indicator)

    def fit(self, X, y=None):
        super(Imputer, self).fit(self.feature_extractor.transform(X))
        return self

    def transform(self, X):
        return pd.DataFrame(super(Imputer, self).transform(self.feature_extractor.transform(X)))

    def serialize_to_bundle(self, path, model_name):
        ImputerSerializer().serialize_to_bundle(self, path, model_name)
