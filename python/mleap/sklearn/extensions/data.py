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

from sklearn.impute import SimpleImputer as SKLearnImputer
from sklearn.preprocessing.data import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OneHotEncoder as SKLearnOneHotEncoder
from mleap.sklearn.preprocessing.data import MLeapSerializer, FeatureExtractor
import numpy as np
import uuid
from mleap.sklearn.preprocessing.data import ImputerSerializer
import pandas as pd

class OneHotEncoder(SKLearnOneHotEncoder, MLeapSerializer):
    def __init__(self, categories='auto', drop=None, sparse=True,
                 dtype=np.float64, handle_unknown='error',
                 drop_last=False, input_features=None, output_features=None):

        if handle_unknown == 'ignore' and drop_last:
            raise ValueError("MLeap cannot ignore unknown features AND drop the last feature column")

        self.name = "{}_{}".format(self.op, uuid.uuid4())
        self.drop_last = drop_last
        self.input_features = input_features
        self.output_features = output_features
        SKLearnOneHotEncoder.__init__(self, categories, drop, sparse, dtype, handle_unknown)

    def fit_transform(self, X, y=None):
        res = super().fit_transform(X, y)
        if self.drop_last:
            res = res[:,:-1]

        if self.sparse:
            return res.todense()
        return res

    def serialize_to_bundle(self, path, model_name):

        for ith_categories in transformer.categories:
            if not np.array_equal(ith_categories, np.arange(ith_categories.size)):
                raise ValueError("All one-hot encoded features must be category indices")
        if transformer.drop is not None:
            raise ValueError("The OneHotEncoder `drop` parameter is not supported by MLeap")

        # compile tuples of mode attributes to serialize
        attributes = list()
        attributes.append(('category_sizes', transformer.categories_[0]))
        if transformer.handle_unknown == 'error':
            attributes.append(('handle_invalid', 'error'))
            attributes.append(('drop_last', drop_last))
        else:
            attributes.append(('handle_invalid', 'keep'))  # OneHotEncoderModel.scala adds an extra column when keeping invalid data
            attributes.append(('drop_last', True))  # drop that extra column to match sklearn's ignore behavior

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

    def __init__(self, missing_values=np.nan, strategy="mean",
                 fill_value=None, verbose=0, copy=True, add_indicator=False,
                 input_features=None, output_features=None):
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