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

from sklearn.ensemble.forest import RandomForestRegressor
from sklearn.ensemble.forest import RandomForestClassifier
from mleap.bundle.serialize import MLeapSerializer
from mleap.bundle.serialize import Vector
import mleap.sklearn.tree.tree
import uuid


def mleap_init(self, input_features, prediction_column, feature_names):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.feature_names = feature_names
    self.name = "{}_{}".format(self.op, uuid.uuid1())


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


setattr(RandomForestRegressor, 'op', 'random_forest_regression')
setattr(RandomForestRegressor, 'mlinit', mleap_init)
setattr(RandomForestRegressor, 'serialize_to_bundle', serialize_to_bundle)
setattr(RandomForestRegressor, 'serializable', True)

setattr(RandomForestClassifier, 'op', 'random_forest_classifier')
setattr(RandomForestClassifier, 'mlinit', mleap_init)
setattr(RandomForestClassifier, 'serialize_to_bundle', serialize_to_bundle)
setattr(RandomForestClassifier, 'serializable', True)


class SimpleSerializer(MLeapSerializer):
    def __init__(self):
        super(SimpleSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model):
        """
        :param transformer: Random Forest Regressor or Classifier
        :param path: Root path where to serialize the model
        :param model: Name of the model to be serialized
        :type transformer: sklearn.ensemble.forest.BaseForest
        :type path: str
        :type model: str
        :return: None
        """

        # Define Node Inputs and Outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "features"
                }]

        outputs = list()
        outputs.append({
                  "name": transformer.prediction_column,
                  "port": "prediction"
                })

        outputs.append({
              "name": "raw_prediction",
              "port": "raw_prediction"
             })

        outputs.append({
              "name": "probability",
              "port": "probability"
            })

        # compile tuples of model attributes to serialize
        tree_weights = Vector([1.0 for x in range(0, len(transformer.estimators_))])
        attributes = list()
        attributes.append(('num_features', transformer.n_features_))
        attributes.append(('tree_weights', tree_weights))
        attributes.append(('trees', ["tree{}".format(x) for x in range(0, len(transformer.estimators_))]))
        if isinstance(transformer, RandomForestClassifier):
            attributes.append(('num_classes', transformer.n_classes_)) # TODO: get number of classes from the transformer

        self.serialize(transformer, path, model, attributes, inputs, outputs)

        rf_path = "{}/{}.node".format(path, model)

        estimators = transformer.estimators_

        i = 0
        for estimator in estimators:
            estimator.mlinit(input_features = transformer.input_features, prediction_column = transformer.prediction_column, feature_names=transformer.feature_names)
            model_name = "tree{}".format(i)
            estimator.serialize_to_bundle(rf_path, model_name, serialize_node=False)

            i += 1
