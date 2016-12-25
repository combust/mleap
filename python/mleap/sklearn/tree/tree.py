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

from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import DecisionTreeRegressor
from mleap.bundle.serialize import MLeapSerializer
from sklearn.tree import _tree
import os
import json
import uuid


def mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "{}_{}".format(self.op, uuid.uuid1())


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


setattr(DecisionTreeRegressor, 'op', 'decision_tree_regression')
setattr(DecisionTreeRegressor, 'minit', mleap_init)
setattr(DecisionTreeRegressor, 'serialize_to_bundle', serialize_to_bundle)
setattr(DecisionTreeRegressor, 'serializable', True)

setattr(DecisionTreeClassifier, 'op', 'decision_tree_classifier')
setattr(DecisionTreeClassifier, 'minit', mleap_init)
setattr(DecisionTreeClassifier, 'serialize_to_bundle', serialize_to_bundle)
setattr(DecisionTreeClassifier, 'serializable', True)


class SimpleSparkSerializer(MLeapSerializer):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()

    @staticmethod
    def serialize_tree(tree, feature_names, outfile):
        """
        :type feature_names: list
        :type tree: sklearn.tree.tree.BaseDecisionTree
        :param tree: sklearn.tree.tree
        :param feature_names:
        :return:
        """

        tree_ = tree.tree_
        feature_names = [feature_names[i] if i != _tree.TREE_UNDEFINED else 'n/a' for i in tree_.feature]

        def traverse(node, depth, outfile):
            if tree_.feature[node] != _tree.TREE_UNDEFINED:
                name = feature_names[node]
                threshold = tree_.threshold[node]

                # Define internal node for serialization
                internal_node = {
                    'type': 'internal',
                    'split': {
                        'type': 'continuous',
                        'featureIndex': feature_names.index(name),
                        'threshold': threshold
                    }
                }

                # Serialize the internal Node
                json.dump(internal_node, outfile)
                outfile.write('\n')

                # Traverse Left
                traverse(tree_.children_left[node], depth + 1, outfile)

                # Traverse Rigiht
                traverse(tree_.children_right[node], depth + 1, outfile)
            else:
                leaf_node = {
                    'type': 'leaf',
                    'values': tree_.value[node].tolist()[0]
                }

                # Serialize the leaf node
                json.dump(leaf_node, outfile)
                outfile.write('\n')

        traverse(0, 1, outfile)

    def serialize_to_bundle(self, transformer, path, model):
        """
        :type transformer: sklearn.ensemble.forest.BaseForest
        :param transformer:
        :param path:
        :param model:
        :return:
        """

        # Serialize the random forest transformer and then move on to serializing each node

        # make pipeline directory
        model_dir = "{}/{}".format(path, model)
        os.mkdir(model_dir)

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
        attributes = list()
        if transformer.n_outputs_ > 1:
            attributes.append(('num_classes', transformer.n_outputs_)) # TODO: get number of classes from the transformer

        self.serialize(transformer, model_dir, transformer.name, attributes, inputs, outputs)

        rf_path = "{}/{}.node".format(model_dir, transformer.name)

        estimators = transformer.estimators_

        # make pipeline directory
        tree_name = "tree{}".format(i)
        tree_dir = "{}/{}".format(rf_path, tree_name)
        os.mkdir(tree_dir)

        # Serialize tree.json
        with open("{}/tree.json".format(tree_dir), 'w') as outfile:
            self.serialize_tree(transformer, transformer.input_features, outfile)

        # Serialize model.json
        # Define attributes
        attributes = list()
        attributes.append(('num_features', len(transformer.input_features)))
        if estimator.n_classes_ > 1:
            attributes.append(('num_classes', estimator.n_classes_))

        with open("{}/model.json".format(tree_dir), 'w') as outfile:
            json.dump(self.get_mleap_model(transformer, attributes), outfile, indent=3)

