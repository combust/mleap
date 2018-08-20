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
import json
import uuid


def mleap_init(self, input_features, prediction_column, feature_names):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.feature_names = feature_names
    self.name = "{}_{}".format(self.op, uuid.uuid1())


def serialize_to_bundle(self, path, model_name, serialize_node=True):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name, serialize_node=serialize_node)


setattr(DecisionTreeRegressor, 'op', 'decision_tree_regression')
setattr(DecisionTreeRegressor, 'mlinit', mleap_init)
setattr(DecisionTreeRegressor, 'serialize_to_bundle', serialize_to_bundle)
setattr(DecisionTreeRegressor, 'serializable', True)

setattr(DecisionTreeClassifier, 'op', 'decision_tree_classifier')
setattr(DecisionTreeClassifier, 'mlinit', mleap_init)
setattr(DecisionTreeClassifier, 'serialize_to_bundle', serialize_to_bundle)
setattr(DecisionTreeClassifier, 'serializable', True)


class SimpleSerializer(MLeapSerializer):
    def __init__(self):
        super(SimpleSerializer, self).__init__()

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
        feature_name = [feature_names[i] if i != _tree.TREE_UNDEFINED else 'n/a' for i in tree_.feature]

        def traverse(node, depth, outfile):
            if tree_.feature[node] != _tree.TREE_UNDEFINED:
                name = feature_name[node]
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

    def serialize_to_bundle(self, transformer, path, model_name, serialize_node=True):
        """
        :type transformer: sklearn.tree.tree.BaseDecisionTree
        :type path: str
        :type model_name: str
        :type serialize_node: bool
        :param transformer:
        :param path:
        :param model_name:
        :return:
        """

        # Define attributes
        attributes = list()
        attributes.append(('num_features', transformer.n_features_))
        if isinstance(transformer, DecisionTreeClassifier):
            attributes.append(('num_classes', int(transformer.n_classes_)))

        inputs = []
        outputs = []
        if serialize_node:
            # define node inputs and outputs
            inputs = [{
                      "name": transformer.input_features,
                      "port": "features"
                    }]

            outputs = [{
                      "name": transformer.prediction_column,
                      "port": "prediction"
                    }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs, node=serialize_node)

        # Serialize tree.json
        tree_path = "{}/{}.node/tree.json".format(path, model_name)
        if not serialize_node:
            tree_path = "{}/{}/tree.json".format(path, model_name)
        with open(tree_path, 'w') as outfile:
            self.serialize_tree(transformer, transformer.feature_names, outfile)
