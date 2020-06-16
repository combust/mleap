import unittest
import os
import shutil
import json
import tempfile
import uuid

from mleap.sklearn.tree.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.datasets import load_iris

class TransformerTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_decision_tree_classifier(self):
        X = [[0, 1], [1, 1]]
        Y = [0, 0]

        dt_classifier = DecisionTreeClassifier()
        dt_classifier = dt_classifier.fit(X, Y)
        dt_classifier.mlinit(input_features ='feature', prediction_column = 'pred', feature_names = ['a'])
        dt_classifier.serialize_to_bundle(self.tmp_dir, dt_classifier.name)

        expected_model = {
            "attributes": {
                "num_features": {
                    "long": 2
                },
                "num_classes": {
                    "long": 1
                }
            },
            "op": "decision_tree_classifier"
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, dt_classifier.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(dt_classifier.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['num_features']['long'], model['attributes']['num_features']['long'])
        self.assertEqual(expected_model['attributes']['num_classes']['long'], model['attributes']['num_classes']['long'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, dt_classifier.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(dt_classifier.name, node['name'])
        self.assertEqual(dt_classifier.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(dt_classifier.prediction_column, node['shape']['outputs'][0]['name'])

    def test_decision_tree_classifier_with_iris_dataset(self):
        iris = load_iris()

        dt_classifier = DecisionTreeClassifier()
        dt_classifier.mlinit(input_features = 'features', prediction_column = 'species', feature_names = iris.feature_names)
        dt_classifier = dt_classifier.fit(iris.data, iris.target)
        dt_classifier.serialize_to_bundle(self.tmp_dir, dt_classifier.name)

        expected_model = {
            "attributes": {
                "num_features": {
                    "long": 4
                },
                "num_classes": {
                    "long": 3
                }
            },
            "op": "decision_tree_classifier"
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, dt_classifier.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(dt_classifier.op, expected_model['op'])
        self.assertEqual(expected_model['attributes']['num_features']['long'], model['attributes']['num_features']['long'])
        self.assertEqual(expected_model['attributes']['num_classes']['long'], model['attributes']['num_classes']['long'])

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, dt_classifier.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(dt_classifier.name, node['name'])
        self.assertEqual(dt_classifier.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(dt_classifier.prediction_column, node['shape']['outputs'][0]['name'])
