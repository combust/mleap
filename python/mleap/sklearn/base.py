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

from sklearn.linear_model import LinearRegression
import uuid
import json
import os


def get_mleap_model(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_model(self)

def get_mleap_node(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_node(self)

def set_prediction_column(self, prediction_column):
    serializer = SimpleSparkSerializer()
    return serializer.set_prediction_column(self, prediction_column)

def set_input_features(self, input_features):
    serializer = SimpleSparkSerializer()
    return serializer.set_input_features(self, input_features)


setattr(LinearRegression, 'get_mleap_model', get_mleap_model)
setattr(LinearRegression, 'get_mleap_node', get_mleap_node)
setattr(LinearRegression, 'op', 'linear_regression')
setattr(LinearRegression, 'name', "{}_{}".format('linear_regression', uuid.uuid1()))
setattr(LinearRegression, 'set_prediction_column', set_prediction_column)
setattr(LinearRegression, 'set_input_features', set_input_features)


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()
        self.prediction_column = None

    def serialize_to_bundle(self, transformer, path):
        """
        :param path:
        :return:
        """
        step_dir = "{}/{}.node".format(path, transformer.name)
        os.mkdir(step_dir)
        # Write Model and Node .json files
        with open("{}/{}".format(step_dir, 'model.json'), 'w') as outfile:
            json.dump(transformer.get_mleap_model(), outfile, indent=3)
        with open("{}/{}".format(step_dir, 'node.json'), 'w') as f:
            json.dump(transformer.get_mleap_node(), f, indent=3)

    def get_mleap_model(self, transformer):
        js = {
            'op': transformer.op,
            "attributes": [{
            "name": "coefficients",
            "type": {
              "type": "tensor",
              "tensor": {
                "base": "double",
                "dimensions": [-1]
              }
            },
            "value": transformer.coef_.tolist()
          }, {
            "name": "intercept",
            "type": "double",
            "value": transformer.intercept_.tolist()
          }]
        }
        return js

    def get_mleap_node(self, transformer):
        js = {
          "name": transformer.op,
          "shape": {
            "inputs": [{
              "name": transformer.input_features,
              "port": "features"
            }],
            "outputs": [{
              "name": transformer.prediction_column,
              "port": "prediction"
            }]
          }
        }
        return js

    def set_prediction_column(self, transformer, prediction_column):
        transformer.prediction_column = prediction_column

    def set_input_features(self, transformer, input_features):
        transformer.input_features = input_features
