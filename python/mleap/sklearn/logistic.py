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

from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LogisticRegressionCV
import uuid


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

setattr(LogisticRegression, 'get_mleap_model', get_mleap_model)
setattr(LogisticRegression, 'op', 'logistic_regression')
setattr(LogisticRegression, 'name', "{}_{}".format('logistic_regression', uuid.uuid1()))
setattr(LogisticRegression, 'set_prediction_column', set_prediction_column)
setattr(LogisticRegression, 'set_input_features', set_input_features)

setattr(LogisticRegressionCV, 'get_mleap_model', get_mleap_model)
setattr(LogisticRegressionCV, 'op', 'logistic_regression')
setattr(LogisticRegressionCV, 'name', "{}_{}".format('logistic_regression_cv', uuid.uuid1()))
setattr(LogisticRegressionCV, 'set_prediction_column', set_prediction_column)
setattr(LogisticRegressionCV, 'set_input_features', set_input_features)


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()
        self.prediction_column = None

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
          }, {
            "name": "num_classes",
            "type": "long",
            "value": 2
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
