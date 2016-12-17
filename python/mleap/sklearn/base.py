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
from mleap.bundle.serialize import MLeapSerializer
import uuid


def get_mleap_model(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_model(self)


def get_mleap_node(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.get_mleap_node(self)


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def set_prediction_column(self, prediction_column):
    serializer = SimpleSparkSerializer()
    return serializer.set_prediction_column(self, prediction_column)


def set_input_features(self, input_features):
    serializer = SimpleSparkSerializer()
    return serializer.set_input_features(self, input_features)


setattr(LinearRegression, 'serialize_to_bundle', serialize_to_bundle)
setattr(LinearRegression, 'op', 'linear_regression')
setattr(LinearRegression, 'name', "{}_{}".format('linear_regression', uuid.uuid1()))
setattr(LinearRegression, 'set_prediction_column', set_prediction_column)
setattr(LinearRegression, 'set_input_features', set_input_features)
setattr(LinearRegression, 'serializable', True)


class SimpleSparkSerializer(MLeapSerializer):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('intercept', transformer.intercept_.tolist()[0]))
        attributes.append(('coefficients', transformer.coef_.tolist()[0]))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "features"
                  }]

        outputs = [{
                  "name": transformer.prediction_column,
                  "port": "prediction"
                   }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)
