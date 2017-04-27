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

from sklearn.decomposition.pca import PCA
from mleap.bundle.serialize import MLeapSerializer
import uuid


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "{}_{}".format(self.op, uuid.uuid4())


setattr(PCA, 'op', 'pca')
setattr(PCA, 'mlinit', mleap_init)
setattr(PCA, 'serialize_to_bundle', serialize_to_bundle)
setattr(PCA, 'serializable', True)


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
        attributes.append(('principal_components', transformer.components_))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                  }]

        outputs = [{
                  "name": transformer.prediction_column,
                  "port": "output"
                   }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)
