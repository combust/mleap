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

from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from mleap.bundle.serialize import MLeapSerializer, MLeapDeserializer
import uuid
import numpy as np
import os


def mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "{}_{}".format(self.op, uuid.uuid1())


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def deserialize_from_bundle(self, path, node_name):
    serializer = SimpleSerializer()
    return serializer.deserialize_from_bundle(self, path, node_name)

setattr(SVC, 'op', 'svm')
setattr(SVC, 'mlinit', mleap_init)
setattr(SVC, 'serialize_to_bundle', serialize_to_bundle)
setattr(SVC, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(SVC, 'serializable', True)

setattr(LinearSVC, 'op', 'svm')
setattr(LinearSVC, 'mlinit', mleap_init)
setattr(LinearSVC, 'serialize_to_bundle', serialize_to_bundle)
setattr(LinearSVC, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(LinearSVC, 'serializable', True)


class SimpleSerializer(MLeapSerializer, MLeapDeserializer):
    def __init__(self):
        super(SimpleSerializer, self).__init__()

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
        attributes.append(('num_classes', len(transformer.classes_))) # TODO: get number of classes from the transformer

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

    def deserialize_from_bundle(self, transformer, node_path, node_name):

        attributes_map = {
            'coefficients': 'coef_',
            'intercept': 'intercept_'
        }

        # Set serialized attributes
        full_node_path = os.path.join(node_path, node_name)
        transformer = self.deserialize_single_input_output(transformer, full_node_path, attributes_map)

        # Set Additional Attributes
        if 'intercept_' in transformer.__dict__:
            transformer.fit_intercept = True
        else:
            transformer.fit_intercept = False

        transformer.coef_ = np.array([transformer.coef_])
        transformer.classes_ = np.array([0, 1])

        return transformer
