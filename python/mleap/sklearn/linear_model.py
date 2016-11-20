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
from sklearn.linear_model import LogisticRegression


def serializeToBundle(self, path):
    serializer = SimpleSparkSerializer()
    serializer.serializeToBundle(self, path)


def deserializeFromBundle(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.deserializeFromBundle(path)

setattr(LinearRegression, 'serializeToBundle', serializeToBundle)
setattr(LinearRegression.__class__, 'deserializeFromBundle', deserializeFromBundle)

setattr(LogisticRegression, 'serializeToBundle', serializeToBundle)
setattr(LogisticRegression.__class__, 'deserializeFromBundle', deserializeFromBundle)


class SimpleSparkSerializer(object):
    def __init__(self):

    def get_mleap_model(self):
        js = {
            'op': self.op,
            "attributes": [{
            "name": "coefficients",
            "type": {
              "type": "tensor",
              "tensor": {
                "base": "double",
                "dimensions": [-1]
              }
            },
            "value": self.coef_.tolist()
          }, {
            "name": "intercept",
            "type": "double",
            "value": self.intercept_.tolist()
          }, {
            "name": "num_classes",
            "type": "long",
            "value": 2
          }]
        }
        return js

    def get_mleap_node(self):

        js = {
          "name": self.name,
          "shape": {
            "inputs": [{
              "name": self.input_features,
              "port": "features"
            }],
            "outputs": [{
              "name": "label_prediction",
              "port": "prediction"
            }, {
              "name": "label_probability",
              "port": "probability"
            }]
          }
        }
        return js

    def serializeToBundle(self, transformer, path):
        self._java_obj.serializeToBundle(transformer._to_java(), path)

    def deserializeFromBundle(self, path):
        return JavaTransformer._from_java( self._java_obj.deserializeFromBundle(path))