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

from sklearn.pipeline import FeatureUnion
import os
import shutil
import uuid


def serialize_to_bundle(self, path, model_name):
    serializer = SimplekSerializer()
    serializer.serialize_to_bundle(self, path, model_name)


def deserialize_from_bundle(self, path):
    serializer = SimplekSerializer()
    return serializer.deserialize_from_bundle(path)


def mleap_init(self):
    self.name = "{}_{}".format(self.op, uuid.uuid1())

setattr(FeatureUnion, 'serialize_to_bundle', serialize_to_bundle)
setattr(FeatureUnion, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(FeatureUnion, 'op', 'feature_union')
setattr(FeatureUnion, 'mlinit', mleap_init)
setattr(FeatureUnion, 'serializable', True)


class SimplekSerializer(object):
    def __init__(self):
        super(SimplekSerializer, self).__init__()

    @staticmethod
    def serialize_to_bundle(transformer, path, model_name):

        for transformer in [x[1] for x in transformer.transformer_list]:

            if os.path.exists("{}/{}.node".format(path, transformer.name)):
                shutil.rmtree("{}/{}.node".format(path, transformer.name))

            model_dir = "{}/{}.node".format(path, transformer.name)
            os.mkdir(model_dir)

            if transformer.op == 'pipeline':
                # Write bundle file
                transformer.serialize_to_bundle(model_dir, transformer.name)
#
            if isinstance(transformer, list):
                pass

    @staticmethod
    def deserialize_from_bundle(self, path):
        return NotImplementedError
