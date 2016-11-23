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

import sklearn
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
import os
import json
import shutil
import uuid



def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    serializer.serialize_to_bundle(self, path, model_name)


def deserialize_from_bundle(self, path):
    serializer = SimpleSparkSerializer()
    return serializer.deserialize_from_bundle(path)

setattr(Pipeline, 'serialize_to_bundle', serialize_to_bundle)
setattr(Pipeline, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(Pipeline, 'op', 'pipeline')
setattr(Pipeline, 'name', "{}_{}".format('pipeline', uuid.uuid1()))


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        if os.path.exists("{}/{}".format(path, model_name)):
            shutil.rmtree("{}/{}".format(path, model_name))

        model_dir = "{}/{}".format(path, model_name)
        os.mkdir(model_dir)

        for step in [x[1] for x in transformer.steps]:
            name = step.name
            print(name)
            bundle_dir = "{}/{}/{}.node".format(path, model_name, name)
            os.mkdir(bundle_dir)
            if step.op == 'pipeline':
                # Write Model and Node .json files
                with open("{}/{}".format(bundle_dir, 'model.json'), 'w') as outfile:
                    json.dump(step.get_mleap_model(), outfile, indent=3)
                with open("{}/{}".format(bundle_dir, 'node.json'), 'w') as f:
                    json.dump(step.get_mleap_node(), f, indent=3)

                for step_i in [x[1] for x in step.steps]:
                    step_i.serialize_to_bundle(bundle_dir)
            else:
                step.serialize_to_bundle(bundle_dir)

            if isinstance(step, list):
                pass

    def deserialize_from_bundle(self, path):
        return NotImplementedError

    def get_bundle(self, transformer):
        js = {
          "name": transformer.name,
          "format": "json",
          "version": "0.4.0-SNAPSHOT",
          "nodes": [x.name for x in transformer.steps]
        }
        return js

