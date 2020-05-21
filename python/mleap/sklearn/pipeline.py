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

from sklearn.pipeline import Pipeline
from mleap import __version__
import os
import json
import shutil
import uuid
import zipfile
import datetime

def serialize_to_bundle(self, path, model_name, init=False):
    serializer = SimpleSerializer()
    serializer.serialize_to_bundle(self, path, model_name, init)


def deserialize_from_bundle(self, path):
    serializer = SimpleSerializer()
    return serializer.deserialize_from_bundle(path)


def mleap_init(self):
    self.name = "{}_{}".format(self.op, uuid.uuid1())


setattr(Pipeline, 'serialize_to_bundle', serialize_to_bundle)
setattr(Pipeline, 'deserialize_from_bundle', deserialize_from_bundle)
setattr(Pipeline, 'op', 'pipeline')
setattr(Pipeline, 'mlinit', mleap_init)
setattr(Pipeline, 'serializable', True)


class SimpleSerializer(object):
    def __init__(self):
        super(SimpleSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name, init=False):

        model_dir = path
        if init:
            # If bundle path already exists, delte it and create a clean directory
            if os.path.exists("{}/{}".format(path, model_name)):
                shutil.rmtree("{}/{}".format(path, model_name))

            # make pipeline directory
            model_dir = "{}/{}".format(path, model_name)
            os.mkdir(model_dir)

            # make pipeline root directory
            root_dir = "{}/root".format(model_dir)
            os.mkdir(root_dir)

            # Write Pipeline Bundle file
            with open("{}/{}".format(model_dir, 'bundle.json'), 'w') as outfile:
                json.dump(self.get_bundle(transformer), outfile, indent=3)

            model_dir = root_dir

            # Write the model and node files
            with open("{}/{}".format(model_dir, 'model.json'), 'w') as outfile:
                json.dump(self.get_model(transformer), outfile, indent=3)

            with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
                json.dump(self.get_node(transformer), outfile, indent=3)

        else:
            # Write model file
            with open("{}/{}".format(model_dir, 'model.json'), 'w') as outfile:
                json.dump(self.get_model(transformer), outfile, indent=3)

            # Write node file
            with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
                json.dump(self.get_node(transformer), outfile, indent=3)

        for step in [x[1] for x in transformer.steps if hasattr(x[1], 'serialize_to_bundle')]:
            name = step.name

            if step.op == 'pipeline':
                # Create the node directory
                bundle_dir = "{}/{}.node".format(model_dir, name)
                os.mkdir(bundle_dir)

                # Write model file
                with open("{}/{}".format(bundle_dir, 'model.json'), 'w') as outfile:
                    json.dump(self.get_model(step), outfile, indent=3)

                # Write node file
                with open("{}/{}".format(bundle_dir, 'node.json'), 'w') as outfile:
                    json.dump(self.get_node(step), outfile, indent=3)

                for step_i in [x[1] for x in step.steps]:
                    step_i.serialize_to_bundle(bundle_dir, step_i.name)

            elif step.op == 'feature_union':
                step.serialize_to_bundle(model_dir, step.name)
            else:
                step.serialize_to_bundle(model_dir, step.name)

            if isinstance(step, list):
                pass

        if init:
            zip_pipeline(path, model_name)

    def deserialize_from_bundle(self, path):
        return NotImplementedError

    @staticmethod
    def get_bundle(transformer):
        js = {
          "name": transformer.name,
          "format": "json",
          "version": __version__,
          "timestamp": datetime.datetime.now().isoformat(),
          "uid": "{}".format(uuid.uuid4())
        }
        return js

    @staticmethod
    def get_node(transformer):
        js = {
          "name": transformer.name,
          "shape": {
            "inputs": [],
            "outputs": []
          }
        }
        return js

    def get_model(self, transformer):
        js = {
          "op": transformer.op,
            "attributes": {
                "nodes": {
                    "type": "list",
                    "string": self._extract_nodes(transformer.steps)
                }
            }
        }
        return js

    @staticmethod
    def _extract_nodes(steps):
        pipeline_steps = []
        for name, step in steps:
            if step.op == 'feature_union':
                union_steps = [x[1].name for x in step.transformer_list if hasattr(x[1], 'serialize_to_bundle') and x[1].serializable]
                pipeline_steps += union_steps
            elif hasattr(step, 'serialize_to_bundle') and step.serializable:
                pipeline_steps.append(step.name)
        return pipeline_steps


def zip_pipeline(path, name):
    zip_file = zipfile.ZipFile("{}/{}.zip".format(path, name), 'w', zipfile.ZIP_DEFLATED)
    abs_src = os.path.abspath("{}/{}".format(path, name))
    for root, dirs, files in os.walk("{}/{}".format(path, name)):
        for file in files:
            absname = os.path.abspath(os.path.join(root, file))
            arcname = absname[len(abs_src) + 1:]

            zip_file.write(os.path.join(root, file), arcname)
    zip_file.close()