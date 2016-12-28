import os
import shutil
import json
import numpy as np

_type_map = {
    int: 'long',
    float: 'double',
    str: 'string',
    np.float64: 'double'
}


class Vector(object):
    def __init__(self, values):
        self.values = values


class MLeapSerializer(object):
    """
    Base class to serialize transformers and estimators to a bundle.ml file. Main components that get serialized are:
        - Model: Contains the data needed for the transformer. For example, if the transformer is a linear regression,
                then we serialize the coefficients and the intercept of the model.
        - Node: Contains the definition of the input and output data.
    """
    def __init__(self):
        pass

    def get_mleap_model(self, transformer, attributes_to_serialize=None):
        """
        Generates the model.json given a list of attributes, which are a tuple comprised of:
            - name
            - value
        Type is figured out automatically, but we should consider specifying it explicitly.
        Note: this only supports doubles and tensors that are vectors/lists of doubles.
        :param transformer:
        :param attributes_to_serialize: Tuple of (name, value)
        :return:
        """
        js = {
            'op': transformer.op
        }

        # If the transformer doesn't have any attributes, return just the op name
        if attributes_to_serialize is None:
            return js

        attributes = []

        for name, value in attributes_to_serialize:
            if isinstance(value, float):
                attribute = {
                    "name": name,
                    "type": "double",
                    "value": value
                  }
                attributes.append(attribute)

            elif isinstance(value, bool) and value in [True, False]:
                attribute = {
                    "name": name,
                    "type": "boolean",
                    "value": value
                  }
                attributes.append(attribute)

            elif isinstance(value, int):
                attribute = {
                    "name": name,
                    "type": "long",
                    "value": value
                  }
                attributes.append(attribute)
            elif isinstance(value, Vector):
                attribute = {
                    "name": name,
                    "type": {
                      "type": "list",
                      "base": "double"
                    },
                    "value": value.values
                  }
                attributes.append(attribute)
            elif isinstance(value, list) and (isinstance(value[0], np.float64) or isinstance(value[0], float)):
                base = type(value[0])
                attribute = {
                    "name": name,
                    "type": {
                      "type": "tensor",
                      "tensor": {
                        "base": _type_map[base],
                        "dimensions": [-1]
                      }
                    },
                    "value": value
                  }
                attributes.append(attribute)

            elif isinstance(value, list) and isinstance(value[0], str):
                attribute = {
                    "name": name,
                    "type": {
                      "type": "list",
                      "base": "string"
                    },
                    "value": value
                  }
                attributes.append(attribute)

            elif isinstance(value, np.ndarray):
                attribute = {
                    "name": name,
                    "type": {
                        "type": "tensor",
                        "tensor": {
                            "base": "double",
                            "dimension": list(value.shape)
                        }
                    },
                    "value": list(value.flatten())
                }
                attributes.append(attribute)

            elif isinstance(value, str):
                attribute = {
                    'name': name,
                    'type': 'string',
                    'value': value
                }
                attributes.append(attribute)

        js['attributes'] = attributes

        return js

    def get_mleap_node(self, transformer, inputs, outputs):
        js = {
              "name": transformer.name,
              "shape": {
                "inputs": inputs,
                "outputs": outputs
              }
            }
        return js

    def serialize(self, transformer, path, model_name, attributes, inputs, outputs, node=True, model=True):
        # If bundle path already exists, delete it and create a clean directory
        if node:
            if os.path.exists("{}/{}.node".format(path, model_name)):
                shutil.rmtree("{}/{}.node".format(path, model_name))

            model_dir = "{}/{}.node".format(path, model_name)
        else:
            if os.path.exists("{}/{}".format(path, model_name)):
                shutil.rmtree("{}/{}".format(path, model_name))

            model_dir = "{}/{}".format(path, model_name)

        os.mkdir(model_dir)

        if model:
            # Write bundle file
            with open("{}/{}".format(model_dir, 'model.json'), 'w') as outfile:
                json.dump(self.get_mleap_model(transformer, attributes), outfile, indent=3)

        if node:
            # Write node file
            with open("{}/{}".format(model_dir, 'node.json'), 'w') as outfile:
                json.dump(self.get_mleap_node(transformer, inputs, outputs), outfile, indent=3)
