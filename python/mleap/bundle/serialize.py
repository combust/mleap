import os
import shutil
import six
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

        attributes = {}

        for name, value in attributes_to_serialize:
            if isinstance(value, float):
                attributes[name] = {
                    "double": value
                }

            elif isinstance(value, bool) and value in [True, False]:
                attributes[name] = {
                    "boolean": value
                }

            elif isinstance(value, int):
                attributes[name] = {
                    "long": value
                }
            elif isinstance(value, Vector):
                attributes[name] = {
                    "type": "list",
                    "double": value.values
                }
            elif isinstance(value, list) and (isinstance(value[0], np.float64) or isinstance(value[0], np.float32) or isinstance(value[0], float)):
                base = type(value[0])
                attributes[name] = {
                    _type_map[base]: value,
                    "shape": {
                        "dimensions": [{
                            "size": len(value),
                            "name": ""
                        }]
                    },
                    "type": "tensor"
                }
            elif isinstance(value, list) and isinstance(value[0], str):
                attributes[name] = {
                    "type": "list",
                    "string": value
                }

            elif isinstance(value, np.ndarray):
                attributes[name] = {
                    "double": value.flatten().tolist(),
                    "shape": {
                        "dimensions": [{
                            "size": dim,
                            "name": ""
                        } for dim in value.shape]
                    },
                    "type": "tensor"
                }

            elif isinstance(value, str):
                attributes[name] = {
                    'string': value
                }

            elif isinstance(value, dict):
                shapes = list()
                for shape in value['data_shape']:
                    if shape['shape'] == 'scalar':
                        shapes.append(({"base": "scalar",
                                        "isNullable": False}))
                    elif shape['shape'] == 'tensor':
                        shapes.append(({
                            "base": "tensor",
                            "isNullable": False,
                            "tensorShape": {
                                "dimensions": [{
                                    "size": shape['tensor_shape']['dimensions'][0]['size'],
                                    "name": ""
                                }]
                            }
                        }))
                attributes[name] = {
                    'type': 'list',
                    'data_shape': shapes
                }

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


class MLeapDeserializer(object):

    def deserialize_from_bundle(self, transformer, node_path, node_name):
        """
        :type node_path: str
        :type node_name: str
        :type transformer: StandardScaler
        :param transformer:
        :param node_path:
        :param node_name:
        :return:
        """
        NotImplementedError()

    @staticmethod
    def _node_features_format(x):
        if isinstance(x, six.string_types):
            return [str(x)]
        return x

    def deserialize_single_input_output(self, transformer, node_path, attributes_map=None):
        """
        :attributes_map: Map of attributes names. For example StandardScaler has `mean_` but is serialized as `mean`
        :param transformer: Scikit or Pandas transformer
        :param node: bundle.ml node json file
        :param model: bundle.ml model json file
        :return: Transformer
        """
        # Load the model file
        with open("{}/model.json".format(node_path)) as json_data:
            model_j = json.load(json_data)

        # Set Transformer Attributes
        attributes = model_j['attributes']
        for attribute in attributes.keys():
            value_key = [key for key in attributes[attribute].keys()
                         if key in ['string', 'boolean', 'long', 'double', 'data_shape']][0]
            if attributes_map is not None and attribute in attributes_map.keys():
                setattr(transformer, attributes_map[attribute], attributes[attribute][value_key])
            else:
                setattr(transformer, attribute, attributes[attribute][value_key])

        transformer.op = model_j['op']

        # Load the node file
        with open("{}/node.json".format(node_path)) as json_data:
            node_j = json.load(json_data)

        transformer.name = node_j['name']
        transformer.input_features = self._node_features_format(node_j['shape']['inputs'][0]['name'])
        transformer.output_features = self._node_features_format(node_j['shape']['outputs'][0]['name'])

        return transformer
