import json
import numpy as np
import pandas as pd
import shutil
import tempfile
import unittest

from mleap.sklearn.extensions.data import Imputer

class ExtensionsTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def imputer_ext_test(self):

        def _set_nulls(df):
            row = df['index']
            if row in [2,5]:
                return np.NaN
            return df.a

        imputer = Imputer(strategy='mean', input_features='a', output_features='a_imputed')

        df2 = self.df
        df2.reset_index(inplace=True)
        df2['a'] = df2.apply(_set_nulls, axis=1)

        imputer.fit(df2)

        self.assertAlmostEqual(imputer.statistics_[0], df2.a.mean(), places = 7)

        imputer.serialize_to_bundle(self.tmp_dir, imputer.name)

        expected_model = {
            "op": "imputer",
            "attributes": {
                "surrogate_value": {
                    "double": df2.a.mean()
                },
                "strategy": {
                    "string": "mean"
                }
            }
        }

        # Test model.json
        with open("{}/{}.node/model.json".format(self.tmp_dir, imputer.name)) as json_data:
            model = json.load(json_data)

        self.assertEqual(expected_model['attributes']['strategy']['string'], model['attributes']['strategy']['string'])
        self.assertAlmostEqual(expected_model['attributes']['surrogate_value']['double'], model['attributes']['surrogate_value']['double'], places = 7)

        # Test node.json
        with open("{}/{}.node/node.json".format(self.tmp_dir, imputer.name)) as json_data:
            node = json.load(json_data)

        self.assertEqual(imputer.name, node['name'])
        self.assertEqual(imputer.input_features, node['shape']['inputs'][0]['name'])
        self.assertEqual(imputer.output_features, node['shape']['outputs'][0]['name'])
