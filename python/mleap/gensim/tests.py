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

import unittest
import shutil
import uuid
import os
import json

import pandas as pd
from mleap.gensim.word2vec import MLeapWord2Vec


class TransformerTests(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_word2vec(self):

        tf_wv = MLeapWord2Vec(input_features='sentence', output_features='sentence_vector')

        sentences4word2vec_ = [
                    ['call', 'me', 'tomorrow'],
                    ['give', 'me', 'a', 'call', 'in',' the', 'after', 'noon'],
                    ['when', 'can', 'i', 'call'],
                    ['when', 'is', 'the', 'best', 'time', 'to', 'call'],
                    ['call', 'me', 'tomorrow', 'after', 'noon'],
                    ['i', 'would', 'like', 'a', 'call', 'tomorrow'],
                    ['do', 'not', 'call', 'until', 'tomorrow'],
                    ['best', 'time', 'is', 'tomorrow', 'after', 'noon'],
                    ['call', 'tomorrow', 'after', 'lunch'],
                    ['call', 'after', 'lunch', 'time'],
                    ['make', 'the', 'call', 'tomorrow'],
                    ['make', 'the', 'call', 'tomorrow', 'after', 'noon'],
                    ['make', 'the' 'call', 'after', 'lunch', 'time']
                ]

        X = pd.DataFrame({'sentence': sentences4word2vec_})

        tf_wv.fit(X, size=5, window=2, min_count=2)

        tf_wv.serialize_to_bundle(self.tmp_dir, tf_wv.name)

        Y = pd.DataFrame({'sentence': [['call', 'me', 'on', 'my', 'cell', 'phone'], ['call', 'me', 'on', 'my', 'cell', 'phone']]})

        res = tf_wv.transform(Y)

        with open('{}/{}.node/node.json'.format(self.tmp_dir, tf_wv.name)) as node_json:
            node = json.load(node_json)

        with open('{}/{}.node/model.json'.format(self.tmp_dir, tf_wv.name)) as model_json:
            model = json.load(model_json)

        self.assertEqual(5, len(res.sentence_vector.values[0]))
        self.assertEqual(node['shape']['inputs'][0]['name'], 'sentence')
        self.assertEqual(node['shape']['outputs'][0]['name'], 'sentence_vector')
        self.assertEqual(model['op'], 'word2vec')

        print(res)
        self.assertEqual(0,1)


