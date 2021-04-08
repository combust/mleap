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

import shutil
import json
import os
import tempfile
import unittest
import uuid

from mleap.gensim.word2vec import Word2Vec


class TransformerTests(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_word2vec(self):

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

        size_ = 5
        window_ = 2

        model_ = Word2Vec(sentences4word2vec_, min_count=2, vector_size=size_, window=window_)
        model_.mlinit(input_features=['input'], prediction_column = 'sentence_vector')

        model_.serialize_to_bundle(self.tmp_dir, model_.name)

        res = model_.sent2vec(['call', 'me', 'on', 'my', 'cell', 'phone'])


        with open('{}/{}.node/node.json'.format(self.tmp_dir, model_.name)) as node_json:
            node = json.load(node_json)

        with open('{}/{}.node/model.json'.format(self.tmp_dir, model_.name)) as model_json:
            model = json.load(model_json)


        self.assertEqual(5, res.size)
        self.assertEqual(node['shape']['inputs'][0]['name'], ['input'])
        self.assertEqual(node['shape']['outputs'][0]['name'], 'sentence_vector')
        self.assertEqual(model['op'], 'word2vec')
