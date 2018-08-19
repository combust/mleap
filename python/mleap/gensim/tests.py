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
import numpy as np

from mleap.gensim.word2vec import Word2Vec


class TransformerTests(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = "/tmp/mleap.python.tests/{}".format(uuid.uuid1())

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir)

    def tearDown(self):
        #shutil.rmtree(self.tmp_dir)
        pass

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

        model_ = Word2Vec(sentences4word2vec_, min_count=2, size=size_, window=window_)
        model_.mlinit(input_features=['sentence'], prediction_column = 'sentence_vector')

        model_.serialize_to_bundle(self.tmp_dir, model_.name)

        res = model_.sent2vec(['call', 'me', 'on', 'my', 'cell', 'phone'])

        self.assertEqual(5, res.size)




