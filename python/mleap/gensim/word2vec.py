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

from gensim.models import Word2Vec
from mleap.bundle.serialize import MLeapSerializer
import uuid
import numpy as np


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSparkSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def sent2vec(self, words):
    serializer = SimpleSparkSerializer()
    return serializer.sent2vec(words, self)


def mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "{}_{}".format(self.op, uuid.uuid4())

setattr(Word2Vec, 'op', 'word2vec')
setattr(Word2Vec, 'mlinit', mleap_init)
setattr(Word2Vec, 'serialize_to_bundle', serialize_to_bundle)
setattr(Word2Vec, 'serializable', True)
setattr(Word2Vec, 'sent2vec', sent2vec)


class SimpleSparkSerializer(MLeapSerializer):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('words', transformer.wv.index_to_key))

        # indices = [np.float64(x) for x in list(range(len(transformer.wv.index_to_key)))]
        word_vectors = np.array([float(y) for x in [transformer.wv.word_vec(w) for w in transformer.wv.index_to_key] for y in x])

        # attributes.append(('indices', indices))
        # Excluding indices because they are 0 - N
        attributes.append(('word_vectors', word_vectors))
        attributes.append(('kernel', 'sqrt'))

        # define node inputs and outputs
        inputs = [{
                  "name": transformer.input_features,
                  "port": "input"
                  }]

        outputs = [{
                  "name": transformer.prediction_column,
                  "port": "output"
                   }]

        self.serialize(transformer, path, model_name, attributes, inputs, outputs)

    def sent2vec(self, words, transformer):
        """
        Used with sqrt kernel
        :param words:
        :param transformer:
        :return:
        """
        sent_vec = np.zeros(transformer.vector_size)
        numw = 0
        for w in words:
            try:
                sent_vec = np.add(sent_vec, transformer.wv[w])
                numw += 1
            except:
                continue
        return sent_vec / np.sqrt(sent_vec.dot(sent_vec))
