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

from sklearn.preprocessing.data import BaseEstimator, TransformerMixin
from mleap.bundle.serialize import MLeapSerializer
from gensim.models import Word2Vec
import uuid
import numpy as np
import pandas as pd


class MLeapWord2Vec(BaseEstimator, TransformerMixin, MLeapSerializer):
    def __init__(self, input_features, output_features, kernel='sqrt', size=35, window=5, min_count=5):
        self.input_features = input_features
        self.output_features = output_features
        self.op = 'word2vec'
        self.name = "{}_{}".format(self.op, str(uuid.uuid4()))
        self.serializable = True
        self.kernel = kernel
        self.model = None
        self.output_features_pandas = []

        self.size=35
        self.window=5
        self.min_count=5

    def fit(self, X, y=None, **fit_params):
        """
        Fit Gensim's Word2vec model
        :param X: Pandas Series of List of Lists that contain words in each sentence
        :return: self
        """

        # TODO: add the other parameters
        self.model = Word2Vec(X[self.input_features], size=self.size, window=self.window, min_count=self.min_count)

        if self.model.vector_size < 100:
            zfill = 2
        elif self.model.vector_size < 1000:
            zfill = 3
        elif self.model.vector_size < 10000:
            zfill = 4
        else:
            zfill = 5

        self.output_features_pandas = ["{}_{}".format(self.op, str(i).zfill(zfill)) for i in list(range(self.model.vector_size))]

        self.null_placeholder = list([0.0 for x in range(self.model.vector_size)])

        return self

    def transform(self, X):

        if self.kernel == 'sqrt':

            return X[self.input_features].apply(self.sent2vec_sqrt)

    def fit_transform(self, X, y=None, **fit_params):

        self.fit(X, **fit_params)
        return self.transform(X)

    def serialize_to_bundle(self, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = list()
        attributes.append(('words', self.model.wv.index2word))

        # indices = [np.float64(x) for x in list(range(len(transformer.wv.index2word)))]
        word_vectors = np.array([float(y) for x in [self.model.wv.word_vec(w) for w in self.model.wv.index2word] for y in x])

        # attributes.append(('indices', indices))
        # Excluding indices because they are 0 - N
        attributes.append(('word_vectors', word_vectors))
        attributes.append(('kernel', self.kernel))

        # define node inputs and outputs
        inputs = [{
                  "name": self.input_features,
                  "port": "input"
                  }]

        outputs = [{
                  "name": self.output_features,
                  "port": "output"
                   }]

        self.serialize(self, path, model_name, attributes, inputs, outputs)

    def sent2vec_sqrt(self, words):
        """
        Used with sqrt kernel
        :param words:
        :param transformer:
        :return:
        """
        sent_vec = np.zeros(self.model.vector_size)
        numw = 0
        for w in words:
            try:
                sent_vec = np.add(sent_vec, self.model.wv[w])
                numw += 1
            except:
                continue
        y = sent_vec / np.sqrt(sent_vec.dot(sent_vec))
        if pd.isnull(y[0]):
            y = self.null_placeholder
        res = dict(zip(self.output_features_pandas, y))
        return pd.Series(res)
