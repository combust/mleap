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

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from mleap.sklearn.pipeline import SimpleSerializer as PipelineSerializer
from mleap.bundle.serialize import MLeapSerializer
import uuid


class ops(object):
    def __init__(self):
        self.COUNT_VECTORIZER = 'tokenizer'
        self.TF_IDF_VECTORIZER = 'pipeline'


ops = ops()


def serialize_to_bundle(self, path, model_name):
    serializer = SimpleSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def mleap_init(self, input_features, prediction_column):
    name = "{}_{}"

    if isinstance(self, TfidfVectorizer):
        name = "tfidf_{}_{}"

    self.name = name.format(self.op, uuid.uuid4())
    self.input_features = input_features
    self.prediction_column = prediction_column


setattr(TfidfVectorizer, 'op', ops.TF_IDF_VECTORIZER)
setattr(TfidfVectorizer, 'mlinit', mleap_init)
setattr(TfidfVectorizer, 'serialize_to_bundle', serialize_to_bundle)
setattr(TfidfVectorizer, 'serializable', True)

setattr(CountVectorizer, 'op', ops.COUNT_VECTORIZER)
setattr(CountVectorizer, 'mlinit', mleap_init)
setattr(CountVectorizer, 'serialize_to_bundle', serialize_to_bundle)
setattr(CountVectorizer, 'serializable', True)


class SimpleSerializer(object):
    def __init__(self):
        super(SimpleSerializer, self).__init__()

    @staticmethod
    def _choose_serializer(transformer):
        serializer = None

        if transformer.op == ops.COUNT_VECTORIZER:
            serializer = CountVectorizerSerializer()
        elif transformer.op == ops.TF_IDF_VECTORIZER:
            serializer = TfidfVectorizerSerializer()

        return serializer

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    def serialize_to_bundle(self, transformer, path, model_name):
        serializer = self._choose_serializer(transformer)
        serializer.serialize_to_bundle(transformer, path, model_name)


class CountVectorizerSerializer(MLeapSerializer):
    def __init__(self):
        super(CountVectorizerSerializer, self).__init__()

    def serialize_to_bundle(self, transformer, path, model_name):

        # compile tuples of model attributes to serialize
        attributes = None

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


class TfidfVectorizerSerializer(MLeapSerializer):
    pipeline_serializer = PipelineSerializer()

    def __init__(self):
        super(TfidfVectorizerSerializer, self).__init__()

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    def serialize_to_bundle(self, transformer, path, model_name):
        num_features = transformer.idf_.shape[0]
        vocabulary = [None] * num_features

        for term, termidx in transformer.vocabulary_.items():
            vocabulary[termidx] = str(term)

        tf_attributes = [
            ('vocabulary', vocabulary),
            ('binary', transformer.binary),
            # no concept of 'min_tf' in scikit's TfidfVectorizer
            ('min_tf', 0)
        ]
        tf_inputs = [{
            "name": transformer.input_features,
            "port": "input"
        }]
        tf_outputs = [{
            "name": "token_counts",
            "port": "output"
        }]
        tf_step = TfidfStep(transformer, 'count_vectorizer', tf_attributes, tf_inputs, tf_outputs)

        idf_attributes = [
            ('idf', transformer.idf_.tolist())
        ]
        idf_inputs = [{
            "name": tf_outputs[0]['name'],
            "port": "input"
        }]
        idf_outputs = [{
            "name": transformer.prediction_column,
            "port": "output"
        }]
        idf_step = TfidfStep(transformer, 'idf', idf_attributes, idf_inputs, idf_outputs)

        pipeline_step = TfidfStep(transformer, 'pipeline', None, None, None)
        pipeline_step.steps = [('tf', tf_step), ('idf', idf_step)]
        self.pipeline_serializer.serialize_to_bundle(pipeline_step, path, model_name, True)


class TfidfStep(MLeapSerializer):
    def __init__(self, original_transformer, op, attributes, inputs, outputs):
        super(TfidfStep, self).__init__()

        self.op = op
        self.attributes = attributes
        self.inputs = inputs
        self.outputs = outputs
        self.name = "{}_{}".format(original_transformer.name, op)
        self.serializable = True

    def serialize_to_bundle(self, path, model_name):
        self.serialize(self, path, model_name, self.attributes, self.inputs, self.outputs)
