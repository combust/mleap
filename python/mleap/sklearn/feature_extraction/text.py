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


def count_vectorizer_serialize_to_bundle(self, path, model_name):
    serializer = CountVectorizerSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def count_vectorizer_mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "{}_{}".format(self.op, uuid.uuid4())


setattr(CountVectorizer, 'op', 'tokenizer')
setattr(CountVectorizer, 'mlinit', count_vectorizer_mleap_init)
setattr(CountVectorizer, 'serialize_to_bundle', count_vectorizer_serialize_to_bundle)
setattr(CountVectorizer, 'serializable', True)


def tfidf_vectorizer_serialize_to_bundle(self, path, model_name):
    serializer = TfidfVectorSerializer()
    return serializer.serialize_to_bundle(self, path, model_name)


def tfidf_vectorizer_mleap_init(self, input_features, prediction_column):
    self.input_features = input_features
    self.prediction_column = prediction_column
    self.name = "tfidf_{}_{}".format(self.op, uuid.uuid4())


setattr(TfidfVectorizer, 'op', 'pipeline')
setattr(TfidfVectorizer, 'mlinit', tfidf_vectorizer_mleap_init)
setattr(TfidfVectorizer, 'serialize_to_bundle', tfidf_vectorizer_serialize_to_bundle)
setattr(TfidfVectorizer, 'serializable', True)


class CountVectorizerSerializer(MLeapSerializer):
    def __init__(self):
        super(CountVectorizerSerializer, self).__init__()

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

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


class TfidfVectorSerializer(MLeapSerializer):
    pipeline_serializer = PipelineSerializer()

    def __init__(self):
        super(TfidfVectorSerializer, self).__init__()

    @staticmethod
    def set_prediction_column(transformer, prediction_column):
        transformer.prediction_column = prediction_column

    @staticmethod
    def set_input_features(transformer, input_features):
        transformer.input_features = input_features

    def serialize_to_bundle(self, transformer, path, model_name):
        num_features = transformer.idf_.shape[0]
        vocabulary = [None] * num_features

        for term, termidx in transformer.vocabulary_.iteritems():
            vocabulary[termidx] = str(term)

        tf_attributes = [
            ('vocabulary', vocabulary),
            ('binary', transformer.binary),
            # no concept of 'min_tf' in scikit's TfidfVectorizer
            ('min_tf', None)
        ]
        tf_inputs = [{
            "name": transformer.input_features,
            "port": "input"
        }]
        tf_outputs = [{
            "name": "{}_count_vectorizer".format(transformer.name),
            "port": "output"
        }]
        tf_step = TfidfStep(transformer, 'count_vectorizer', tf_attributes, tf_inputs, tf_outputs)

        idf_attributes = [
            ('idf', transformer.idf_)
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