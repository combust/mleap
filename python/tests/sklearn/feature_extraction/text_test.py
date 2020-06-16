import json
import os
import shutil
import tempfile
import unittest
import uuid

from mleap.sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


class TransformerTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

        self.docs = ['test']

        self.tfidf = TfidfVectorizer(binary=False)
        self.tfidf.mlinit(input_features='some_text', prediction_column='features')
        self.tfidf.fit(self.docs)

        self.assertEqual(self.tfidf.vocabulary_, {u'test': 0})
        self.assertEqual(self.tfidf.idf_, [1.0])

        self.tfidf.serialize_to_bundle(self.tmp_dir, self.tfidf.name)
        self.pipe_model = self.load_pipeline_model()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_countvectorizer_serializer(self):
        count = CountVectorizer()
        count.mlinit(input_features='input', prediction_column='pred')
        count.fit(self.docs)
        count.serialize_to_bundle(self.tmp_dir, count.name)

        with open('{}/{}.node/node.json'.format(self.tmp_dir, count.name)) as node_json:
            node = json.load(node_json)

        with open('{}/{}.node/model.json'.format(self.tmp_dir, count.name)) as model_json:
            model = json.load(model_json)

        self.assertEqual(node['shape']['inputs'][0]['name'], 'input')
        self.assertEqual(node['shape']['outputs'][0]['name'], 'pred')
        self.assertEqual(model['op'], 'tokenizer')

    def test_tfidf_vectorizer_serializer_pipeline_part(self):
        self.assertEqual(self.pipe_model['op'], 'pipeline')

    def test_tfidf_vectorizer_serializer_tf_part(self):
        nodes = self.pipe_model['attributes']['nodes']['string']
        tf_node_name = nodes[0]

        with open('{}/{}/root/{}.node/model.json'.format(self.tmp_dir, self.tfidf.name, tf_node_name)) as tf_model_json:
            tf_model = json.load(tf_model_json)

        self.assertEqual(tf_model['attributes']['binary']['boolean'], False)
        self.assertEqual(tf_model['attributes']['vocabulary']['string'], self.docs)
        self.assertEqual(tf_model['op'], 'count_vectorizer')

        with open('{}/{}/root/{}.node/node.json'.format(self.tmp_dir, self.tfidf.name, tf_node_name)) as tf_node_json:
            tf_node = json.load(tf_node_json)

        self.assertEqual(tf_node['shape']['inputs'][0]['name'], 'some_text')
        self.assertEqual(tf_node['shape']['outputs'][0]['name'], 'token_counts')

    def test_tfidf_vectorizer_serializer_idf_part(self):
        nodes = self.pipe_model['attributes']['nodes']['string']
        idf_node_name = nodes[1]

        with open('{}/{}/root/{}.node/node.json'.format(self.tmp_dir, self.tfidf.name, idf_node_name)) as idf_node_json:
            idf_node = json.load(idf_node_json)

        self.assertEqual(idf_node['shape']['inputs'][0]['name'], 'token_counts')
        self.assertEqual(idf_node['shape']['outputs'][0]['name'], 'features')

        idf_model_file = '{}/{}/root/{}.node/model.json'.format(self.tmp_dir, self.tfidf.name, idf_node_name)
        with open(idf_model_file) as idf_model_json:
            idf_model = json.load(idf_model_json)

        idf_shape = {
            'dimensions': [
                {
                    'name': '',
                    'size': 1
                }
            ]
        }

        self.assertEqual(idf_model['attributes']['idf']['double'], [1.0])
        self.assertEqual(idf_model['attributes']['idf']['shape'], idf_shape)
        self.assertEqual(idf_model['attributes']['idf']['type'], 'tensor')
        self.assertEqual(idf_model['op'], 'idf')

    def load_pipeline_model(self):
        with open('{}/{}/root/model.json'.format(self.tmp_dir, self.tfidf.name)) as pipeline_model_json:
            return json.load(pipeline_model_json)
