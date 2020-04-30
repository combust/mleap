import os
import tempfile
import unittest

from py4j.protocol import Py4JJavaError
from pyspark.ml import Pipeline
from pyspark.sql import types as t

from mleap.pyspark.feature.string_map import StringMap
from mleap.pyspark.spark_support import SimpleSparkSerializer
from tests.pyspark.lib.assertions import assert_df
from tests.pyspark.lib.spark_session import spark_session


INPUT_SCHEMA = t.StructType([t.StructField('key_col', t.StringType(), False),
                             t.StructField('extra_col', t.StringType(), False)])

OUTPUT_SCHEMA = t.StructType([t.StructField('key_col', t.StringType(), False),
                              t.StructField('extra_col', t.StringType(), False),
                              t.StructField('value_col', t.DoubleType(), False)])


class StringMapTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.input = StringMapTest.spark.createDataFrame([['a', 'b']], INPUT_SCHEMA)

    def test_map(self):
        result = StringMap(
            labels={'a': 1.0},
            inputCol='key_col',
            outputCol='value_col',
        ).transform(self.input)
        expected = StringMapTest.spark.createDataFrame([['a', 'b', 1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_default_value(self):
        result = StringMap(
            labels={'z': 1.0},
            inputCol='key_col',
            outputCol='value_col',
            handleInvalid='keep',
        ).transform(self.input)
        expected = StringMapTest.spark.createDataFrame([['a', 'b', 0.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_custom_default_value(self):
        result = StringMap(
            labels={'z': 1.0},
            inputCol='key_col',
            outputCol='value_col',
            handleInvalid='keep',
            defaultValue=-1.0
        ).transform(self.input)
        expected = StringMapTest.spark.createDataFrame([['a', 'b', -1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_missing_value_error(self):
        with self.assertRaises(Py4JJavaError) as error:
            StringMap(
                labels={'z': 1.0},
                inputCol='key_col',
                outputCol='value_col'
            ).transform(self.input).collect()
        self.assertIn('java.util.NoSuchElementException: Missing label: a', str(error.exception))

    def test_map_from_dataframe(self):
        labels_df = StringMapTest.spark.createDataFrame([['a', 1.0]], 'key_col: string, value_col: double')
        result = StringMap.from_dataframe(
            labels_df=labels_df,
            inputCol='key_col',
            outputCol='value_col'
        ).transform(self.input)
        expected = StringMapTest.spark.createDataFrame([['a', 'b', 1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_serialize_to_bundle(self):
        string_map = StringMap(
            labels={'a': 1.0},
            inputCol='key_col',
            outputCol='value_col',
        )
        pipeline = Pipeline(stages=[string_map]).fit(self.input)
        serialization_dataset = pipeline.transform(self.input)

        jar_file_path = _serialize_to_file(pipeline, serialization_dataset)
        deserialized_pipeline = _deserialize_from_file(jar_file_path)

        result = deserialized_pipeline.transform(self.input)
        expected = StringMapTest.spark.createDataFrame([['a', 'b', 1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    @staticmethod
    def test_validate_handleInvalid_ok():
        StringMap(labels={}, handleInvalid='error')

    def test_validate_handleInvalid_bad(self):
        with self.assertRaises(AssertionError):
            StringMap(labels=None, inputCol=dict(), outputCol=None, handleInvalid='invalid')

    def test_validate_labels_type_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(labels=None, inputCol=set(), outputCol=None)

    def test_validate_labels_key_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(labels=None, inputCol={False: 0.0}, outputCol=None)

    def test_validate_labels_value_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(labels=None, inputCol={'valid_key_type': 'invalid_value_type'}, outputCol=None)


def _serialize_to_file(model, df_for_serializing):
    jar_file_path = _to_jar_file_path(
        os.path.join(tempfile.mkdtemp(), 'test_serialize_to_bundle-pipeline.zip'))
    SimpleSparkSerializer().serializeToBundle(model, jar_file_path, df_for_serializing)
    return jar_file_path


def _to_jar_file_path(path):
    return "jar:file:" + path


def _deserialize_from_file(path):
    return SimpleSparkSerializer().deserializeFromBundle(path)
