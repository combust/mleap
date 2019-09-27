import unittest
from py4j.protocol import Py4JJavaError
from mleap.pyspark.feature.string_map import StringMap
from pyspark.sql import types as t
from tests.pyspark.lib.assertions import assert_df
from tests.pyspark.lib.spark_session import spark_session


INPUT_SCHEMA = t.StructType([t.StructField('key_col', t.StringType(), False),
                             t.StructField('extra_col', t.StringType(), False)])

OUTPUT_SCHEMA = t.StructType([t.StructField('key_col', t.StringType(), False),
                              t.StructField('extra_col', t.StringType(), False),
                              t.StructField('value_col', t.DoubleType(), False)])


class StringMapTest(unittest.TestCase):

    def setUp(self):
        self.spark = spark_session()
        self.input = self.spark.createDataFrame([['a', 'b']], INPUT_SCHEMA)

    def tearDown(self):
        self.spark.stop()

    def test_map(self):
        result = StringMap({'a': 1.0}, 'key_col', 'value_col').transform(self.input)
        expected = self.spark.createDataFrame([['a', 'b', 1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_default_value(self):
        result = StringMap({'z': 1.0}, 'key_col', 'value_col', handleInvalid='keep').transform(self.input)
        expected = self.spark.createDataFrame([['a', 'b', 0.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_custom_default_value(self):
        result = StringMap({'z': 1.0}, 'key_col', 'value_col', handleInvalid='keep', defaultValue=-1.0) \
            .transform(self.input)
        expected = self.spark.createDataFrame([['a', 'b', -1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    def test_map_missing_value_error(self):
        with self.assertRaises(Py4JJavaError) as error:
            StringMap({'z': 1.0}, 'key_col', 'value_col').transform(self.input).collect()
        self.assertIn('java.util.NoSuchElementException: Missing label: a', str(error.exception))

    def test_map_created_from_dataframe(self):
        labels_df = self.spark.createDataFrame([['a', 1.0]], 'key_col: string, value_col: double')
        result = StringMap(labels_df, 'key_col', 'value_col').transform(self.input)
        expected = self.spark.createDataFrame([['a', 'b', 1.0]], OUTPUT_SCHEMA)
        assert_df(expected, result)

    @staticmethod
    def test_validate_handleInvalid_ok():
        StringMap({}, handleInvalid='error')

    def test_validate_handleInvalid_bad(self):
        with self.assertRaises(AssertionError):
            StringMap(None, dict(), handleInvalid='invalid')

    def test_validate_labels_type_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(None, set())

    def test_validate_labels_key_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(None, {False: 0.0})

    def test_validate_labels_value_fails(self):
        with self.assertRaises(AssertionError):
            StringMap(None, {'valid_key_type': 'invalid_value_type'})
