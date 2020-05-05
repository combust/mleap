import math
import os
import shutil
import tempfile
import unittest

import mleap.pyspark  # noqa
from mleap.pyspark.spark_support import SimpleSparkSerializer  # noqa

import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.ml import Pipeline
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

from mleap.pyspark.feature.math_binary import MathBinary
from mleap.pyspark.feature.math_binary import BinaryOperation
from tests.pyspark.lib.spark_session import spark_session


INPUT_SCHEMA = StructType([
    StructField('f1', FloatType()),
    StructField('f2', FloatType()),
])


class MathBinaryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.input = self.spark.createDataFrame([
            (
                float(i),
                float(i * 2),
            )
            for i in range(1, 10)
        ], INPUT_SCHEMA)

        self.expected_add = pd.DataFrame(
            [(
                float(i + i * 2)
            )
            for i in range(1, 10)],
            columns=['add(f1, f2)'],
        )

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def _new_add_math_binary(self):
        return MathBinary(
            operation=BinaryOperation.Add,
            inputA="f1",
            inputB="f2",
            outputCol="add(f1, f2)",
        )

    def test_add_math_binary(self):
        add_transformer = self._new_add_math_binary()
        result = add_transformer.transform(self.input).toPandas()[['add(f1, f2)']]
        assert_frame_equal(self.expected_add, result)

    def test_math_binary_pipeline(self):
        add_transformer = self._new_add_math_binary()

        mul_transformer = MathBinary(
            operation=BinaryOperation.Multiply,
            inputA="f1",
            inputB="add(f1, f2)",
            outputCol="mul(f1, add(f1, f2))",
        )

        expected = pd.DataFrame(
            [(
                float(i * (i + i * 2))
            )
            for i in range(1, 10)],
            columns=['mul(f1, add(f1, f2))'],
        )

        pipeline = Pipeline(
            stages=[add_transformer, mul_transformer]
        )

        pipeline_model = pipeline.fit(self.input)
        result = pipeline_model.transform(self.input).toPandas()[['mul(f1, add(f1, f2))']]
        assert_frame_equal(expected, result)

    def test_can_instantiate_all_math_binary(self):
        for binary_operation in BinaryOperation:
            transformer = MathBinary(
                operation=binary_operation,
                inputA="f1",
                inputB="f2",
                outputCol="operation",
            )

    def test_serialize_deserialize_math_binary(self):
        add_transformer = self._new_add_math_binary()

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'math_binary.zip'))

        add_transformer.serializeToBundle(file_path, self.input)
        deserialized_math_binary = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = deserialized_math_binary.transform(self.input).toPandas()[['add(f1, f2)']]
        assert_frame_equal(self.expected_add, result)

    def test_serialize_deserialize_pipeline(self):
        add_transformer = self._new_add_math_binary()

        mul_transformer = MathBinary(
            operation=BinaryOperation.Multiply,
            inputA="f1",
            inputB="add(f1, f2)",
            outputCol="mul(f1, add(f1, f2))",
        )

        expected = pd.DataFrame(
            [(
                float(i * (i + i * 2))
            )
            for i in range(1, 10)],
            columns=['mul(f1, add(f1, f2))'],
        )

        pipeline = Pipeline(
            stages=[add_transformer, mul_transformer]
        )

        pipeline_model = pipeline.fit(self.input)

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'math_binary_pipeline.zip'))

        pipeline_model.serializeToBundle(file_path, self.input)
        deserialized_pipeline = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = pipeline_model.transform(self.input).toPandas()[['mul(f1, add(f1, f2))']]
        assert_frame_equal(expected, result)
