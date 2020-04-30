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

from mleap.pyspark.feature.math_unary import MathUnary
from mleap.pyspark.feature.math_unary import UnaryOperation
from tests.pyspark.lib.spark_session import spark_session


INPUT_SCHEMA = StructType([
    StructField('f1', FloatType()),
])


class MathUnaryTest(unittest.TestCase):

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
            )
            for i in range(1, 10)
        ], INPUT_SCHEMA)

        self.expected_sin = pd.DataFrame(
            [(
                math.sin(i),
            )
            for i in range(1, 10)],
            columns=['sin(f1)'],
        )
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_sin_math_unary(self):
        sin_transformer = MathUnary(
            operation=UnaryOperation.Sin,
            inputCol="f1",
            outputCol="sin(f1)",
        )

        result = sin_transformer.transform(self.input).toPandas()[['sin(f1)']]
        assert_frame_equal(self.expected_sin, result)

    def test_math_unary_pipeline(self):
        sin_transformer = MathUnary(
            operation=UnaryOperation.Sin,
            inputCol="f1",
            outputCol="sin(f1)",
        )

        exp_transformer = MathUnary(
            operation=UnaryOperation.Exp,
            inputCol="sin(f1)",
            outputCol="exp(sin(f1))",
        )

        expected = pd.DataFrame(
            [(
                math.exp(math.sin(i)),
            )
            for i in range(1, 10)],
            columns=['exp(sin(f1))'],
        )

        pipeline = Pipeline(
            stages=[sin_transformer, exp_transformer]
        )

        pipeline_model = pipeline.fit(self.input)
        result = pipeline_model.transform(self.input).toPandas()[['exp(sin(f1))']]
        assert_frame_equal(expected, result)

    def test_can_instantiate_all_math_unary(self):
        for unary_operation in UnaryOperation:
            transformer = MathUnary(
                operation=unary_operation,
                inputCol="f1",
                outputCol="operation",
            )

    def test_serialize_deserialize_math_unary(self):
        sin_transformer = MathUnary(
            operation=UnaryOperation.Sin,
            inputCol="f1",
            outputCol="sin(f1)",
        )

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'math_unary.zip'))

        sin_transformer.serializeToBundle(file_path, self.input)
        deserialized_math_unary = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = deserialized_math_unary.transform(self.input).toPandas()[['sin(f1)']]
        assert_frame_equal(self.expected_sin, result)

    def test_serialize_deserialize_pipeline(self):
        sin_transformer = MathUnary(
            operation=UnaryOperation.Sin,
            inputCol="f1",
            outputCol="sin(f1)",
        )

        exp_transformer = MathUnary(
            operation=UnaryOperation.Exp,
            inputCol="sin(f1)",
            outputCol="exp(sin(f1))",
        )

        expected = pd.DataFrame(
            [(
                math.exp(math.sin(i)),
            )
            for i in range(1, 10)],
            columns=['exp(sin(f1))'],
        )

        pipeline = Pipeline(
            stages=[sin_transformer, exp_transformer]
        )

        pipeline_model = pipeline.fit(self.input)

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'math_unary_pipeline.zip'))

        pipeline_model.serializeToBundle(file_path, self.input)
        deserialized_pipeline = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = pipeline_model.transform(self.input).toPandas()[['exp(sin(f1))']]
        assert_frame_equal(expected, result)

