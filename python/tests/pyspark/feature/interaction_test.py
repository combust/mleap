import os
import shutil
import tempfile
import unittest

import mleap.pyspark  # noqa
from mleap.pyspark.spark_support import SimpleSparkSerializer  # noqa

import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

from mleap.pyspark.feature.interaction import Interaction
from tests.pyspark.lib.spark_session import spark_session


class InteractionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.input = self.spark.createDataFrame([
            (1, 1, 2, 3, 8),
            (2, 4, 3, 8, 7),
            (3, 6, 1, 9, 2),
            (4, 10, 8, 6, 9),
            (5, 9, 2, 7, 10),
            (6, 1, 1, 4, 2)
        ], ["f1", "f2", "f3", "f4", "f5"])

        self.expected_result = pd.DataFrame([
            (1, (1, 2), (3, 8), [3.0, 8.0, 6.0, 16.0]),
            (2, (4, 3), (8, 7), [64.0, 56.0, 48.0, 42.0]),
            (3, (6, 1), (9, 2), [162.0, 36.0, 27.0, 6.0]),
            (4, (10, 8), (6, 9), [240.0, 360.0, 192.0, 288.0]),
            (5, (9, 2), (7, 10), [315.0, 450.0, 70.0, 100.0]),
            (6, (1, 1), (4, 2), [24.0, 12.0, 24.0, 12.0]),
        ], columns=['f1', 'vec1', 'vec2', 'interactedCol'])

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    # def _new_add_math_binary(self):
        # return MathBinary(
            # operation=BinaryOperation.Add,
            # inputA="f1",
            # inputB="f2",
            # outputCol="add(f1, f2)",
        # )

    def test_vectors_interaction(self):

        assembler1 = VectorAssembler(inputCols=["f2", "f3"], outputCol="vec1")
        assembled1 = assembler1.transform(self.input)

        assembler2 = VectorAssembler(inputCols=["f4", "f5"], outputCol="vec2")
        assembled2 = assembler2.transform(assembled1).select("f1", "vec1", "vec2")

        interaction = Interaction(inputCols=["f1", "vec1", "vec2"], outputCol="interactedCol")
        result = interaction.transform(assembled2).toPandas()

        assert_frame_equal(self.expected_result, result)

    def test_interaction_pipeline(self):
        assembler1 = VectorAssembler(inputCols=["f2", "f3"], outputCol="vec1")
        assembler2 = VectorAssembler(inputCols=["f4", "f5"], outputCol="vec2")
        interaction = Interaction(inputCols=["f1", "vec1", "vec2"], outputCol="interactedCol")

        pipeline = Pipeline(
            stages=[assembler1, assembler2, interaction]
        )

        pipeline_model = pipeline.fit(self.input)
        result = pipeline_model.transform(
            self.input).toPandas()[['f1', 'vec1', 'vec2', 'interactedCol']]
        assert_frame_equal(self.expected_result, result)

    def test_serialize_deserialize_interaction(self):
        assembler1 = VectorAssembler(inputCols=["f2", "f3"], outputCol="vec1")
        assembled1 = assembler1.transform(self.input)

        assembler2 = VectorAssembler(inputCols=["f4", "f5"], outputCol="vec2")
        assembled2 = assembler2.transform(assembled1).select("f1", "vec1", "vec2")

        interaction = Interaction(inputCols=["f1", "vec1", "vec2"], outputCol="interactedCol")
        datasetForSerialize = interaction.transform(assembled2)

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'interaction.zip'))

        interaction.serializeToBundle(file_path, datasetForSerialize)
        deserialized_interaction = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = deserialized_interaction.transform(assembled2).toPandas()
        assert_frame_equal(self.expected_result, result)

    def test_serialize_deserialize_pipeline(self):
        assembler1 = VectorAssembler(inputCols=["f2", "f3"], outputCol="vec1")
        assembler2 = VectorAssembler(inputCols=["f4", "f5"], outputCol="vec2")
        interaction = Interaction(inputCols=["f1", "vec1", "vec2"], outputCol="interactedCol")

        pipeline = Pipeline(
            stages=[assembler1, assembler2, interaction]
        )

        pipeline_model = pipeline.fit(self.input)
        dataset_to_serialize = pipeline_model.transform(self.input)

        file_path = '{}{}'.format('jar:file:', os.path.join(self.tmp_dir, 'interaction.zip'))

        pipeline_model.serializeToBundle(file_path, dataset_to_serialize)
        deserialized_pipeline = SimpleSparkSerializer().deserializeFromBundle(file_path)

        result = deserialized_pipeline.transform(
            self.input
        ).toPandas()[['f1', 'vec1', 'vec2', 'interactedCol']]
        assert_frame_equal(self.expected_result, result)
