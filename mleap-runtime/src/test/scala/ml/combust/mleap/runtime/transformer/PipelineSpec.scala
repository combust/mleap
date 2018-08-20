package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class PipelineSpec extends FunSpec {

  describe("input/output schema") {
    it("has inputs or outputs of its transformers") {
      val vectorAssembler = VectorAssembler(
        shape = NodeShape().withInput("input0", "feature1").
          withInput("input1", "feature2").
          withInput("input2", "feature3").
          withStandardOutput("features"),
        model = VectorAssemblerModel(Seq(ScalarShape(), ScalarShape(), ScalarShape())))

      val regression = LinearRegression(shape = NodeShape().withInput("features", "features").
        withOutput("prediction", "prediction"),
        model = LinearRegressionModel(Vectors.dense(1.0, 2.0, 3.0), 4.0))

      val pipeline = Pipeline(uid = "root_pipeline", shape = NodeShape(), PipelineModel(Seq(
          Pipeline(uid = "child_pipeline_1", shape = NodeShape(), PipelineModel(Seq(vectorAssembler))),
        Pipeline(uid = "child_pipeline_2", shape = NodeShape(), PipelineModel(Seq(regression))))))

      assert(pipeline.schema.fields == Seq(
          StructField("feature1", ScalarType.Double),
          StructField("feature2", ScalarType.Double),
          StructField("feature3", ScalarType.Double),
          StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))

      assert(pipeline.inputSchema.fields == Seq(
        StructField("feature1", ScalarType.Double),
        StructField("feature2", ScalarType.Double),
        StructField("feature3", ScalarType.Double)))

      assert(pipeline.outputSchema.fields == Seq(
        StructField("features", TensorType.Double(3)),
        StructField("prediction", ScalarType.Double.nonNullable)))

      assert(pipeline.strictOutputSchema.fields == Seq(
        StructField("prediction", ScalarType.Double.nonNullable)))

      assert(pipeline.intermediateSchema.fields == Seq(
        StructField("features", TensorType.Double(3))))
    }
  }
}
