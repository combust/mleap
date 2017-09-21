package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/30/16.
  */
class RowTransformBuilderSpec extends FunSpec {
  val schema = StructType(Seq(StructField("feature1", ScalarType.Double),
    StructField("feature2", ScalarType.Double),
    StructField("feature3", ScalarType.Double))).get
  val assembler = VectorAssembler(shape = NodeShape().withInput("input0", "feature1").
        withInput("input1", "feature2").
        withInput("input2", "feature3").
      withStandardOutput("features"),
    model = VectorAssemblerModel(Seq(ScalarShape(), ScalarShape(), ScalarShape())))
  val linearRegression = LinearRegression(shape = NodeShape().withInput("features", "features").
      withOutput("prediction", "prediction"),
    model = LinearRegressionModel(coefficients = Vectors.dense(Array(1.0, 0.5, 5.0)),
      intercept = 73.0))
  val pipeline = Pipeline(shape = NodeShape(), model = PipelineModel(Seq(assembler, linearRegression)))

  describe("building a transformer pipeline") {
    it("transforms single rows at a time") {
      val builder = RowTransformBuilder(schema)
      val transformer = pipeline.transform(builder).get
      val row1 = transformer.transform(Row(20.0, 10.0, 5.0))
      val row2 = transformer.transform(Row(5.0, 17.0, 9.0))

      assert(row1.toArray sameElements Array(20.0, 10.0, 5.0, Tensor.denseVector(Array(20.0, 10.0, 5.0)), 123.0))
      assert(row2.toArray sameElements Array(5.0, 17.0, 9.0, Tensor.denseVector(Array(5.0, 17.0, 9.0)), 131.5))
    }
  }
}
