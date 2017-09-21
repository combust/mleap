package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class PipelineSpec extends FunSpec {

  describe("input/output schema") {
    it("has inputs or outputs of its transformers") {
      val pipeline = Pipeline(uid = "pipeline", shape = NodeShape(), PipelineModel(Seq(
                      LinearRegression(shape = NodeShape().withInput("features", "features").
                                              withOutput("prediction", "prediction"),
                        model = LinearRegressionModel(Vectors.dense(1.0, 2.0, 3.0), 4.0)))))
      assert(pipeline.schema.fields == Seq(
          StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)
      ))
    }
  }
}
