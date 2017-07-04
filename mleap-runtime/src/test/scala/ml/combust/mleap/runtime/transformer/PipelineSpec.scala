package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class PipelineSpec extends FunSpec {

  describe("#getFields") {
    it("has inputs or outputs of its transformers") {
      val pipeline = new Pipeline(uid = "pipeline", Seq(
                      LinearRegression("transformer", "features", "prediction", LinearRegressionModel(Vectors.dense(1.0, 2.0, 3.0), 4.0))))
      assert(pipeline.getFields().get == Seq(
          StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)
      ))
    }
  }
}
