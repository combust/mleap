package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class PipelineSpec extends FunSpec {

  describe("#getFields") {
    it("has inputs or outputs of its transformers") {
      val pipeline = new Pipeline(uid = "pipeline", Seq(
                      LinearRegression("transformer", "features", "prediction", null)))
      assert(pipeline.getFields().get == Seq(
          StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())
      ))
    }
  }
}
