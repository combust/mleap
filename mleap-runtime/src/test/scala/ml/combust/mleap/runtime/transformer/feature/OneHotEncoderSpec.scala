package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneHotEncoderSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = OneHotEncoder(shape = NodeShape().withStandardInput("input").
              withStandardOutput("output"), model = OneHotEncoderModel(5))
      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.Double),
          StructField("output", TensorType.Double())))
    }
  }
}