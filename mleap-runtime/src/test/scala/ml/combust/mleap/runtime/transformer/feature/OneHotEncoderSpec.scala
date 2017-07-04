package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneHotEncoderSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = OneHotEncoder(shape = NodeShape().withStandardInput("input", ScalarType.Double).
        withStandardOutput("output", TensorType(BasicType.Double, Seq(4))), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.Double),
          StructField("output", TensorType(BasicType.Double, Seq(4)))))
    }
  }
}