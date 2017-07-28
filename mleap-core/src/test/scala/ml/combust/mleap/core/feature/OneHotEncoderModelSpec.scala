package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class OneHotEncoderModelSpec extends FunSpec {
  describe("encoder model") {

    val encoder = OneHotEncoderModel(5)

    it("encodes the value as a vector") {

      assert(encoder(1.0).toArray.sameElements(Array(0.0, 1.0, 0.0, 0.0, 0.0)))
      assert(encoder(3.0).toArray.sameElements(Array(0.0, 0.0, 0.0, 1.0, 0.0)))
    }

    it("has the right input schema") {
      assert(encoder.inputSchema.fields == Seq(StructField("input", ScalarType.Double)))
    }

    it("has the right output schema") {
      assert(encoder.outputSchema.fields == Seq(StructField("output", TensorType.Double())))
    }
  }
}
