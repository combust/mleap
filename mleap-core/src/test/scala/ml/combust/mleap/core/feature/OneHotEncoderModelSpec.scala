package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class OneHotEncoderModelSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("encoder model") {

    val encoder = OneHotEncoderModel(Array(5))

    it("encodes the value as a vector") {
      assert(encoder(Array(1.0)).head.toArray.sameElements(Array(0.0, 1.0, 0.0, 0.0)))
      assert(encoder(Array(3.0)).head.toArray.sameElements(Array(0.0, 0.0, 0.0, 1.0)))
    }

    it("has the right input schema") {
      assert(encoder.inputSchema.fields == Seq(StructField("input0", ScalarType.Double.nonNullable)))
    }

    it("has the right output schema") {
      assert(encoder.outputSchema.fields == Seq(StructField("output0", TensorType.Double(5))))
    }
  }
}
