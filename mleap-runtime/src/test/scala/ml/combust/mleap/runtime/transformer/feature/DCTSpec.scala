package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.DCTModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DCTSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = DCT(shape = NodeShape.vector(3, 3),
        model = DCTModel(false))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double()),
          StructField("output", TensorType.Double())))
    }
  }
}