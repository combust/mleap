package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.DCTModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DCTSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = DCT(shape = NodeShape.vector(3, 3),
        model = DCTModel(false, 3))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(3))))
    }
  }
}