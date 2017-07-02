package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class ReverseStringIndexerSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = ReverseStringIndexer("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", ScalarType.Double),
          StructField("output", ScalarType.String)))
    }
  }
}