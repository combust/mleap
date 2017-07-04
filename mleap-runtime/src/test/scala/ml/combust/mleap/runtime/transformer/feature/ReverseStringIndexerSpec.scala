package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class ReverseStringIndexerSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = ReverseStringIndexer(shape = NodeShape.scalar(
        inputBase = BasicType.Double,
        outputBase = BasicType.String
      ), model = null)

      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.Double),
          StructField("output", ScalarType.String)))
    }
  }
}