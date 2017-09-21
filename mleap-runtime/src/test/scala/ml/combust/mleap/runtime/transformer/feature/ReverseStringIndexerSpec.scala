package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class ReverseStringIndexerSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = ReverseStringIndexer(shape = NodeShape.scalar(
        inputBase = BasicType.Double,
        outputBase = BasicType.String
      ), model = new ReverseStringIndexerModel(Seq("one", "two", "three")))

      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.Double.nonNullable),
          StructField("output", ScalarType.String)))
    }
  }
}