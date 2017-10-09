package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class BucketedRandomProjectionLSHSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = BucketedRandomProjectionLSH(shape = NodeShape.feature(),
        model = new BucketedRandomProjectionLSHModel(Seq(), 5, 3))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType(BasicType.Double, Seq(3))),
          StructField("output", TensorType(BasicType.Double, Seq(3, 1)))))
    }
  }
}