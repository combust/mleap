package ml.combust.mleap.core.clustering

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.FunSpec

class GaussianMixtureModelSpec extends FunSpec {

  describe("gaussian mixture model") {
    val model = new GaussianMixtureModel(Array(null, null, null), Array(1, 2, 3))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Int.nonNullable),
          StructField("probability", TensorType.Double(3))))
    }
  }
}
