package ml.combust.mleap.core.recommendation

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class ALSModelSpec extends FunSpec {

  describe("ALS") {
    val userFactors = Map(0 -> Array(1.0f, 2.0f), 1 -> Array(3.0f, 1.0f), 2 -> Array(2.0f, 3.0f))
    val itemFactors = Map(0 -> Array(1.0f, 2.0f), 1 -> Array(3.0f, 1.0f), 2 -> Array(2.0f, 3.0f), 3 -> Array(1.0f, 2.0f))
    val aLSModel = ALSModel(2, userFactors, itemFactors)

    describe("predict") {
      it("applies the ALS function for prediction") {
        assert(aLSModel(0, 1) == 5.0)
      }
    }

    describe("input/output schema"){
      it("has the right input schema") {
        assert(aLSModel.inputSchema.fields == Seq(StructField("user", ScalarType.Int.nonNullable),
          StructField("item", ScalarType.Int.nonNullable)))
      }

      it("has the right output schema") {
        assert(aLSModel.outputSchema.fields ==
          Seq(StructField("prediction", ScalarType.Float.nonNullable)))
      }
    }
  }
}
