package ml.combust.mleap.core.recommendation

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class ALSModelSpec extends FunSpec {

  describe("ALS") {
    val userFactors = Map(0 -> Vectors.dense(1, 2), 1 -> Vectors.dense(3, 1),
      2 -> Vectors.dense(2, 3))
    val itemFactors = Map(0 -> Vectors.dense(1, 2), 1 -> Vectors.dense(3, 1),
      2 -> Vectors.dense(2, 3), 3 -> Vectors.dense(1, 2))
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
