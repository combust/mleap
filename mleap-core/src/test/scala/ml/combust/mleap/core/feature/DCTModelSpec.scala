package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 4/12/17.
  */
class DCTModelSpec extends FunSpec {
  describe("dct model") {
    val model = DCTModel(false, 3)
    describe("issue167") {
      it("should not modify input features") {
        val expected = Array(123.4, 23.4, 56.7)
        val features = Vectors.dense(Array(123.4, 23.4, 56.7))
        val dctFeatures = model(features)

        assert(features.toArray.sameElements(expected))
        assert(!features.toArray.sameElements(dctFeatures.toArray))
        assert(features.toArray != dctFeatures.toArray)
      }
    }

    describe("input/output schema") {
      it("has the right input schema") {
        assert(model.inputSchema.fields == Seq(StructField("input", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(model.outputSchema.fields == Seq(StructField("output", TensorType.Double(3))))
      }
    }
  }
}
