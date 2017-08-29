package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerModelSpec extends FunSpec {
  describe("normalizer model") {

    val normalizer = NormalizerModel(20.0, 3)

    it("normalizes the feature vector using the p normalization value") {
      val features = Vectors.dense(Array(0.0, 20.0, 40.0))
      val norm = normalizer(features).toArray

      assert(norm(0) < 0.0001 && norm(0) > -0.0001)
      assert(norm(1) < 0.5001 && norm(1) > 0.49999)
      assert(norm(2) < 1.0001 && norm(2) > 0.99999)
    }

    it("has the right input schema") {
      assert(normalizer.inputSchema.fields == Seq(StructField("input", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(normalizer.outputSchema.fields == Seq(StructField("output", TensorType.Double(3))))
    }
  }
}
