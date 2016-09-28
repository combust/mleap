package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Array(Row(Vectors.dense(Array(0.0, 20.0, 40.0)))))
  val frame = LeapFrame(schema, dataset)

  val normalizer = Normalizer(inputCol = "test_vec",
    outputCol = "test_norm",
    model = NormalizerModel(20.0))

  describe("#transform") {
    it("normalizes the input column") {
      val frame2 = normalizer.transform(frame).get
      val data = frame2.dataset.toArray
      val norm = data(0).getVector(1)

      assert(norm(0) < 0.0001 && norm(0) > -0.0001)
      assert(norm(1) < 0.5001 && norm(1) > 0.49999)
      assert(norm(2) < 1.0001 && norm(2) > 0.99999)
    }

    describe("with invalid input column") {
      val normalizer2 = normalizer.copy(inputCol = "bad_input")

      it("returns a Failure") { assert(normalizer2.transform(frame).isFailure) }
    }
  }
}
