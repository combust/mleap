package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
  val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.0, 20.0, 40.0)))))
  val frame = LeapFrame(schema, dataset)

  val normalizer = Normalizer(
    shape = NodeShape.feature(inputCol = "test_vec", outputCol = "test_norm"),
    model = NormalizerModel(20.0, 3))

  describe("#transform") {
    it("normalizes the input column") {
      val frame2 = normalizer.transform(frame).get
      val data = frame2.dataset.toArray
      val norm = data(0).getTensor[Double](1)

      assert(norm(0) < 0.0001 && norm(0) > -0.0001)
      assert(norm(1) < 0.5001 && norm(1) > 0.49999)
      assert(norm(2) < 1.0001 && norm(2) > 0.99999)
    }

    describe("with invalid input column") {
      val normalizer2 = normalizer.copy(shape = NodeShape.feature(inputCol = "bad_input"))

      it("returns a Failure") { assert(normalizer2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(normalizer.schema.fields ==
        Seq(StructField("test_vec", TensorType.Double(3)),
          StructField("test_norm", TensorType.Double(3))))
    }
  }
}
