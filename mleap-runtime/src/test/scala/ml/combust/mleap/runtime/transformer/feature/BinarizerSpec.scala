package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerSpec extends FunSpec {
  val binarizer = Binarizer(shape = NodeShape.vector(3, 3,
    inputCol = "test_vec",
    outputCol = "test_binarizer"),
    model = BinarizerModel(0.6, TensorShape(3)))

  describe("with a double tensor input column") {
    describe("#transform") {
      it("thresholds the input column to 0 or 1") {
        val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
        val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.1, 0.6, 0.7)))))
        val frame = LeapFrame(schema, dataset)

        val frame2 = binarizer.transform(frame).get
        val data = frame2.dataset(0).getTensor[Double](1)

        assert(data(0) == 0.0)
        assert(data(1) == 0.0)
        assert(data(2) == 1.0)
      }
    }

    describe("input/output schema") {
      it("has the correct inputs and outputs") {
        assert(binarizer.schema.fields ==
          Seq(StructField("test_vec", TensorType(BasicType.Double, Seq(3))),
            StructField("test_binarizer", TensorType(BasicType.Double, Seq(3)))))
      }
    }
  }

  describe("with a double input column") {
    val binarizer2 = binarizer.copy(shape = NodeShape.scalar(
      inputCol = "test",
      outputCol = "test_binarizer"), model = binarizer.model.copy(inputShape = ScalarShape()))
    describe("#transform") {
      it("thresholds the input column to 0 or 1") {
        val schema = StructType(Seq(StructField("test", ScalarType.Double))).get
        val dataset = LocalDataset(Seq(Row(0.7), Row(0.1)))
        val frame = LeapFrame(schema, dataset)

        val frame2 = binarizer2.transform(frame).get
        val data = frame2.dataset

        assert(data(0).getDouble(1) == 1.0)
        assert(data(1).getDouble(1) == 0.0)
      }
    }

    describe("input/output schema") {
      it("has the correct inputs and outputs") {
        assert(binarizer2.schema.fields ==
          Seq(StructField("test", ScalarType.Double),
            StructField("test_binarizer", ScalarType.Double)))
      }
    }
  }
}
