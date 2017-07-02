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
  val binarizer = Binarizer(inputCol = "test_vec",
    outputCol = "test_binarizer",
    model = BinarizerModel(0.6, DoubleType(), TensorShape(3)))

  describe("with a double tensor input column") {
    describe("#transform") {
      it("thresholds the input column to 0 or 1") {
        val schema = StructType(Seq(StructField("test_vec", TensorType(DoubleType())))).get
        val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.1, 0.6, 0.7)))))
        val frame = LeapFrame(schema, dataset)

        val frame2 = binarizer.transform(frame).get
        val data = frame2.dataset(0).getTensor[Double](1)

        assert(data(0) == 0.0)
        assert(data(1) == 0.0)
        assert(data(2) == 1.0)
      }
    }

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(binarizer.getFields().get ==
          Seq(StructField("test_vec", TensorType(DoubleType(), Some(Seq(3)))),
            StructField("test_binarizer", TensorType(DoubleType(), Some(Seq(3))))))
      }
    }
  }

  describe("with a double input column") {
    val binarizer2 = binarizer.copy(inputCol = "test", model = binarizer.model.copy(inputShape = ScalarShape()))
    describe("#transform") {
      it("thresholds the input column to 0 or 1") {
        val schema = StructType(Seq(StructField("test", DoubleType()))).get
        val dataset = LocalDataset(Seq(Row(0.7), Row(0.1)))
        val frame = LeapFrame(schema, dataset)

        val frame2 = binarizer2.transform(frame).get
        val data = frame2.dataset

        assert(data(0).getDouble(1) == 1.0)
        assert(data(1).getDouble(1) == 0.0)
      }
    }

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(binarizer2.getFields().get ==
          Seq(StructField("test", DoubleType()),
            StructField("test_binarizer", DoubleType())))
      }
    }
  }
}
