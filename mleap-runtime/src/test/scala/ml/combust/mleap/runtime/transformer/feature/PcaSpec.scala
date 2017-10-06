package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PcaModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/12/16.
  */
class PcaSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(2.0, 1.0, 0.0))))
  val frame = DefaultLeapFrame(schema, dataset)

  val pc = new DenseMatrix(3, 2, Array(1d, -1, 2,
    0, -3, 1))
  val input = Vectors.dense(Array(2d, 1, 0))
  val pca = Pca(
    shape = NodeShape.feature(inputCol = "test_vec", outputCol = "test_pca"),
    model = PcaModel(pc))

  describe("#transform") {
    it("extracts the principal components from the input column") {
      val frame2 = pca.transform(frame).get
      val data = frame2.dataset(0).getTensor[Double](1).toArray

      assert(data sameElements Array[Double](1, -3))
    }

    describe("with invalid input column") {
      val pca2 = pca.copy(shape = NodeShape.feature(inputCol = "bad_input"))

      it("returns a Failure") { assert(pca2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(pca.schema.fields ==
        Seq(StructField("test_vec", TensorType.Double()),
          StructField("test_pca", TensorType.Double())))
    }
  }
}
