package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 4/26/17.
  */
class InteractionModelSpec extends FunSpec {
  describe("with all numeric inputs") {

    val encoderSpec: Array[Array[Int]] = Array(Array(1), Array(1, 1))
    val model = InteractionModel(encoderSpec, Seq(ScalarShape(), TensorShape(2)))

    it("produces the expected interaction vector when using spark Vector") {
      val features = Seq(2.toDouble, Vectors.dense(3, 4))
      assert(model(features).toArray.toSeq == Seq(6, 8))
    }

    it("produces the expected interaction vector when using DenseTensor") {
      val features = Seq(2.toDouble, DenseTensor(Seq(3, 4).toArray, Seq(2)))
      assert(model(features).toArray.toSeq == Seq(6, 8))
    }

    it("produces the expected interaction vector when using SparseTensor") {
      val features = Seq(2.toDouble, SparseTensor(Seq(Seq(0), Seq(1)), Seq(3, 4).toArray, Seq(2)))
      assert(model(features).toArray.toSeq == Seq(6, 8))
    }

    it("has the right inputs") {
      assert(model.inputSchema.fields == Seq(StructField("input0", ScalarType.Double),
        StructField("input1", TensorType.Double(2))))
    }

    it("has the right outputs") {
      assert(model.outputSchema.fields == Seq(StructField("output", TensorType.Double(2))))
    }
  }

  describe("with one nominal input") {
    val encoderSpec: Array[Array[Int]] = Array(Array(4), Array(1, 1))
    val model = InteractionModel(encoderSpec, Seq(ScalarShape(), TensorShape(2)))

    it("produce the expected interaction vector when using spark Vector") {
      val features = Seq(2.toDouble, Vectors.dense(3, 4))

      assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 3, 4, 0, 0))
    }

    describe("produce the expected interaction vector when using Dense Tensor") {
      it("when the first feature is 2 and the second is (3,4)"){
        val features = Seq(2.toDouble, DenseTensor(Seq(3, 4).toArray, Seq(2)))
        assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 3, 4, 0, 0))
      }
      it("when the first feature is 3 and the second is (3,4)"){
        val features = Seq(3.toDouble, DenseTensor(Seq(3, 4).toArray, Seq(2)))
        assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 0, 0, 3, 4))
      }
    }

    describe("produce the expected interaction vector when using Sparse Tensor") {
      it("when the first feature is 2 and the second is (3, 4)"){
        val features = Seq(2.toDouble, SparseTensor(Seq(Seq(0), Seq(1)), Seq(3, 4).toArray, Seq(2)))
        assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 3, 4, 0, 0))
      }
      it("when the first feature is 3 and the second is (3, 4)"){
        val features = Seq(3.toDouble, SparseTensor(Seq(Seq(0), Seq(1)), Seq(3, 4).toArray, Seq(2)))
        assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 0, 0, 3, 4))
      }
    }

    it("has the right inputs") {
      assert(model.inputSchema.fields == Seq(StructField("input0", ScalarType.Double),
        StructField("input1", TensorType.Double(2))))
    }

    it("has the right outputs") {
      assert(model.outputSchema.fields == Seq(StructField("output", TensorType.Double(8))))
    }

  }
}
