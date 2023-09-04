package ml.combust.mleap.core.util

import ml.combust.mleap.tensor.{DenseTensor, SparseTensor}
import org.scalatest.funspec.AnyFunSpec

class VectorConvertersSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("mleapTensorToSparkVector works when") {

    it("using a Sparse Tensor") {
      val vec = Seq(1.0, 0, 3.0)
      val indices = Seq(0, 2).map(Seq(_))
      val values = indices.map((i: Seq[Int]) => vec(i.head)).toArray
      val tensor = SparseTensor(indices, values, Seq(vec.length))
      assert(VectorConverters.mleapTensorToSparkVector(tensor).toArray sameElements vec)
    }

    it("using a Sparse Tensor with non-increasing indices") {
      val vec = Seq(1.0, 0, 3.0)
      val indices = Seq(2, 0).map(Seq(_))
      val values = indices.map((i: Seq[Int]) => vec(i.head)).toArray
      val tensor = SparseTensor(indices, values, Seq(vec.length))
      assert(VectorConverters.mleapTensorToSparkVector(tensor).toArray sameElements vec)
    }

    it("using a Dense Tensor") {
      val vec = Seq(1.0, 2.0, 3.0).toArray
      val tensor = DenseTensor(vec, Seq(vec.length))
      assert(VectorConverters.mleapTensorToSparkVector(tensor).toArray sameElements vec)
    }
  }
}