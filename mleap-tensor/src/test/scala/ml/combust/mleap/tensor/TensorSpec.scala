package ml.combust.mleap.tensor

import org.scalatest.FunSpec

class TensorSpec extends FunSpec {

  describe("DenseTensor") {
    it("should return false for dense tensors with different base") {
      val tensor1 = Tensor.denseVector(Array(20, 10, 5))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should return true for empty dense tensors") {
      val tensor1 = Tensor.denseVector(Array())
      val tensor2 = Tensor.denseVector(Array())
      assert(tensor1 == tensor2)
    }

    it("should return true for dense tensors with same elements and dimensions") {
      val tensor1 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 == tensor2)
    }

    it("should return false for dense tensors with different dimensions") {
      val tensor1 = Tensor.denseVector(Array(20.0, 10.0, 5.0, 23.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should return false for dense tensors with same dimension but different elements") {
      val tensor1 = Tensor.denseVector(Array(20.0, 11.0, 5.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should return true for dense tensors with dimension -1 and same elements") {
      val tensor1 = DenseTensor(Array(2.0, 5.0, 34.0), Seq(-1))
      val tensor2 = DenseTensor(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 == tensor2)
    }

    it("should return false for dense tensors with dimension -1 but different elements") {
      val tensor1 = DenseTensor(Array(2.0, 1.0, 34.0), Seq(-1))
      val tensor2 = DenseTensor(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 != tensor2)
    }

    it("should access the right element of tensor, even if first dimension is -1") {
      val tensor1 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(2,2))
      val tensor2 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(-1,2))
      assert(tensor1.get(1,1).get == 4.0)
      assert(tensor2.get(1,1).get == 4.0)
      assert(tensor1.get(3).get == 4.0)
      assert(tensor2.get(3).get == 4.0)
    }

    it("should yield length of tensor") {
      val tensor1 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(2,2))
      val tensor2 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(-1,2))
      assert(tensor1.size == 4)
      assert(tensor2.size == 4)
    }

    it("should yield length of raw vector") {
      val tensor1 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(2,2))
      val tensor2 = DenseTensor(Array(1.0, 2.0, 3.0, 4.0), Seq(-1,2))
      assert(tensor1.rawSize == 4)
      assert(tensor2.rawSize == 4)
    }

  }

  describe("SparseTensor") {
    it("should return false for sparse tensors with different base") {

      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1), Seq(2), Seq(4))))
      val tensor2 = Tensor.create(Array(22, 45, 99), Seq(5), Some(Seq(Seq(1), Seq(2), Seq(4))))
      assert(tensor1 != tensor2)
    }

    it("should return true for empty sparse tensors") {
      val tensor1 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      val tensor2 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      assert(tensor1 == tensor2)
    }

    it("should return true for sparse tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1), Seq(2), Seq(4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1), Seq(2), Seq(4))))
      assert(tensor1 == tensor2)
    }

    it("should return false for sparse tensors with different dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(4), Some(Seq(Seq(1), Seq(2), Seq(4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1), Seq(2), Seq(4))))
      assert(tensor1 != tensor2)
    }

    it("should return false for sparse tensors with same dimension but different elements") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(4), Some(Seq(Seq(1), Seq(2), Seq(4))))
      val tensor2 = Tensor.create(Array(22.3, 95.6, 99.3), Seq(4), Some(Seq(Seq(1), Seq(2), Seq(4))))
      assert(tensor1 != tensor2)
    }

    it("We should be able to access zero and non-zero elements of the tensor") {
      val tensor1 = Tensor.create(Array(15.0), Seq(4,4), Some(Seq(Seq(1,1))))
      assert(tensor1.get(1,1).get == 15.0)
      assert(tensor1.get(0,1).get == 0.0)
    }

    it("Should return none when we access a tensor with the wrong number of dimensions") {
      val tensor1 = Tensor.create(Array(15.0), Seq(4,4), Some(Seq(Seq(2,1))))
      assert(tensor1.get(1) == None)
      assert(tensor1.get(1,1,1) == None)
    }

    it("Should return none when accessing element out of bounds") {
      val tensor1 = Tensor.create(Array(15.0), Seq(4,4), Some(Seq(Seq(1, 1))))
      assert(tensor1.get(1,1).get == 15.0)
      assert(tensor1.get(1,2).get == 0.0)
      assert(tensor1.get(4,4) == None)
    }

    it("should yield length of tensor") {
      val tensor = Tensor.create(Array(15.0), Seq(4,4), Some(Seq(Seq(2,1))))
      assert(tensor.size == 16)
    }

    it("should yield length of raw vector") {
      val tensor = Tensor.create(Array(15.0), Seq(4,4), Some(Seq(Seq(2,1))))
      assert(tensor.rawSize == 1)
    }

  }
}
