package ml.combust.mleap.tensor

import org.scalatest.FunSpec

class TensorSpec extends FunSpec {
  def toIndices(dimensions: Seq[Int]): Seq[Seq[Int]] =  combine(dimensions.map(d => 0 until d))

  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }

  describe("DenseVector") {
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
      val tensor1 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      val tensor2 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 == tensor2)
    }

    it("should return false for dense tensors with dimension -1 but different elements") {
      val tensor1 = Tensor.create(Array(2.0, 1.0, 34.0), Seq(-1))
      val tensor2 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 != tensor2)
    }
  }

  describe("DenseTensor") {
    val random = new scala.util.Random
    val shape = Seq(5,4,3,2,1)
    val dims = shape.length
    val size = shape.product
    val floatArray = Array.fill(size){random.nextFloat}

    it("should return true for dense tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray, dimensions = shape)
      assert(tensor1 == tensor2)
    }

    it("should return false for dense tensors with different dimensions") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray, dimensions = shape.slice(0,dims -1))
      assert(tensor1 != tensor2)
    }

    it("should return false for dense tensors with same dimension but different elements") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray.map(x=>x*2), dimensions = shape)
      assert(tensor1 != tensor2)
    }

    it("should return true for dense tensors with dimension -1 and same elements") {
      val tensor1 = Tensor.create(floatArray, Seq(-1) ++ shape.slice(1, 5))
      val tensor2 = Tensor.create(floatArray, shape.slice(0,4) ++ Seq(-1))
      assert(tensor1 == tensor2)
    }

    it("should return true for dense tensors each elements compare to original array") {
      val tensor = Tensor.create(floatArray, dimensions = shape)
      println(floatArray.mkString(" "))
      println(toIndices(shape).map(indices => tensor(indices: _*) ).mkString(" "))
      assert( floatArray sameElements toIndices(shape).map(indices => tensor(indices: _*) ) )
    }
  }

  describe("SparseVector") {
    it("should return false for sparse tensors with different base") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22, 45, 99), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 != tensor2)
    }

    it("should return true for empty sparse tensors") {
      val tensor1 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      val tensor2 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      assert(tensor1 == tensor2)
    }

    it("should return true for sparse tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 == tensor2)
    }

    it("should return false for sparse tensors with different dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(4), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 != tensor2)
    }

    it("should return false for sparse tensors with same dimension but different elements") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(4), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22.3, 95.6, 99.3), Seq(4), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 != tensor2)
    }

    it("should raise exception for sparse tensors with dimension -1") {
      assertThrows[IllegalArgumentException] {
        Tensor.create(Array(22.3, 45.6, 99.3), Seq(-1), Some(Seq(Seq(1, 2, 4))))
      }
    }

  }

  describe("SparseTensor") {
    val random = new scala.util.Random
    val indices = Seq(Seq(0,0,0), Seq(1, 1, 0), Seq(2, 1, 0))
    val array = Array.fill[Float](indices.length){random.nextFloat()}
    val shape = Seq(5,4,3)
    val otherShape = Seq(3,2,1)

    it("should return true for empty sparse tensors") {
      val tensor1 = Tensor.create(Array(), shape, Some(Seq(Seq())))
      val tensor2 = Tensor.create(Array(), shape, Some(Seq(Seq())))
      assert(tensor1 == tensor2)
    }

    it("should return true for sparse tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(array, shape, Some(indices))
      val tensor2 = Tensor.create(array, shape, Some(indices))
      assert(tensor1 == tensor2)
    }

    it("should return false for sparse tensors with different dimensions") {
      val tensor1 = Tensor.create(array, shape, Some(indices))
      val tensor2 = Tensor.create(array, otherShape, Some(indices))
      assert(tensor1 != tensor2)
    }

    it("should return false for sparse tensors with same dimension but different elements") {
      val tensor1 = Tensor.create(array, shape, Some(indices))
      val tensor2 = Tensor.create(array.map(x=> x*2), shape, Some(indices))
      assert(tensor1 != tensor2)
    }

    it("should raise exception for sparse tensors with dimension -1") {
      assertThrows[IllegalArgumentException] {
        Tensor.create(array, Seq(-1), Some(indices))
      }
    }

    it("should return true for sparse tensors each elements compare to original array") {
      val tensor = Tensor.create(array, shape, Some(indices))
      println(array.mkString(" "))
      println(indices.map(i => tensor(i: _*) ).mkString(" "))
      assert( array sameElements indices.map(i=> tensor(i: _*) ) )
    }
    it("should return true for sparse tensors each elements compare to original array after converted to dense") {
      val tensor = Tensor.create(array, shape, Some(indices)).toDense
      println(array.mkString(" "))
      println(indices.map(i => tensor(i: _*) ).mkString(" "))
      assert( array sameElements indices.map(i=> tensor(i: _*) ) )
    }

  }
}
