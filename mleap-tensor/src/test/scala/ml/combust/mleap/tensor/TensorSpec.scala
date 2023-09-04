package ml.combust.mleap.tensor

class TensorSpec extends org.scalatest.funspec.AnyFunSpec {
  def toIndices(dimensions: Seq[Int]): Seq[Seq[Int]] = combine(dimensions.map(d => 0 until d))

  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }

  describe("Test Legacy DenseTensor Get") {
    def legacy_get(indices: Seq[Int], dimensions: Seq[Int]) = {
      //This is the legacy get method from DenseTensor
      var i = 0
      var dimI = 1
      var n = indices.head
      var tail = indices.tail
      while (i < tail.size) {
        var ti = dimI
        var tn = tail.head
        tail = tail.tail
        while (ti < dimensions.size) {
          tn *= dimensions(ti)
          ti += 1
        }
        println(tn)
        dimI += 1
        i += 1
        n += tn
        println(n)
      }
      n
    }

    ignore("Fortran layout") {
        assert(legacy_get(Seq(0, 0), Seq(3, 4)) == 0)
        assert(legacy_get(Seq(2, 3), Seq(3, 4)) == 3 * 3 + 2) // should be 11 no matter C or F but result is 14
        assert(legacy_get(Seq(1, 1), Seq(3, 4)) == 1 * 3 + 1) // should be 4 if follow 'F' but result is 5, it's 'C' 1*4+1
        assert(legacy_get(Seq(1, 1, 1), Seq(3, 4, 5)) == 1 * 4 * 3 + 1 * 3 + 1) //should be 16 if follow 'F' but got 21, 'C' should  be 1*4*5+1*5+1=26
        assert(legacy_get(Seq(2, 3, 4), Seq(3, 4, 5)) == 4 * 4 * 3 + 3 * 3 + 2) // should be 59 no matter 'C' or 'F', but not 62
    }
    ignore("C layout") {
      assert(legacy_get(Seq(0, 0), Seq(3, 4)) == 0)
      assert(legacy_get(Seq(2, 3), Seq(3, 4)) == 2 * 4 + 3) // should be 11 no matter C or F but result is 14
      assert(legacy_get(Seq(1, 1), Seq(3, 4)) == 1 * 4 + 1) // should be 5 if follow 'C'
      assert(legacy_get(Seq(1, 1, 1), Seq(3, 4, 5)) == 1 * 4 * 5 + 1 * 5 + 1) // should be 16 if follow 'F' but got 21, 'C' should  be 1*4*5+1*5+1=26
      assert(legacy_get(Seq(2, 3, 4), Seq(3, 4, 5)) == 2 * 4 * 5 + 3 * 5 + 4) // should be 59 no matter 'C' or 'F', but not 62
    }
  }

  describe("DenseVector") {
    it("should not be equal for dense tensors with different base") {
      val tensor1 = Tensor.denseVector(Array(20, 10, 5))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should be equal for empty dense tensors") {
      val tensor1 = Tensor.denseVector(Array())
      val tensor2 = Tensor.denseVector(Array())
      assert(tensor1 == tensor2)
    }

    it("should be equal for dense tensors with same elements and dimensions") {
      val tensor1 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 == tensor2)
    }

    it("should  not be equal for dense tensors with different dimensions") {
      val tensor1 = Tensor.denseVector(Array(20.0, 10.0, 5.0, 23.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should  not be equal for dense tensors with same dimension but different elements") {
      val tensor1 = Tensor.denseVector(Array(20.0, 11.0, 5.0))
      val tensor2 = Tensor.denseVector(Array(20.0, 10.0, 5.0))
      assert(tensor1 != tensor2)
    }

    it("should be equal for dense tensors with dimension -1 and same elements") {
      val tensor1 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      val tensor2 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 == tensor2)
    }

    it("should not be equal for dense tensors with dimension -1 but different elements") {
      val tensor1 = Tensor.create(Array(2.0, 1.0, 34.0), Seq(-1))
      val tensor2 = Tensor.create(Array(2.0, 5.0, 34.0), Seq(-1))
      assert(tensor1 != tensor2)
    }
  }

  describe("DenseTensor") {
    val random = new scala.util.Random
    val shape = Seq(5, 4, 3, 2, 1)
    val dims = shape.length
    val size = shape.product
    val floatArray = Array.fill(size) {
      random.nextFloat
    }

    it("should be equal for dense tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray, dimensions = shape)
      assert(tensor1 == tensor2)
    }

    it("should not be equal for dense tensors with different dimensions") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray, dimensions = shape.slice(0, dims - 1))
      assert(tensor1 != tensor2)
    }

    it("should not be equal for dense tensors with same dimension but different elements") {
      val tensor1 = Tensor.create(floatArray, dimensions = shape)
      val tensor2 = Tensor.create(floatArray.map(x => x * 2), dimensions = shape)
      assert(tensor1 != tensor2)
    }

    it("should be equal for dense tensors with dimension -1 and same elements") {
      val tensor1 = Tensor.create(floatArray, Seq(-1) ++ shape.slice(1, 5))
      val tensor2 = Tensor.create(floatArray, shape.slice(0, 4) ++ Seq(-1))
      assert(tensor1 == tensor2)
    }

    it("should dense tensors has same elements after flatten compare to original array") {
      val tensor = Tensor.create(floatArray, dimensions = shape)
      assert(floatArray sameElements toIndices(shape).map(indices => tensor(indices: _*)))
    }
  }

  describe("SparseVector") {
    it("should not be equal for sparse tensors with different base") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22, 45, 99), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 != tensor2)
    }

    it("should be equal for empty sparse tensors") {
      val tensor1 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      val tensor2 = Tensor.create(Array(), Seq(), Some(Seq(Seq())))
      assert(tensor1 == tensor2)
    }

    it("should be equal for sparse tensors with same elements and dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 == tensor2)
    }

    it("should not be equal for sparse tensors with different dimensions") {
      val tensor1 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(4), Some(Seq(Seq(1, 2, 4))))
      val tensor2 = Tensor.create(Array(22.3, 45.6, 99.3), Seq(5), Some(Seq(Seq(1, 2, 4))))
      assert(tensor1 != tensor2)
    }

    it("should not be equal for sparse tensors with same dimension but different elements") {
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
    val indices = Seq(Seq(0, 0, 0), Seq(1, 1, 0), Seq(2, 1, 0))
    val array = Array.fill[Float](indices.length) {
      random.nextFloat()
    }
    val shape = Seq(5, 4, 3)
    val otherShape = Seq(3, 2, 1)

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
      val tensor2 = Tensor.create(array.map(x => x * 2), shape, Some(indices))
      assert(tensor1 != tensor2)
    }

    it("should raise exception for sparse tensors with dimension -1") {
      assertThrows[IllegalArgumentException] {
        Tensor.create(array, Seq(-1), Some(indices))
      }
    }

    it("should return true for sparse tensors each elements compare to original array") {
      val tensor = Tensor.create(array, shape, Some(indices))
      assert(array sameElements indices.map(i => tensor(i: _*)))
    }
    it("should return true for sparse tensors each elements compare to original array after converted to dense") {
      val tensor = Tensor.create(array, shape, Some(indices)).toDense
      assert(array sameElements indices.map(i => tensor(i: _*)))
    }

  }
}
