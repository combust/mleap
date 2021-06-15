package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.core.types.TensorType
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.scalatest.FunSpec
import org.tensorflow.types._

class MleapConverterSpec extends FunSpec {
  val random = new scala.util.Random
  val shape = Seq(3, 3, 3, 3, 3, 3, 3)
  val size = shape.product

  def toIndices(dimensions: Traversable[Int]): Seq[Seq[Int]] = {
    combine(dimensions.map(d => 0 until d))
  }

  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }

  def assertSameTensor(lhr: Tensor[_], rhs: Tensor[_]) = {
    assert(lhr.dimensions == rhs.dimensions)
    assert(lhr.base == rhs.base)
    assert(lhr.size == lhr.size)
    assert(lhr.toArray sameElements lhr.toArray)
  }

  describe("round trip from mleap tensor to tensorflow tensor") {

    it("vector") {
      val array = Array.fill(size) {
        random.nextInt()
      }
      val mlTensor = Tensor.denseVector(array)
      val tfTensorConverted = MleapConverter.convert(mlTensor)
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Int())
      assertSameTensor(mlTensor, mlTensorConverted)
    }

    it("int nd dense tensors") {
      val array = Array.fill(size) {
        random.nextInt()
      }
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TInt32]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Int())
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )
    }

    it("long nd dense tensors") {
      val array = Array.fill(size) {
        random.nextLong()
      }
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TInt64]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Long())
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )
    }

    it("float nd dense tensors") {
      val array = Array.fill(size) {
        random.nextFloat()
      }
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TFloat32]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Float())
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )

    }

    it("double nd dense tensors") {
      val array = Array.fill(size) {
        random.nextDouble()
      }
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TFloat64]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Double())
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )
    }
    it("byte nd dense tensors") {
      val array = Array.fill[Byte](size) {
        0
      }
      random.nextBytes(array)
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TUint8]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.Byte())
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )
    }

    it("byte string nd dense tensors") {
      val array = Array.fill[ByteString](size) {
        val bytes = Array.fill[Byte](100) {
          'a'
        }
        random.nextBytes(bytes)
        ByteString(bytes)
      }

      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TString]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.ByteString()).asInstanceOf[Tensor[ByteString]]
      assertSameTensor(mlTensor, mlTensorConverted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      val expected = toIndices(shape).map(indices => mlTensor(indices: _*).bytes)
      val actual = toIndices(shape).map(indices => tfTensorConverted.asBytes().getObject(indices.map(_.toLong): _*))
      assert(expected.zip(actual).forall({ case (lhs, rhs) => lhs sameElements rhs }))
    }

    it("string nd dense tensors") {
      val array = Array.fill[String](size) {
        "👋，⛰, Hello️" + random.alphanumeric.take(100).mkString
      } // use emoji to make sure utf-8 encoding is working
      val mlTensor = Tensor.create(array, dimensions = shape)
      val tfTensorConverted = MleapConverter.convert(mlTensor).asInstanceOf[TString]
      val mlTensorConverted = TensorflowConverter.convert(tfTensorConverted, TensorType.String())
      assertSameTensor(mlTensor, mlTensorConverted)
      assert(
        toIndices(shape).map(indices => tfTensorConverted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => mlTensor(indices: _*))
      )
    }

    it("int16 nd dense tensor should raise exception") {
      try {
        val array = Array.fill[Short](size) {
          0
        }
        val mlTensor = Tensor.create(array, dimensions = shape)
        val _ = MleapConverter.convert(mlTensor)
        fail()
      }
      catch {
        case _: IllegalArgumentException => // continue
      }
    }
  }
}
