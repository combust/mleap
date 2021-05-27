package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.tensor.{ByteString, Tensor}
import ml.combust.mleap.core.types.TensorType
import org.scalatest.FunSpec
import org.tensorflow.types.TFloat32
import org.tensorflow.types.TFloat64
import org.tensorflow.types.TInt32
import org.tensorflow.types.TInt64
import org.tensorflow.types.TString
import org.tensorflow.types.TUint8

class MleapConverterSpec extends FunSpec {
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

  val random = new scala.util.Random
  val shape = Seq(3, 3, 3, 3, 3, 3, 3)
  val size = shape.product

  describe("round trip from mleap tensor to tensorflow tensor") {

    it("vector") {
      val array = Array.fill(size) {
        random.nextInt()
      }
      val ml_tensor = Tensor.denseVector(array)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Int())
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Int())
      assertSameTensor(ml_tensor, ml_tensor_converted)
    }

    it("int nd dense tensors") {
      val array = Array.fill(size) {
        random.nextInt()
      }
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Int()).asInstanceOf[TInt32]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Int())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
      )
    }

    it("long nd dense tensors") {
      val array = Array.fill(size) {
        random.nextLong()
      }
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Long()).asInstanceOf[TInt64]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Long())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
      )
    }

    it("float nd dense tensors") {
      val array = Array.fill(size) {
        random.nextFloat()
      }
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Float()).asInstanceOf[TFloat32]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Float())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
      )

    }

    it("double nd dense tensors") {
      val array = Array.fill(size) {
        random.nextDouble()
      }
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Double()).asInstanceOf[TFloat64]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Double())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
      )
    }
    it("byte nd dense tensors") {
      val array = Array.fill[Byte](size) {
        0
      }
      random.nextBytes(array)
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.Byte()).asInstanceOf[TUint8]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.Byte())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
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

      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.ByteString()).asInstanceOf[TString]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.ByteString()).asInstanceOf[Tensor[ByteString]]
      assertSameTensor(ml_tensor, ml_tensor_converted)
      // make sure same indices has same value between tensorflow tensor and mleap tensor
      val expected = toIndices(shape).map(indices => ml_tensor(indices: _*).bytes)
      val actual = toIndices(shape).map(indices => tf_tensor_converted.asBytes().getObject(indices.map(_.toLong): _*))
      assert(expected.zip(actual).forall({ case (lhs, rhs) => lhs sameElements rhs }))
    }

    it("string nd dense tensors") {
      val array = Array.fill[String](size) {
        "👋，⛰, Hello️" + random.alphanumeric.take(100).mkString
      } // use emoji to make sure utf-8 encoding is working
      val ml_tensor = Tensor.create(array, dimensions = shape)
      val tf_tensor_converted = MleapConverter.convert(ml_tensor, TensorType.String()).asInstanceOf[TString]
      val ml_tensor_converted = TensorflowConverter.convert(tf_tensor_converted, TensorType.String())
      assertSameTensor(ml_tensor, ml_tensor_converted)
      assert(
        toIndices(shape).map(indices => tf_tensor_converted.getObject(indices.map(_.toLong): _*))
          sameElements toIndices(shape).map(indices => ml_tensor(indices: _*))
      )
    }
  }
}