package ml.combust.bundle.v07.tensor

import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import ml.bundle.v07.BasicType
import ml.bundle.v07.Tensor
import ml.bundle.v07.TensorType
import ml.combust.mleap.tensor

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 1/15/17.
  */
object TensorSerializer {
  def toBundleType[T](base: ClassTag[T]): BasicType = base.runtimeClass match {
    case tensor.Tensor.BooleanClass => BasicType.BOOLEAN
    case tensor.Tensor.StringClass => BasicType.STRING
    case tensor.Tensor.ByteClass => BasicType.BYTE
    case tensor.Tensor.ShortClass => BasicType.SHORT
    case tensor.Tensor.IntClass => BasicType.INT
    case tensor.Tensor.LongClass => BasicType.LONG
    case tensor.Tensor.FloatClass => BasicType.FLOAT
    case tensor.Tensor.DoubleClass => BasicType.DOUBLE
    case _ => throw new RuntimeException(s"unsupported base type ${base.runtimeClass.getName}")
  }

  def toProto(t: tensor.Tensor[_]): Tensor = {
    val indices = t match {
      case t: tensor.SparseTensor[_] =>
        ByteString.copyFrom(writeIndices(t.indices, t.dimensions))
      case _: tensor.DenseTensor[_] => ByteString.EMPTY
    }

    val (tpe, values) = t.base.runtimeClass match {
      case tensor.Tensor.BooleanClass =>
        (BasicType.BOOLEAN, BooleanArraySerializer.write(t.rawValues.asInstanceOf[Array[Boolean]]))
      case tensor.Tensor.ByteClass =>
        (BasicType.BYTE, ByteArraySerializer.write(t.rawValues.asInstanceOf[Array[Byte]]))
      case tensor.Tensor.ShortClass =>
        (BasicType.SHORT, ShortArraySerializer.write(t.rawValues.asInstanceOf[Array[Short]]))
      case tensor.Tensor.IntClass =>
        (BasicType.INT, IntArraySerializer.write(t.rawValues.asInstanceOf[Array[Int]]))
      case tensor.Tensor.LongClass =>
        (BasicType.LONG, LongArraySerializer.write(t.rawValues.asInstanceOf[Array[Long]]))
      case tensor.Tensor.FloatClass =>
        (BasicType.FLOAT, FloatArraySerializer.write(t.rawValues.asInstanceOf[Array[Float]]))
      case tensor.Tensor.DoubleClass =>
        (BasicType.DOUBLE, DoubleArraySerializer.write(t.rawValues.asInstanceOf[Array[Double]]))
      case _ => throw new IllegalArgumentException(s"unsupported tensor type ${t.base}")
    }

    Tensor(base = tpe,
      dimensions = t.dimensions,
      value = ByteString.copyFrom(values),
      indices = indices)
  }

  def fromProto(tt: TensorType, t: Tensor): tensor.Tensor[_] = {
    val dimensions = t.dimensions
    val indices = if(!t.indices.isEmpty) {
      Some(readIndices(t.indices.toByteArray, dimensions))
    } else { None }
    val valueBytes = t.value.toByteArray

    tt.base match {
      case BasicType.BOOLEAN =>
        tensor.Tensor.create(BooleanArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.STRING =>
        tensor.Tensor.create(StringArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.BYTE =>
        tensor.Tensor.create(ByteArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.SHORT =>
        tensor.Tensor.create(ShortArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.INT =>
        tensor.Tensor.create(IntArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.LONG =>
        tensor.Tensor.create(LongArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.FLOAT =>
        tensor.Tensor.create(FloatArraySerializer.read(valueBytes), dimensions, indices)
      case BasicType.DOUBLE =>
        tensor.Tensor.create(DoubleArraySerializer.read(valueBytes), dimensions, indices)
      case _ => throw new IllegalArgumentException(s"unsupported tensor type ${tt.base}")
    }
  }

  def writeIndices(indices: Seq[Seq[Int]], dims: Seq[Int]): Array[Byte] = {
    val b = ByteBuffer.allocate(dims.product)
    indices.foreach(_.foreach(b.putInt))
    b.array()
  }

  def readIndices(bytes: Array[Byte], dims: Seq[Int]): Seq[Seq[Int]] = {
    val b = ByteBuffer.wrap(bytes)
    val indices = new Array[Seq[Int]](dims.product)

    var i = 0
    while(b.hasRemaining) {
      indices(i) = dims.indices.map(_ => b.getInt())
      i += 1
    }

    indices
  }
}
