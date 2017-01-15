package ml.combust.bundle.tensor

import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import ml.bundle.BasicType.BasicType
import ml.bundle.Tensor.Tensor
import ml.bundle.TensorType.TensorType
import ml.combust.mleap.tensor

/**
  * Created by hollinwilkins on 1/15/17.
  */
object TensorSerializer {
  def toBundleType(tpe: Byte): BasicType = tpe match {
    case tensor.Tensor.BOOLEAN => BasicType.BOOLEAN
    case tensor.Tensor.STRING => BasicType.STRING
    case tensor.Tensor.BYTE => BasicType.BYTE
    case tensor.Tensor.SHORT => BasicType.SHORT
    case tensor.Tensor.INT => BasicType.INT
    case tensor.Tensor.LONG => BasicType.LONG
    case tensor.Tensor.FLOAT => BasicType.FLOAT
    case tensor.Tensor.DOUBLE => BasicType.DOUBLE
    case _ => throw new RuntimeException(s"unsupported base type $tpe")
  }

  def toProto(t: tensor.Tensor[_]): Tensor = {
    val indices = t match {
      case t: tensor.SparseTensor[_] =>
        ByteString.copyFrom(writeIndices(t.indices, t.dimensions))
      case t: tensor.DenseTensor[_] => ByteString.EMPTY
    }

    val (tpe, values) = t.base match {
      case tensor.Tensor.BOOLEAN =>
        (BasicType.BOOLEAN, BooleanArraySerializer.write(t.rawValues.asInstanceOf[Array[Boolean]]))
      case tensor.Tensor.BYTE =>
        (BasicType.BYTE, ByteArraySerializer.write(t.rawValues.asInstanceOf[Array[Byte]]))
      case tensor.Tensor.SHORT =>
        (BasicType.SHORT, ShortArraySerializer.write(t.rawValues.asInstanceOf[Array[Short]]))
      case tensor.Tensor.INT =>
        (BasicType.INT, IntArraySerializer.write(t.rawValues.asInstanceOf[Array[Int]]))
      case tensor.Tensor.LONG =>
        (BasicType.LONG, LongArraySerializer.write(t.rawValues.asInstanceOf[Array[Long]]))
      case tensor.Tensor.FLOAT =>
        (BasicType.FLOAT, FloatArraySerializer.write(t.rawValues.asInstanceOf[Array[Float]]))
      case tensor.Tensor.DOUBLE =>
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
