package ml.combust.bundle.tensor

import com.google.protobuf
import ml.bundle.bundle.{BasicType, Tensor, TensorDimension, TensorShape}
import ml.combust.mleap.tensor
import ml.combust.mleap.tensor.ByteString

import scala.reflect.{ClassTag, classTag}

/**
  * Created by hollinwilkins on 1/15/17.
  */
object TensorSerializer {
  def toBundleType[T: ClassTag]: BasicType = classTag[T].runtimeClass match {
    case tensor.Tensor.BooleanClass => BasicType.BOOLEAN
    case tensor.Tensor.ByteClass => BasicType.BYTE
    case tensor.Tensor.ShortClass => BasicType.SHORT
    case tensor.Tensor.IntClass => BasicType.INT
    case tensor.Tensor.LongClass => BasicType.LONG
    case tensor.Tensor.FloatClass => BasicType.FLOAT
    case tensor.Tensor.DoubleClass => BasicType.DOUBLE
    case tensor.Tensor.StringClass => BasicType.STRING
    case tensor.Tensor.ByteStringClass => BasicType.BYTE_STRING
    case _ => throw new RuntimeException(s"unsupported base type ${classTag[T].runtimeClass.getName}")
  }

  def toProto[T](t: tensor.Tensor[T]): Tensor = {
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
      case tensor.Tensor.StringClass =>
        (BasicType.DOUBLE, StringArraySerializer.write(t.rawValues.asInstanceOf[Array[String]]))
      case tensor.Tensor.ByteStringClass =>
        (BasicType.DOUBLE, ByteStringArraySerializer.write(t.rawValues.asInstanceOf[Array[ByteString]]))
      case _ => throw new IllegalArgumentException(s"unsupported tensor type ${t.base}")
    }

    val dimensions = t.dimensions.map(i => TensorDimension(size = i))
    Tensor(base = tpe,
      shape = Some(TensorShape(dimensions)),
      value = protobuf.ByteString.copyFrom(values))
  }

  def fromProto[T](t: Tensor): tensor.Tensor[T] = {
    val dimensions = t.shape.get.dimensions.map(_.size)
    val valueBytes = t.value.toByteArray

    val tn = t.base match {
      case BasicType.BOOLEAN =>
        tensor.Tensor.create(BooleanArraySerializer.read(valueBytes), dimensions)
      case BasicType.BYTE =>
        tensor.Tensor.create(ByteArraySerializer.read(valueBytes), dimensions)
      case BasicType.SHORT =>
        tensor.Tensor.create(ShortArraySerializer.read(valueBytes), dimensions)
      case BasicType.INT =>
        tensor.Tensor.create(IntArraySerializer.read(valueBytes), dimensions)
      case BasicType.LONG =>
        tensor.Tensor.create(LongArraySerializer.read(valueBytes), dimensions)
      case BasicType.FLOAT =>
        tensor.Tensor.create(FloatArraySerializer.read(valueBytes), dimensions)
      case BasicType.DOUBLE =>
        tensor.Tensor.create(DoubleArraySerializer.read(valueBytes), dimensions)
      case BasicType.STRING =>
        tensor.Tensor.create(StringArraySerializer.read(valueBytes), dimensions)
      case BasicType.BYTE_STRING =>
        tensor.Tensor.create(ByteStringArraySerializer.read(valueBytes), dimensions)
      case _ => throw new IllegalArgumentException(s"unsupported tensor type ${t.base}")
    }

    tn.asInstanceOf[tensor.Tensor[T]]
  }
}
