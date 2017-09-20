package ml.combust.mleap.binary

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.core.types.{BasicType, DataType, TensorType}
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{ByteString, DenseTensor, SparseTensor, Tensor}

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 11/1/16.
  */
object ValueSerializer {
  val byteCharset: Charset = Charset.forName("UTF-8")

  def maybeNullableSerializer[T](serializer: ValueSerializer[T],
                                 isNullable: Boolean): ValueSerializer[Any] = {
    if(isNullable) {
      NullableSerializer(serializer).asInstanceOf[ValueSerializer[Any]]
    } else { serializer.asInstanceOf[ValueSerializer[Any]] }
  }

  def serializerForBasicType(basicType: BasicType, isNullable: Boolean): ValueSerializer[Any] = basicType match {
    case BasicType.Boolean => maybeNullableSerializer(BooleanSerializer, isNullable)
    case BasicType.Byte => maybeNullableSerializer(ByteSerializer, isNullable)
    case BasicType.Short => maybeNullableSerializer(ShortSerializer, isNullable)
    case BasicType.Int => maybeNullableSerializer(IntegerSerializer, isNullable)
    case BasicType.Long => maybeNullableSerializer(LongSerializer, isNullable)
    case BasicType.Float => maybeNullableSerializer(FloatSerializer, isNullable)
    case BasicType.Double => maybeNullableSerializer(DoubleSerializer, isNullable)
    case BasicType.String => maybeNullableSerializer(StringSerializer, isNullable)
    case BasicType.ByteString => maybeNullableSerializer(ByteStringSerializer, isNullable)
  }

  def serializerForDataType(dataType: DataType): ValueSerializer[Any] = dataType match {
    case ScalarType(base, isNullable) => serializerForBasicType(base, isNullable)
    case ListType(base, isNullable) =>
      base match {
        case BasicType.Boolean => maybeNullableSerializer(ListSerializer(BooleanSerializer), isNullable)
        case BasicType.Byte => maybeNullableSerializer(ListSerializer(ByteSerializer), isNullable)
        case BasicType.Short => maybeNullableSerializer(ListSerializer(ShortSerializer), isNullable)
        case BasicType.Int => maybeNullableSerializer(ListSerializer(IntegerSerializer), isNullable)
        case BasicType.Long => maybeNullableSerializer(ListSerializer(LongSerializer), isNullable)
        case BasicType.Float => maybeNullableSerializer(ListSerializer(FloatSerializer), isNullable)
        case BasicType.Double => maybeNullableSerializer(ListSerializer(DoubleSerializer), isNullable)
        case BasicType.String => maybeNullableSerializer(ListSerializer(StringSerializer), isNullable)
        case BasicType.ByteString => maybeNullableSerializer(ListSerializer(ByteStringSerializer), isNullable)
      }
    case tt: TensorType =>
      val isNullable = tt.isNullable
      tt.base match {
        case BasicType.Boolean => maybeNullableSerializer(TensorSerializer(BooleanSerializer), isNullable)
        case BasicType.Byte => maybeNullableSerializer(TensorSerializer(ByteSerializer), isNullable)
        case BasicType.Short => maybeNullableSerializer(TensorSerializer(ShortSerializer), isNullable)
        case BasicType.Int => maybeNullableSerializer(TensorSerializer(IntegerSerializer), isNullable)
        case BasicType.Long => maybeNullableSerializer(TensorSerializer(LongSerializer), isNullable)
        case BasicType.Float => maybeNullableSerializer(TensorSerializer(FloatSerializer), isNullable)
        case BasicType.Double => maybeNullableSerializer(TensorSerializer(DoubleSerializer), isNullable)
        case BasicType.String => maybeNullableSerializer(TensorSerializer(StringSerializer), isNullable)
        case BasicType.ByteString => maybeNullableSerializer(TensorSerializer(ByteStringSerializer), isNullable)
      }
    case _ => throw new IllegalArgumentException(s"invalid data type for serialization: $dataType")
  }
}

trait ValueSerializer[T] {
  def write(value: T, out: DataOutputStream): Unit
  def read(in: DataInputStream): T
}

case class NullableSerializer[T](base: ValueSerializer[T]) extends ValueSerializer[T] {
  override def write(value: T, out: DataOutputStream): Unit = {
    val o = Option(value)
    out.writeBoolean(o.isDefined)
    o.foreach(v => base.write(v, out))
  }

  override def read(in: DataInputStream): T = {
    if(in.readBoolean()) {
      base.read(in)
    } else { null.asInstanceOf[T] }
  }
}

object BooleanSerializer extends ValueSerializer[Boolean] {
  override def write(value: Boolean, out: DataOutputStream): Unit = out.writeBoolean(value)
  override def read(in: DataInputStream): Boolean = in.readBoolean()
}

object StringSerializer extends ValueSerializer[String] {
  override def write(value: String, out: DataOutputStream): Unit = {
    val bytes = value.getBytes(ValueSerializer.byteCharset)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def read(in: DataInputStream): String = {
    val bytes = new Array[Byte](in.readInt())
    in.readFully(bytes)
    new String(bytes, ValueSerializer.byteCharset)
  }
}

object ByteSerializer extends ValueSerializer[Byte] {
  override def write(value: Byte, out: DataOutputStream): Unit = out.writeByte(value)
  override def read(in: DataInputStream): Byte = in.readByte()
}

object ShortSerializer extends ValueSerializer[Short] {
  override def write(value: Short, out: DataOutputStream): Unit = out.writeShort(value)
  override def read(in: DataInputStream): Short = in.readShort()
}

object IntegerSerializer extends ValueSerializer[Int] {
  override def write(value: Int, out: DataOutputStream): Unit = out.writeInt(value)
  override def read(in: DataInputStream): Int = in.readInt()
}

object LongSerializer extends ValueSerializer[Long] {
  override def write(value: Long, out: DataOutputStream): Unit = out.writeLong(value)
  override def read(in: DataInputStream): Long = in.readLong()
}

object FloatSerializer extends ValueSerializer[Float] {
  override def write(value: Float, out: DataOutputStream): Unit = out.writeFloat(value)
  override def read(in: DataInputStream): Float = in.readFloat()
}

object DoubleSerializer extends ValueSerializer[Double] {
  override def write(value: Double, out: DataOutputStream): Unit = out.writeDouble(value)
  override def read(in: DataInputStream): Double = in.readDouble()
}

object ByteStringSerializer extends ValueSerializer[ByteString] {
  override def write(value: ByteString, out: DataOutputStream): Unit = {
    out.writeInt(value.size)
    out.write(value.bytes)
  }
  override def read(in: DataInputStream): ByteString = {
    val size = in.readInt()
    val bytes = new Array[Byte](size)
    in.readFully(bytes)
    ByteString(bytes)
  }
}

case class ListSerializer[T: ClassTag](base: ValueSerializer[T]) extends ValueSerializer[Seq[T]] {
  override def write(value: Seq[T], out: DataOutputStream): Unit = {
    out.writeInt(value.length)
    for(v <- value) { base.write(v, out) }
  }

  override def read(in: DataInputStream): Seq[T] = {
    val length = in.readInt()
    val arr = new Array[T](length)
    for(i <- 0 until length) { arr(i) = base.read(in) }
    arr
  }
}

case class TensorSerializer[T: ClassTag](base: ValueSerializer[T]) extends ValueSerializer[Tensor[T]] {
  val DENSE = 0
  val SPARSE = 1

  override def write(value: Tensor[T], out: DataOutputStream): Unit = {
    if(value.isDense) out.writeChar(DENSE) else out.writeChar(SPARSE)

    out.writeChar(value.dimensions.length)
    value.dimensions.foreach(out.writeInt)

    value match {
      case dense: DenseTensor[_] =>
        out.writeInt(dense.values.length)
        dense.asInstanceOf[DenseTensor[T]].values.foreach(v => base.write(v, out))
      case sparse: SparseTensor[_] =>
        out.writeInt(sparse.values.length)
        sparse.asInstanceOf[SparseTensor[T]].values.foreach(v => base.write(v, out))
        out.writeInt(sparse.indices.length)
        sparse.asInstanceOf[SparseTensor[T]].indices.foreach {
          index =>
            out.writeChar(index.length)
            index.foreach(out.writeInt)
        }
    }
  }

  override def read(in: DataInputStream): Tensor[T] = {
    val tpe = in.readChar()
    var size: Int = in.readChar()
    val dimensions = (0 until size).map(_ => in.readInt())

    size = in.readInt()
    val values = new Array[T](size)
    for(i <- 0 until size) { values(i) = base.read(in) }

    if(tpe == DENSE) {
      DenseTensor(values, dimensions)
    } else if(tpe == SPARSE) {
      size = in.readInt()
      val indices = new Array[Seq[Int]](size)
      indices.indices.foreach {
        i =>
          val indexSize = in.readChar()
          val index = new Array[Int](indexSize)
          index.indices.foreach(i => index(i) = in.readInt())
          indices(i) = index
      }
      SparseTensor(indices, values, dimensions)
    } else {
      throw new RuntimeException(s"invalid tensor type: $tpe")
    }
  }
}
