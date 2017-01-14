package ml.combust.mleap.binary

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.core.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.runtime.types._

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 11/1/16.
  */
object ValueSerializer {
  val byteCharset = Charset.forName("UTF-8")

  def maybeNullableSerializer[T](serializer: ValueSerializer[T],
                                 isNullable: Boolean): ValueSerializer[Any] = {
    if(isNullable) {
      NullableSerializer(serializer).asInstanceOf[ValueSerializer[Any]]
    } else { serializer.asInstanceOf[ValueSerializer[Any]] }
  }

  def serializerForBasicType(basicType: BasicType): ValueSerializer[Any] = basicType match {
    case FloatType(isNullable) => maybeNullableSerializer(FloatSerializer, isNullable)
    case DoubleType(isNullable) => maybeNullableSerializer(DoubleSerializer, isNullable)
    case StringType(isNullable) => maybeNullableSerializer(StringSerializer, isNullable)
    case IntegerType(isNullable) => maybeNullableSerializer(IntegerSerializer, isNullable)
    case LongType(isNullable) => maybeNullableSerializer(LongSerializer, isNullable)
    case BooleanType(isNullable) => maybeNullableSerializer(BooleanSerializer, isNullable)
  }

  def serializerForDataType(dataType: DataType): ValueSerializer[Any] = dataType match {
    case basicType: BasicType => serializerForBasicType(basicType)
    case ListType(base, isNullable) =>
      base match {
        case FloatType(_) => maybeNullableSerializer(ListSerializer(FloatSerializer), isNullable)
        case DoubleType(_) => maybeNullableSerializer(ListSerializer(DoubleSerializer), isNullable)
        case StringType(_) => maybeNullableSerializer(ListSerializer(StringSerializer), isNullable)
        case IntegerType(_) => maybeNullableSerializer(ListSerializer(IntegerSerializer), isNullable)
        case LongType(_) => maybeNullableSerializer(ListSerializer(LongSerializer), isNullable)
        case BooleanType(_) => maybeNullableSerializer(ListSerializer(BooleanSerializer), isNullable)
        case _ => maybeNullableSerializer(ListSerializer(serializerForDataType(base)), isNullable)
      }
    case tt: TensorType =>
      val isNullable = tt.isNullable
      tt.base match {
        case FloatType(_) => maybeNullableSerializer(TensorSerializer(FloatSerializer), isNullable)
        case DoubleType(_) => maybeNullableSerializer(TensorSerializer(DoubleSerializer), isNullable)
        case StringType(_) => maybeNullableSerializer(TensorSerializer(StringSerializer), isNullable)
        case IntegerType(_) => maybeNullableSerializer(TensorSerializer(IntegerSerializer), isNullable)
        case LongType(_) => maybeNullableSerializer(TensorSerializer(LongSerializer), isNullable)
        case BooleanType(_) => maybeNullableSerializer(TensorSerializer(BooleanSerializer), isNullable)
      }
    case ct: CustomType => maybeNullableSerializer(CustomSerializer(ct), ct.isNullable)
    case _ => throw new IllegalArgumentException(s"invalid data type for serialization: $dataType")
  }
}

trait ValueSerializer[T] {
  def write(value: T, out: DataOutputStream): Unit
  def read(in: DataInputStream): T
}

case class NullableSerializer[T](base: ValueSerializer[T]) extends ValueSerializer[Option[T]] {
  override def write(value: Option[T], out: DataOutputStream): Unit = {
    out.writeBoolean(value.isDefined)
    value.foreach(v => base.write(v, out))
  }

  override def read(in: DataInputStream): Option[T] = {
    if(in.readBoolean()) {
      Option(base.read(in))
    } else { None }
  }
}

object DoubleSerializer extends ValueSerializer[Double] {
  override def write(value: Double, out: DataOutputStream): Unit = out.writeDouble(value)
  override def read(in: DataInputStream): Double = in.readDouble()
}

object FloatSerializer extends ValueSerializer[Float] {
  override def write(value: Float, out: DataOutputStream): Unit = out.writeFloat(value)
  override def read(in: DataInputStream): Float = in.readFloat()
}

object IntegerSerializer extends ValueSerializer[Int] {
  override def write(value: Int, out: DataOutputStream): Unit = out.writeInt(value)
  override def read(in: DataInputStream): Int = in.readInt()
}

object LongSerializer extends ValueSerializer[Long] {
  override def write(value: Long, out: DataOutputStream): Unit = out.writeLong(value)
  override def read(in: DataInputStream): Long = in.readLong()
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
      val indices = new Array[Array[Int]](size)
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

case class CustomSerializer(ct: CustomType) extends ValueSerializer[Any] {
  override def write(value: Any, out: DataOutputStream): Unit = {
    val bytes = ct.toBytes(value)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def read(in: DataInputStream): Any = {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    ct.fromBytes(bytes)
  }
}
