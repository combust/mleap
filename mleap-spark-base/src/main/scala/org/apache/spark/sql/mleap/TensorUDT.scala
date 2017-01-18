package org.apache.spark.sql.mleap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import ml.combust.mleap.binary._
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._
import resource._

/**
  * Created by hollinwilkins on 1/14/17.
  */
object TensorUDT {
  UDTRegistration.register(classOf[Tensor[_]].getName, classOf[TensorUDT].getName)
}

class TensorUDT extends UserDefinedType[Tensor[_]] {
  val BOOLEAN: Byte = 0
  val STRING: Byte = 1
  val BYTE: Byte = 2
  val SHORT: Byte = 3
  val INT: Byte = 4
  val LONG: Byte = 5
  val FLOAT: Byte = 6
  val DOUBLE: Byte = 7

  override def sqlType: DataType = StructType(Seq(
    StructField("type", ByteType, nullable = false),
    StructField("binary", ArrayType(IntegerType, containsNull = false), nullable = false)
  ))

  override def userClass: Class[Tensor[_]] = classOf[Tensor[_]]

  override def serialize(obj: Tensor[_]): InternalRow = {
    val binary = (for(outByte <- managed(new ByteArrayOutputStream())) yield {
      val out = new DataOutputStream(outByte)
      obj.base.runtimeClass match {
        case Tensor.BooleanClass =>
          TensorSerializer(BooleanSerializer).write(obj.asInstanceOf[Tensor[Boolean]], out)
        case Tensor.StringClass =>
          TensorSerializer(StringSerializer).write(obj.asInstanceOf[Tensor[String]], out)
        case Tensor.ByteClass =>
          TensorSerializer(ByteSerializer).write(obj.asInstanceOf[Tensor[Byte]], out)
        case Tensor.ShortClass =>
          TensorSerializer(ShortSerializer).write(obj.asInstanceOf[Tensor[Short]], out)
        case Tensor.IntClass =>
          TensorSerializer(IntegerSerializer).write(obj.asInstanceOf[Tensor[Int]], out)
        case Tensor.LongClass =>
          TensorSerializer(LongSerializer).write(obj.asInstanceOf[Tensor[Long]], out)
        case Tensor.FloatClass =>
          TensorSerializer(FloatSerializer).write(obj.asInstanceOf[Tensor[Float]], out)
        case Tensor.DoubleClass =>
          TensorSerializer(DoubleSerializer).write(obj.asInstanceOf[Tensor[Double]], out)
        case _ =>
          throw new RuntimeException(s"unsupported tensor type: ${obj.base}")
      }

      out.flush()
      outByte.toByteArray
    }).tried.get.map(_.toInt)

    val typeByte = obj.base.runtimeClass match {
      case Tensor.BooleanClass => BOOLEAN
      case Tensor.StringClass => STRING
      case Tensor.ByteClass => BYTE
      case Tensor.ShortClass => SHORT
      case Tensor.IntClass => INT
      case Tensor.LongClass => LONG
      case Tensor.FloatClass => FLOAT
      case Tensor.DoubleClass => DOUBLE
      case _ =>
        throw new RuntimeException(s"unsupported tensor type: ${obj.base}")
    }

    new GenericInternalRow(Array(typeByte, UnsafeArrayData.fromPrimitiveArray(binary)))
  }

  override def deserialize(datum: Any): Tensor[_] = {
    datum match {
      case row: InternalRow =>
        val rawInts = row.getArray(1).toIntArray()
        val b = ByteBuffer.allocate(rawInts.length)
        rawInts.foreach(i => b.put(i.toByte))
        val binary = b.array()
        (for(in <- managed(new DataInputStream(new ByteArrayInputStream(binary)))) yield {
          row.getByte(0) match {
            case BOOLEAN =>
              TensorSerializer(BooleanSerializer).read(in)
            case STRING =>
              TensorSerializer(StringSerializer).read(in)
            case BYTE =>
              TensorSerializer(ByteSerializer).read(in)
            case SHORT =>
              TensorSerializer(ShortSerializer).read(in)
            case INT =>
              TensorSerializer(IntegerSerializer).read(in)
            case LONG =>
              TensorSerializer(LongSerializer).read(in)
            case FLOAT =>
              TensorSerializer(FloatSerializer).read(in)
            case DOUBLE =>
              TensorSerializer(DoubleSerializer).read(in)
            case _ =>
              throw new RuntimeException(s"invalid base type byte: ${row.getByte(0)}")
          }
        }).tried.get
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case v: TensorUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[TensorUDT].getName.hashCode()
}
