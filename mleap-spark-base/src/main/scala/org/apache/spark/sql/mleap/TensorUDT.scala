package org.apache.spark.sql.mleap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

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
  override def sqlType: DataType = StructType(Seq(
    StructField("type", ByteType, nullable = false),
    StructField("binary", ArrayType(ByteType, containsNull = false), nullable = false)
  ))

  override def userClass: Class[Tensor[_]] = classOf[Tensor[_]]

  override def serialize(obj: Tensor[_]): InternalRow = {
    val binary = (for(outByte <- managed(new ByteArrayOutputStream())) yield {
      val out = new DataOutputStream(outByte)
      if(obj.base == Tensor.BOOLEAN) {
        TensorSerializer(BooleanSerializer).write(obj.asInstanceOf[Tensor[Boolean]], out)
      } else if(obj.base == Tensor.STRING) {
        TensorSerializer(StringSerializer).write(obj.asInstanceOf[Tensor[String]], out)
      } else if(obj.base == Tensor.INT) {
        TensorSerializer(IntegerSerializer).write(obj.asInstanceOf[Tensor[Int]], out)
      } else if(obj.base == Tensor.LONG) {
        TensorSerializer(LongSerializer).write(obj.asInstanceOf[Tensor[Long]], out)
      } else if(obj.base == Tensor.FLOAT) {
        TensorSerializer(FloatSerializer).write(obj.asInstanceOf[Tensor[Float]], out)
      } else if(obj.base == Tensor.DOUBLE) {
        TensorSerializer(DoubleSerializer).write(obj.asInstanceOf[Tensor[Double]], out)
      } else {
        throw new RuntimeException(s"unsupported tensor type: ${obj.base}")
      }

      out.flush()
      outByte.toByteArray
    }).opt.get

    val row = new GenericInternalRow(2)
    row.setByte(0, obj.base)
    row.update(1, UnsafeArrayData.fromPrimitiveArray(binary))
    row
  }

  override def deserialize(datum: Any): Tensor[_] = {
    datum match {
      case row: InternalRow =>
        val binary = row.getArray(1).toByteArray()
        (for(in <- managed(new DataInputStream(new ByteArrayInputStream(binary)))) yield {
          row.getByte(0) match {
            case Tensor.BOOLEAN =>
              TensorSerializer(BooleanSerializer).read(in)
            case Tensor.STRING =>
              TensorSerializer(StringSerializer).read(in)
            case Tensor.INT =>
              TensorSerializer(IntegerSerializer).read(in)
            case Tensor.LONG =>
              TensorSerializer(LongSerializer).read(in)
            case Tensor.FLOAT =>
              TensorSerializer(FloatSerializer).read(in)
            case Tensor.DOUBLE =>
              TensorSerializer(DoubleSerializer).read(in)
          }
        }).opt.get
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
