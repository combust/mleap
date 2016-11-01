package ml.combust.mleap.binary

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import ml.combust.mleap.runtime.{ArrayRow, Row}
import ml.combust.mleap.runtime.serialization.RowSerializer
import ml.combust.mleap.runtime.types.StructType
import resource._

/**
  * Created by hollinwilkins on 11/1/16.
  */
case class BinaryRowSerializer(schema: StructType) extends RowSerializer {
  val serializers = schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)

  override def toBytes(row: Row): Array[Byte] = {
    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val dout = new DataOutputStream(out)
      var i = 0
      for(s <- serializers) {
        s.write(row(i), dout)
        i = i + 1
      }
      dout.flush()
      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }

  override def fromBytes(bytes: Array[Byte]): Row = {
    (for(in <- managed(new ByteArrayInputStream(bytes))) yield {
      val din = new DataInputStream(in)
      val row = ArrayRow(new Array[Any](schema.fields.length))
      var i = 0
      for(s <- serializers) {
        row.set(i, s.read(din))
        i = i + 1
      }
      row
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(row) => row
    }
  }
}
