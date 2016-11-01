package ml.combust.mleap.binary

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime._
import ml.combust.mleap.runtime.serialization.{FrameSerializer, FrameSerializerContext, RowSerializer}
import ml.combust.mleap.runtime.types.StructType
import ml.combust.mleap.json.JsonSupport._
import spray.json._
import resource._

import scala.collection.mutable

/**
  * Created by hollinwilkins on 11/1/16.
  */
object DefaultFrameSerializer {
  val byteCharset = Charset.forName("UTF-8")
}

class DefaultFrameSerializer(override val serializerContext: FrameSerializerContext) extends FrameSerializer {
  implicit val context = serializerContext.context

  override def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = {
    val serializers = frame.schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)
    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val dout = new DataOutputStream(out)
      val schemaBytes = frame.schema.toJson.prettyPrint.getBytes(DefaultFrameSerializer.byteCharset)
      dout.writeInt(schemaBytes.length)
      dout.write(schemaBytes)
      dout.writeInt(frame.dataset.size)

      for(row <- frame.dataset) {
        var i = 0
        for(s <- serializers) {
          s.write(row(i), dout)
          i = i + 1
        }
      }

      dout.flush()
      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }

  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    (for(in <- managed(new ByteArrayInputStream(bytes))) yield {
      val din = new DataInputStream(in)
      val length = din.readInt()
      val schemaBytes = new Array[Byte](length)
      din.readFully(schemaBytes)
      val schema = new String(schemaBytes, DefaultFrameSerializer.byteCharset).parseJson.convertTo[StructType]
      val serializers = schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)
      val rowCount = din.readInt()
      val rows = mutable.WrappedArray.make[Row](new Array[Row](rowCount))

      for(i <- 0 until rowCount) {
        val row = new ArrayRow(new Array[Any](schema.fields.length))

        var j = 0
        for(s <- serializers) {
          row.set(j, s.read(din))
          j = j + 1
        }

        rows(i) = row
      }

      val dataset = LocalDataset(rows)
      DefaultLeapFrame(schema, dataset)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(frame) => frame
    }
  }

  override def rowSerializer(schema: StructType): BinaryRowSerializer = BinaryRowSerializer(schema)
}
