package ml.combust.mleap.binary

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.json.JsonSupport._
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Row}
import spray.json._
import scala.util.Using

import scala.collection.mutable
import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader extends FrameReader {
  override def fromBytes(bytes: Array[Byte],
                         charset: Charset = BuiltinFormats.charset): Try[DefaultLeapFrame] = {
    Using(new ByteArrayInputStream(bytes)) { in =>
      val din = new DataInputStream(in)
      val length = din.readInt()
      val schemaBytes = new Array[Byte](length)
      din.readFully(schemaBytes)
      val schema = new String(schemaBytes, BuiltinFormats.charset).parseJson.convertTo[StructType]
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

      DefaultLeapFrame(schema, rows.toSeq)
    }
  }
}
