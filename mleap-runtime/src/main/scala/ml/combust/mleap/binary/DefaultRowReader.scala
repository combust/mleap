package ml.combust.mleap.binary

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowReader}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.{ArrayRow, Row}
import scala.util.Using

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowReader(override val schema: StructType) extends RowReader {
  private val serializers = schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)

  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[Row] = {
    Using(new ByteArrayInputStream(bytes)) { in =>
      val din = new DataInputStream(in)
      val row = ArrayRow((new Array[Any](schema.fields.length)).toSeq)
      var i = 0
      for(s <- serializers) {
        row.set(i, s.read(din))
        i = i + 1
      }
      row
    }
  }
}
