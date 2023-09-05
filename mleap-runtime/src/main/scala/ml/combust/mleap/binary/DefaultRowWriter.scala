package ml.combust.mleap.binary

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowWriter}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.Row
import scala.util.Using

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowWriter(override val schema: StructType) extends RowWriter {
  private val serializers = schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)

  override def toBytes(row: Row, charset: Charset = BuiltinFormats.charset): Try[Array[Byte]] = {
    Using(new ByteArrayOutputStream()) { out =>
      val dout = new DataOutputStream(out)
      var i = 0
      for(s <- serializers) {
        s.write(row.getRaw(i), dout)
        i = i + 1
      }
      dout.flush()
      out.toByteArray
    }
  }
}
