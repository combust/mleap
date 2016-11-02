package ml.combust.mleap.json

import java.nio.charset.Charset

import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowReader}
import ml.combust.mleap.runtime.types.StructType
import spray.json._

/**
  * Created by hollinwilkins on 11/1/16.
  */
class DefaultRowReader(override val schema: StructType) extends RowReader {
  val rowFormat: RowFormat = RowFormat(schema)

  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Row = {
    rowFormat.read(new String(bytes, charset).parseJson)
  }
}
