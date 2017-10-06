package ml.combust.mleap.json

import java.nio.charset.Charset

import ml.combust.mleap.core.frame.Row
import ml.combust.mleap.core.serialization.{BuiltinFormats, RowReader}
import ml.combust.mleap.core.types.StructType
import spray.json._

import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
class DefaultRowReader(override val schema: StructType) extends RowReader {
  val rowFormat: RowFormat = RowFormat(schema)

  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[Row] = {
    Try(rowFormat.read(new String(bytes, charset).parseJson))
  }
}
