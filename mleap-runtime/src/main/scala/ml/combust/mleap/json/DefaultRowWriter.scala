package ml.combust.mleap.json

import java.nio.charset.Charset

import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowWriter}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.Row

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowWriter(override val schema: StructType) extends RowWriter {
  val rowFormat = RowFormat(schema)

  override def toBytes(row: Row, charset: Charset = BuiltinFormats.charset): Try[Array[Byte]] = {
    Try(rowFormat.write(row).compactPrint.getBytes(charset))
  }
}
