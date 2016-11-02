package ml.combust.mleap.json

import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowWriter}
import ml.combust.mleap.runtime.types.StructType

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowWriter(override val schema: StructType) extends RowWriter {
  val rowFormat = RowFormat(schema)

  override def toBytes(row: Row): Array[Byte] = rowFormat.write(row).compactPrint.getBytes(BuiltinFormats.charset)
}
