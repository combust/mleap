package ml.combust.mleap.runtime.serialization.json

import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.serialization.RowSerializer
import ml.combust.mleap.runtime.types.StructType
import spray.json._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class JsonRowSerializer(schema: StructType) extends RowSerializer {
  val rowFormat: RowFormat = RowFormat(schema)

  override def toBytes(row: Row): Array[Byte] = rowFormat.write(row).compactPrint.getBytes(DefaultFrameSerializer.charset)
  override def fromBytes(bytes: Array[Byte]): Row = rowFormat.read(new String(bytes, DefaultFrameSerializer.charset).parseJson)
}
