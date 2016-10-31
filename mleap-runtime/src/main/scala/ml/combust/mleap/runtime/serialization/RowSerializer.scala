package ml.combust.mleap.runtime.serialization

import ml.combust.mleap.runtime.Row

/**
  * Created by hollinwilkins on 10/31/16.
  */
trait RowSerializer {
  def toBytes(row: Row): Array[Byte]
  def fromBytes(bytes: Array[Byte]): Row
}
