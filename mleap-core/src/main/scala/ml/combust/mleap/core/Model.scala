package ml.combust.mleap.core

import ml.combust.mleap.core.types.StructType

/**
  * Created by hollinwilkins on 7/5/17.
  */
trait Model {
  def inputSchema: StructType
  def outputSchema: StructType
}
