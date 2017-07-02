package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{DataShape, BasicType}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class CoalesceModel(base: BasicType,
                         inputShapes: Seq[DataShape]) {
  def apply(values: Any *): Option[Double] = {
    var i = 0
    while(i < values.size) {
      values(i) match {
        case value: Double => return Some(value)
        case Some(value: Double) => return Some(value)
        case None => // next
      }
      i += 1
    }

    None
  }
}
