package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class CoalesceModel(nullableInputs: Seq[Boolean]) extends Model {
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

  override def inputSchema: StructType = {
    val is = nullableInputs.zipWithIndex.map {
      case (n, i) => StructField(s"input$i", ScalarType.Double.setNullable(n))
    }
    StructType(is).get
  }

  override def outputSchema: StructType = StructType(StructField("output", ScalarType.Double)).get
}
