package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class CoalesceModel(nullableInputs: Seq[Boolean]) extends Model {
  def apply(values: Any *): java.lang.Double = {
    var i = 0
    while(i < values.size) {
      Option(values(i)) match {
        case Some(value) => return value.asInstanceOf[Double]
        case None => // next
      }
      i += 1
    }

    null
  }

  override def inputSchema: StructType = {
    val is = nullableInputs.zipWithIndex.map {
      case (n, i) => StructField(s"input$i", ScalarType.Double.setNullable(n))
    }
    StructType(is).get
  }

  override def outputSchema: StructType = StructType(StructField("output", ScalarType.Double)).get
}
