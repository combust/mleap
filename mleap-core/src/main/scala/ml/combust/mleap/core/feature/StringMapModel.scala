package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class StringMapModel(labels: Map[String, Double]) extends Model {
  def apply(label: String): Double = labels(label)

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable).get
}
