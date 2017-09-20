package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ScalarType, StructField, StructType}

/**
  * Created by mikhail on 12/18/16.
  */
case class ImputerModel(surrogateValue: Double,
                        missingValue: Double,
                        strategy: String,
                        nullableInput: Boolean = true) extends Model {
  def apply(value: Double): Double = {
    if(value.isNaN || value == missingValue) surrogateValue else value
  }

  def apply(value: java.lang.Double): Double = {
    Option(value).map(v => apply(v: Double)).getOrElse(surrogateValue)
  }

  override def inputSchema: StructType = StructType(StructField("input" -> ScalarType(BasicType.Double, nullableInput))).get

  override def outputSchema: StructType = StructType(StructField("output" -> ScalarType.Double.nonNullable)).get
}