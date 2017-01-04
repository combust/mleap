package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 12/18/16.
  */
case class ImputerModel(imputeValue: Double, missingValue: Option[Double], strategy: String) extends Serializable{
  def apply(value: Double): Double = {
    if (value.isNaN || missingValue.exists( _ == value)) imputeValue else value
  }
}