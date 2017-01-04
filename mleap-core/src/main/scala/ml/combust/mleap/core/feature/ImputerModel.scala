package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 12/18/16.
  */
case class ImputerModel(imputeValue: Double, missingValue: Option[Double], strategy: String) extends Serializable {
  def predictAny(value: Any): Double = value match {
    case value: Double => apply(value)
    case value: Option[_] => apply(value.asInstanceOf[Option[Double]])
  }

  def apply(value: Double): Double = {
    if(value.isNaN || missingValue.exists( _ == value)) imputeValue else value
  }

  def apply(value: Option[Double]): Double = value match {
    case `missingValue` => imputeValue
    case Some(v) => v
    case None => imputeValue
  }
}