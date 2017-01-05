package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 12/18/16.
  */
case class ImputerModel(surrogateValue: Double, missingValue: Double, strategy: String) extends Serializable {
  def predictAny(value: Any): Double = value match {
    case value: Double => apply(value)
    case value: Option[_] => apply(value.asInstanceOf[Option[Double]])
  }

  def apply(value: Double): Double = {
    if(value.isNaN || value == missingValue) surrogateValue else value
  }

  def apply(value: Option[Double]): Double = value match {
    case Some(v) => apply(v)
    case None => surrogateValue
  }
}