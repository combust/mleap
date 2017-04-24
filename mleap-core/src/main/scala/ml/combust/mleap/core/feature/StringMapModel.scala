package ml.combust.mleap.core.feature

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class StringMapModel(labels: Map[String, Double]) extends Serializable {
  def apply(label: String): Double = labels(label)
}
