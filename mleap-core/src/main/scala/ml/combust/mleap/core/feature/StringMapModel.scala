package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class StringMapModel(labels: Map[String, Double]) extends Model {
  def apply(label: String): Double = labels(label)
}
