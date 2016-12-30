package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 12/25/16.
  */
case class LabeledPoint(label: Double, features: Vector)
