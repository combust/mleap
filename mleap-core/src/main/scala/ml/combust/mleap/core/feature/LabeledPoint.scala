package ml.combust.mleap.core.feature

import breeze.linalg.Vector

/**
  * Created by hollinwilkins on 12/25/16.
  */
case class LabeledPoint(label: Double, features: Vector[Double])
