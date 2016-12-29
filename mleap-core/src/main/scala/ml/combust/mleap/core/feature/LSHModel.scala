package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 12/28/16.
  */
trait LSHModel {
  def keyDistance(x: Vector, y: Vector): Double
  def hashDistance(x: Seq[Vector], y: Seq[Vector]): Double
}
