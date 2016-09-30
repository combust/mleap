package ml.combust.mleap.core.clustering

import ml.combust.mleap.core.linalg.LinalgUtils
import org.apache.spark.ml.linalg.mleap.VectorWithNorm
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 9/30/16.
  */
object KMeansModel {
  def apply(clusterCenters: Array[Vector]): KMeansModel = {
    KMeansModel(clusterCenters.map(VectorWithNorm.apply))
  }
}
case class KMeansModel(clusterCenters: Array[VectorWithNorm]) {
  def clusterCount: Int = clusterCenters.length

  def apply(features: Vector): Int = predict(features)

  def predict(features: Vector): Int = {
    findClosest(VectorWithNorm(features))._1
  }

  private def findClosest(point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    clusterCenters.foreach {
      center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = LinalgUtils.fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }
}
