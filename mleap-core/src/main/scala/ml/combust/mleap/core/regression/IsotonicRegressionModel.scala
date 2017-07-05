package ml.combust.mleap.core.regression

import java.util.Arrays.binarySearch

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 12/27/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/regression/IsotonicRegression.scala")
case class IsotonicRegressionModel(boundaries: Array[Double],
                                   predictions: Seq[Double],
                                   isotonic: Boolean,
                                   featureIndex: Option[Int]) extends Model {
  def apply(features: Vector): Double = apply(features(featureIndex.get))

  def apply(feature: Double): Double = {
    val foundIndex = binarySearch(boundaries, feature)
    val insertIndex = -foundIndex - 1

    // Find if the index was lower than all values,
    // higher than all values, in between two values or exact match.
    if (insertIndex == 0) {
      predictions.head
    } else if (insertIndex == boundaries.length) {
      predictions.last
    } else if (foundIndex < 0) {
      linearInterpolation(
        boundaries(insertIndex - 1),
        predictions(insertIndex - 1),
        boundaries(insertIndex),
        predictions(insertIndex),
        feature)
    } else {
      predictions(foundIndex)
    }
  }

  private def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
    y1 + (y2 - y1) * (x - x1) / (x2 - x1)
  }
}
