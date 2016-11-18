package ml.combust.mleap.core.clustering

import org.apache.spark.ml.linalg.mleap.Utils._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

/**
  * Created by hollinwilkins on 11/17/16.
  */
object GaussianMixtureModel {
  def computeProbabilities(features: DenseVector,
                           dists: Array[MultivariateGaussian],
                           weights: Array[Double]): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => EPSILON + weight * dist.pdf(features)
    }
    val pSum = p.sum
    var i = 0
    while (i < weights.length) {
      p(i) /= pSum
      i += 1
    }
    p
  }
}

case class GaussianMixtureModel(gaussians: Array[MultivariateGaussian],
                                weights: Array[Double]) {
  def apply(features: Vector): Int = predict(features)

  def predict(features: Vector): Int = {
    predictionFromProbability(predictProbability(features))
  }

  def predictionFromProbability(probabilities: Vector): Int = {
    probabilities.argmax
  }

  def predictProbability(features: Vector): Vector = {
    val probs: Array[Double] = GaussianMixtureModel.computeProbabilities(features.toDense, gaussians, weights)
    Vectors.dense(probs)
  }
}
