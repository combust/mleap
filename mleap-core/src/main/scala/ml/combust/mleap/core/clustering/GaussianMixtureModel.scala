package ml.combust.mleap.core.clustering

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.mleap.Utils._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

/**
  * Created by hollinwilkins on 11/17/16.
  */
object GaussianMixtureModel {
  @SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.0/mllib/src/main/scala/org/apache/spark/ml/clustering/GaussianMixture.scala")
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
                                weights: Array[Double]) extends Model {
  val numClusters = gaussians.length
  val numFeatures: Int = weights.length

  def apply(features: Vector): Int = predict(features)

  def predict(features: Vector): Int = {
    predictionFromProbability(predictProbability(features))
  }

  def predictWithProbability(features: Vector): (Int, Double) = {
    val probability = predictProbability(features)
    val index = probability.argmax
    (index, probability(index))
  }

  def predictionFromProbability(probabilities: Vector): Int = {
    probabilities.argmax
  }

  def predictProbability(features: Vector): Vector = {
    val probs: Array[Double] = GaussianMixtureModel.computeProbabilities(features.toDense, gaussians, weights)
    Vectors.dense(probs)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Int,
    "probability" -> TensorType.Double(numClusters)).get
}
