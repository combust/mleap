package ml.combust.mleap.core.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import breeze.linalg.{Vector, DenseVector, sum, argmax}

/** Trait for all classification models.
  */
trait ClassificationModel extends Model {
  /** Alias for [[ml.combust.mleap.core.classification.ClassificationModel#predict]].
    *
    * @param features feature vector
    * @return prediction
    */
  def apply(features: Vector[Double]): Double = predict(features)

  /** Predict class based on feature vector.
    *
    * @param features feature vector
    * @return predicted class or probability
    */
  def predict(features: Vector[Double]): Double
}

@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/ProbabilisticClassifier.scala")
object ProbabilisticClassificationModel {
  def normalizeToProbabilitiesInPlace(v: DenseVector[Double]): Unit = {
    val s = sum(v)
    if (s != 0) {
      var i = 0
      val size = v.size
      while (i < size) {
        v(i) /= s
        i += 1
      }
    }
  }
}

/** Trait for classification models.
  *
  * This trait handles multinomial classification models as well as
  * binary classification models.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/classification/ProbabilisticClassifier.scala")
trait ProbabilisticClassificationModel extends ClassificationModel {
  /** Number of classes this model predicts.
    *
    * 2 indicates this is a binary classification model.
    * Greater than 2 indicates a multinomial classifier.
    */
  val numClasses: Int

  val numFeatures: Int

  def thresholds: Option[Array[Double]] = None

  def predict(features: Vector[Double]): Double = probabilityToPrediction(predictProbabilities(features))
  def predictWithProbability(features: Vector[Double]): (Double, Double) = {
    val probabilities = predictProbabilities(features)
    val index = probabilityToPredictionIndex(probabilities)
    (index.toDouble, probabilities(index))
  }

  def predictProbabilities(features: Vector[Double]): Vector[Double] = {
    val raw = predictRaw(features)
    rawToProbabilityInPlace(raw)
    raw
  }

  def rawToProbability(raw: Vector[Double]): Vector[Double] = {
    val probabilities = raw.copy
    rawToProbabilityInPlace(probabilities)
  }

  def rawToPrediction(raw: Vector[Double]): Double = {
    thresholds match {
      case Some(_) => probabilityToPrediction(rawToProbability(raw))
      case None => argmax(raw)
    }
  }

  def probabilityToPrediction(probability: Vector[Double]): Double = {
    probabilityToPredictionIndex(probability).toDouble
  }

  def probabilityToPredictionIndex(probability: Vector[Double]): Int = {
    thresholds match {
      case Some(ts) =>
        val scaledProbability: Array[Double] =
          probability.toArray.zip(ts).map { case (p, t) =>
            if (t == 0.0) Double.PositiveInfinity else p / t
          }
        argmax(DenseVector[Double](scaledProbability))
      case None => argmax(probability)
    }
  }

  def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double]

  def predictRaw(features: Vector[Double]): Vector[Double]

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("raw_prediction" -> TensorType.Double(numClasses),
    "probability" -> TensorType.Double(numClasses),
    "prediction" -> ScalarType.Double.nonNullable).get
}
