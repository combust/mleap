package ml.combust.mleap.core.classification

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}

/** Trait for all classification models.
  */
trait ClassificationModel {
  /** Alias for [[ml.combust.mleap.core.classification.ClassificationModel#predict]].
    *
    * @param features feature vector
    * @return prediction
    */
  def apply(features: Vector): Double = predict(features)

  /** Predict class based on feature vector.
    *
    * @param features feature vector
    * @return predicted class or probability
    */
  def predict(features: Vector): Double
}

@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/ProbabilisticClassifier.scala")
object MultinomialClassificationModel {
  def normalizeToProbabilitiesInPlace(v: DenseVector): Unit = {
    val sum = v.values.sum
    if (sum != 0) {
      var i = 0
      val size = v.size
      while (i < size) {
        v.values(i) /= sum
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
trait MultinomialClassificationModel extends ClassificationModel {
  /** Number of classes this model predicts.
    *
    * 2 indicates this is a binary classification model.
    * Greater than 2 indicates a multinomial classifier.
    */
  val numClasses: Int

  def thresholds: Option[Array[Double]] = None

  def predict(features: Vector): Double = predictProbabilities(features).argmax.toDouble
  def predictWithProbability(features: Vector): (Double, Double) = {
    val probabilities = predictProbabilities(features)
    val index = probabilities.argmax
    (index.toDouble, probabilities(index))
  }

  def predictProbabilities(features: Vector): Vector = {
    val raw = predictRaw(features)
    rawToProbabilityInPlace(raw)
    raw
  }

  def rawToProbability(raw: Vector): Vector = {
    val probabilities = raw.copy
    rawToProbabilityInPlace(probabilities)
  }

  def rawToPrediction(raw: Vector): Double = {
    thresholds match {
      case Some(t) => probabilityToPrediction(rawToProbability(raw))
      case None => raw.argmax
    }
  }

  def probabilityToPrediction(probability: Vector): Double = {
    thresholds match {
      case Some(ts) =>
        val scaledProbability: Array[Double] =
          probability.toArray.zip(ts).map { case (p, t) =>
            if (t == 0.0) Double.PositiveInfinity else p / t
          }
        Vectors.dense(scaledProbability).argmax
      case None => probability.argmax
    }
  }

  def rawToProbabilityInPlace(raw: Vector): Vector

  def predictRaw(features: Vector): Vector
}

/** Trait for binary classifiers.
  *
  * This is only used for binary classifiers.
  * See [[MultinomialClassificationModel]] for multinomial classifiers.
  */
trait BinaryClassificationModel extends MultinomialClassificationModel {
  override val numClasses: Int = 2

  /** Threshold for binary classifiers.
    *
    * If the prediction probability is over this value, then
    * the prediction is pegged to 1.0. Otherwise the prediction
    * is pegged to 0.0.
    */
  val threshold: Option[Double] = None

  override lazy val thresholds: Option[Array[Double]] = threshold.map(t => Array[Double](1 - t, t))

  /** Predict the class taking into account threshold.
    *
    * @param features features for prediction
    * @return prediction with threshold
    */
  override def predict(features: Vector): Double = {
    binaryProbabilityToPrediction(predictBinaryProbability(features))
  }

  /** Predict the class without taking into account threshold.
    *
    * @param features features for prediction
    * @return probability that prediction is the predictable class
    */
  def predictBinaryProbability(features: Vector): Double

  /** Predict class and probability.
    *
    * @param features features to predict
    * @return (prediction, probability)
    */
  def predictBinaryWithProbability(features: Vector): (Double, Double) = {
    val probability = predictBinaryProbability(features)

    (binaryProbabilityToPrediction(probability), probability)
  }

  def binaryProbabilityToPrediction(probability: Double): Double = threshold match {
    case Some(t) => if (probability > t) 1.0 else 0.0
    case None => probability
  }
}
