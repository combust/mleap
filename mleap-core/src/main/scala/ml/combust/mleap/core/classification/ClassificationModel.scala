package ml.combust.mleap.core.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}

/** Trait for all classification models.
  */
trait ClassificationModel extends Model {
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
object ProbabilisticClassificationModel {
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
trait ProbabilisticClassificationModel extends ClassificationModel {
  /** Number of classes this model predicts.
    *
    * 2 indicates this is a binary classification model.
    * Greater than 2 indicates a multinomial classifier.
    */
  val numClasses: Int

  val numFeatures: Int

  def thresholds: Option[Array[Double]] = None

  def predict(features: Vector): Double = probabilityToPrediction(predictProbabilities(features))
  def predictWithProbability(features: Vector): (Double, Double) = {
    val probabilities = predictProbabilities(features)
    val index = probabilityToPredictionIndex(probabilities)
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
    probabilityToPredictionIndex(probability).toDouble
  }

  def probabilityToPredictionIndex(probability: Vector): Int = {
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

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("raw_prediction" -> TensorType.Double(numClasses),
    "probability" -> TensorType.Double(numClasses),
    "prediction" -> ScalarType.Double.nonNullable).get
}
