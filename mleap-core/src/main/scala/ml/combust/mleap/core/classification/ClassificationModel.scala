package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.Vector

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

/** Trait for classification models.
  *
  * This trait handles multinomial classification models as well as
  * binary classification models.
  */
trait MultinomialClassificationModel extends ClassificationModel {
  /** Number of classes this model predicts.
    *
    * 2 indicates this is a binary classification model.
    * Greater than 2 indicates a multinomial classifier.
    */
  val numClasses: Int

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

  def rawToProbabilityInPlace(raw: Vector): Vector

  def predictRaw(features: Vector): Vector
}

/** Trait for binary classifiers.
  *
  * This is only used for binary classifiers.
  * See [[MultinomialClassificationModel]] for multinomial classifiers.
  */
trait BinaryClassificationModel extends ClassificationModel {
  /** Threshold for binary classifiers.
    *
    * If the prediction probability is over this value, then
    * the prediction is pegged to 1.0. Otherwise the prediction
    * is pegged to 0.0.
    */
  val threshold: Option[Double] = None

  /** Predict the class taking into account threshold.
    *
    * @param features features for prediction
    * @return prediction with threshold
    */
  def predict(features: Vector): Double = {
    threshold match {
      case Some(t) => if (predictProbability(features) > t) 1.0 else 0.0
      case None => predictProbability(features)
    }
  }

  /** Predict the class without taking into account threshold.
    *
    * @param features features for prediction
    * @return probability that prediction is the predictable class
    */
  def predictProbability(features: Vector): Double
}
