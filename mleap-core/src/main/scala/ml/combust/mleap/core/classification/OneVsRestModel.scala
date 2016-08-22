package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.Vector

/** Class for multinomial one vs rest models.
  *
  * One vs rest models are comprised of a series of
  * [[BinaryClassificationModel]]s which are used to
  * predict each class.
  *
  * @param classifiers binary classification models
  */
case class OneVsRestModel(classifiers: Array[BinaryClassificationModel]) {
  /** Alias for [[ml.combust.mleap.core.classification.OneVsRestModel#predict]].
    *
    * @param features feature vector
    * @return prediction
    */
  def apply(features: Vector): Double = predict(features)

  /** Predict the class for a feature vector.
    *
    * @param features feature vector
    * @return predicted class
    */
  def predict(features: Vector): Double = {
    predictWithProbability(features)._1
  }

  /** Predict the class and probability for a feature vector.
    *
    * @param features feature vector
    * @return (predicted class, probability of class)
    */
  def predictWithProbability(features: Vector): (Double, Double) = {
    val (prediction, probability) = classifiers.zipWithIndex.map {
      case (c, i) =>
        val raw = c.predictProbability(features)
        (i.toDouble, raw)
    }.maxBy(_._2)

    (prediction, probability)
  }
}
