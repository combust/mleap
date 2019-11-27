package ml.combust.mleap.core.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

/** Class for multinomial one vs rest models.
  *
  * One vs rest models are comprised of a series of
  * [[ClassificationModel]]s which are used to
  * predict each class.
  *
  * @param classifiers binary classification models
  */
case class OneVsRestModel(classifiers: Array[ClassificationModel],
                          numFeatures: Int) extends Model {
  /** Alias for [[ml.combust.mleap.core.classification.OneVsRestModel#predict]].
    *
    * @param features feature vector
    * @return prediction
    */
  def apply(features: Vector): Double = predictAll(features)._3

  /** Predict the class, probability and raw predictions for a feature vector.
    *
    * @param features feature vector
    * @return (predicted class, probability of class)
    */
  def predictAll(features: Vector): (Double, Vector, Double) = {
    val predArray = Array.fill[Double](classifiers.length)(0.0)
    val (prediction, probability) = classifiers.zipWithIndex.map {
      case (c:ProbabilisticClassificationModel, i) =>
        val raw = c.predictRaw(features)
        predArray(i) = raw(1)
        val probability = c.rawToProbabilityInPlace(raw)(1)

        (i.toDouble, probability)
      case (c,i) =>
        val raw = c.predict(features)
        predArray(i) = raw
        (i.toDouble,raw)

    }.maxBy(_._2)

    (probability, Vectors.dense(predArray), prediction)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("probability" -> ScalarType.Double,
    "raw_prediction" -> TensorType.Double(classifiers.length),
    "prediction" -> ScalarType.Double).get
}
