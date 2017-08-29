package ml.combust.mleap.core.tree.loss

import ml.combust.mleap.core.annotation.SparkCode

@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.2/mllib/src/main/scala/org/apache/spark/mllib/tree/loss/Loss.scala")
trait Loss extends Serializable {
  /**
    * Method to calculate the gradients for the gradient boosting calculation.
    *
    * @param prediction Predicted feature
    * @param label True label.
    * @return Loss gradient.
    */
  def gradient(prediction: Double, label: Double): Double

  /**
    * Method to calculate loss when the predictions are already known.
    *
    * @param prediction Predicted label.
    * @param label True label.
    * @return Measure of model error on datapoint.
    * @note This method is used in the method evaluateEachIteration to avoid recomputing the
    *       predicted values from previously fit trees.
    */
  protected def computeError(prediction: Double, label: Double): Double
}

trait ClassificationLoss extends Loss {
  /**
    * Computes the class probability given the margin.
    */
  def computeProbability(margin: Double): Double
}