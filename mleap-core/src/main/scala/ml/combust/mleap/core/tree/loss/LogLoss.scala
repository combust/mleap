package ml.combust.mleap.core.tree.loss

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.linalg.LinalgUtils

/**
  * Class for log loss calculation (for classification).
  * This uses twice the binomial negative log likelihood, called "deviance" in Friedman (1999).
  *
  * The log loss is defined as:
  *   2 log(1 + exp(-2 y F(x)))
  * where y is a label in {-1, 1} and F(x) is the model prediction for features x.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.2/mllib/src/main/scala/org/apache/spark/mllib/tree/loss/Loss.scala")
object LogLoss extends ClassificationLoss {

  /**
    * Method to calculate the loss gradients for the gradient boosting calculation for binary
    * classification
    * The gradient with respect to F(x) is: - 4 y / (1 + exp(2 y F(x)))
    *
    * @param prediction Predicted label.
    * @param label True label.
    * @return Loss gradient
    */
  override def gradient(prediction: Double, label: Double): Double = {
    -4.0 * label / (1.0 + math.exp(2.0 * label * prediction))
  }

  override def computeError(prediction: Double, label: Double): Double = {
    val margin = 2.0 * label * prediction
    // The following is equivalent to 2.0 * log(1 + exp(-margin)) but more numerically stable.
    2.0 * LinalgUtils.log1pExp(-margin)
  }

  /**
    * Returns the estimated probability of a label of 1.0.
    */
  override def computeProbability(margin: Double): Double = {
    1.0 / (1.0 + math.exp(-2.0 * margin))
  }
}
