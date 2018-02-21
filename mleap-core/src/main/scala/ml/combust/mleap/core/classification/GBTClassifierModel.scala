package ml.combust.mleap.core.classification

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.TreeEnsemble
import ml.combust.mleap.core.tree.loss.LogLoss
import breeze.linalg.{DenseVector, SparseVector, Vector}

/** Companion object for constructing [[GBTClassifierModel]].
  */
object GBTClassifierModel {
  def apply(trees: Seq[DecisionTreeRegressionModel],
            numFeatures: Int): GBTClassifierModel = {
    apply(trees,
      Seq.fill[Double](trees.length)(1.0),
      numFeatures)
  }
}

/** Class for a gradient boost classifier model.
  *
  * @param trees       trees in the gradient boost model
  * @param treeWeights weights of each tree
  * @param numFeatures number of features
  */
case class GBTClassifierModel(override val trees: Seq[DecisionTreeRegressionModel],
                              override val treeWeights: Seq[Double],
                              numFeatures: Int,
                              override val thresholds: Option[Array[Double]] = None) extends ProbabilisticClassificationModel
  with TreeEnsemble
  with Serializable {
  private val treeWeightsVector = DenseVector[Double](treeWeights.toArray)
  // hard coded loss, which is not meant to be changed in the model
  private val loss = LogLoss

  // Multiclass labels are not currently supported
  override val numClasses: Int = 2

  override def predict(features: Vector[Double]): Double = {
    if (thresholds.nonEmpty) {
      super.predict(features)
    } else {
      if (margin(features) > 0.0) 1.0 else 0.0
    }
  }

  /** Raw prediction for the positive class. */
  def margin(features: Vector[Double]): Double = {
    val treePredictions = DenseVector[Double](trees.map(_.predict(features)).toArray)
    treePredictions.dot(treeWeightsVector)
  }

  override def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double] = {
    raw match {
      case _dv: DenseVector[_] =>
        val dv = _dv.asInstanceOf[DenseVector[Double]]
        dv(0) = loss.computeProbability(dv(0))
        dv(1) = 1.0 - dv(0)
        dv
      case _: SparseVector[_] => throw new RuntimeException("GBTClassificationModel encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector[Double]): Vector[Double] = {
    val prediction: Double = margin(features)
    DenseVector[Double](Array(-prediction, prediction))
  }
}
