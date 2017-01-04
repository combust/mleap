package ml.combust.mleap.core.classification

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.TreeEnsemble
import org.apache.spark.ml.linalg.mleap.BLAS
import org.apache.spark.ml.linalg.{Vector, Vectors}

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
  * @param trees trees in the gradient boost model
  * @param treeWeights weights of each tree
  * @param numFeatures number of features
  */
case class GBTClassifierModel(override val trees: Seq[DecisionTreeRegressionModel],
                              override val treeWeights: Seq[Double],
                              numFeatures: Int) extends TreeEnsemble with Serializable {
  private val treeWeightsVector = Vectors.dense(treeWeights.toArray)

  def apply(features: Vector): Double = if(predictProbability(features) > 0.0) 1.0 else 0.0

  def predictProbability(features: Vector): Double = {
    val treePredictions = Vectors.dense(trees.map(_.predict(features)).toArray)
    BLAS.dot(treePredictions, treeWeightsVector)
  }
}
