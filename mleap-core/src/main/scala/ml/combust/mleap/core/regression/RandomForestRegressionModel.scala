package ml.combust.mleap.core.regression

import org.apache.spark.ml.linalg.Vector
import ml.combust.mleap.core.tree.TreeEnsemble

/** Companion object for constructing [[RandomForestRegressionModel]].
  */
object RandomForestRegressionModel {
  def apply(trees: Seq[DecisionTreeRegressionModel],
            numFeatures: Int): RandomForestRegressionModel = {
    RandomForestRegressionModel(trees,
      Array.fill[Double](trees.length)(1.0),
      numFeatures)
  }
}

/** Class for random forest regression.
  *
  * @param trees trees in the random forest
  * @param numFeatures number of features needed for prediction
  */
case class RandomForestRegressionModel(override val trees: Seq[DecisionTreeRegressionModel],
                                       override val treeWeights: Seq[Double],
                                       numFeatures: Int)
  extends TreeEnsemble with Serializable {
  /** Alias for [[ml.combust.mleap.core.regression.RandomForestRegressionModel#predict]].
    *
    * @param features feature for prediction
    * @return prediction
    */
  def apply(features: Vector): Double = predict(features)

  /** Predict a value with the forest.
    *
    * @param features features for prediction
    * @return prediction
    */
  def predict(features: Vector): Double = {
    trees.map(_.predict(features)).sum / numTrees
  }
}
