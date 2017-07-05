package ml.combust.mleap.core.regression

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.tree.TreeEnsemble
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

/** Companion object for constructing [[GBTRegressionModel]].
  */
object GBTRegressionModel {
  def apply(trees: Seq[DecisionTreeRegressionModel],
            numFeatures: Int): GBTRegressionModel = {
    GBTRegressionModel(trees,
      Seq.fill[Double](trees.length)(1.0),
      numFeatures)
  }
}

/** Class for gradient boosted tree regression model.
  *
  * @param trees trees in model
  * @param treeWeights weight of each tree
  * @param numFeatures number of features
  */
case class GBTRegressionModel(override val trees: Seq[DecisionTreeRegressionModel],
                              override val treeWeights: Seq[Double],
                              numFeatures: Int) extends TreeEnsemble with Model {
  private val treeWeightsVector = Vectors.dense(treeWeights.toArray)

  /** Alias for [[ml.combust.mleap.core.regression.GBTRegressionModel#predict]]
    *
    * @param features features to predict
    * @return prediction
    */
  def apply(features: Vector): Double = predict(features)

  /** Make prediction based on feature vector.
    *
    * @param features features to predict
    * @return prediction
    */
  def predict(features: Vector): Double = {
    val predictions = Vectors.dense(trees.map(_.predict(features)).toArray)
    BLAS.dot(predictions, treeWeightsVector)
  }
}
