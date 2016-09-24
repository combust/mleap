package ml.combust.mleap.core.regression

import ml.combust.mleap.core.tree.TreeEnsemble
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

object GBTRegressionModel {
  def apply(trees: Seq[DecisionTreeRegressionModel],
            numFeatures: Int): GBTRegressionModel = {
    GBTRegressionModel(trees,
      Array.fill[Double](trees.length)(1.0),
      numFeatures)
  }
}

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTRegressionModel(override val trees: Seq[DecisionTreeRegressionModel],
                              override val treeWeights: Seq[Double],
                              numFeatures: Int) extends TreeEnsemble with Serializable {
  private val treeWeightsVector = Vectors.dense(treeWeights.toArray)

  def apply(features: Vector): Double = predict(features)

  def predict(features: Vector): Double = {
    val predictions = Vectors.dense(trees.map(_.predict(features)).toArray)
    BLAS.dot(predictions, treeWeightsVector)
  }
}
