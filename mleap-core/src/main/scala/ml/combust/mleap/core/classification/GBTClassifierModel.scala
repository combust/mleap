package ml.combust.mleap.core.classification

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.TreeEnsemble
import org.apache.spark.ml.linalg.mleap.BLAS
import org.apache.spark.ml.linalg.{Vector, Vectors}

object GBTClassifierModel {
  def apply(trees: Seq[DecisionTreeRegressionModel],
            threshold: Option[Double] = Some(0.0),
            numFeatures: Int): GBTClassifierModel = {
    GBTClassifierModel(trees,
      Array.fill[Double](trees.length)(1.0),
      threshold,
      numFeatures)
  }
}

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTClassifierModel(override val trees: Seq[DecisionTreeRegressionModel],
                              override val treeWeights: Seq[Double],
                              override val threshold: Option[Double],
                              numFeatures: Int) extends BinaryClassificationModel with TreeEnsemble with Serializable {
  private val treeWeightsVector = Vectors.dense(treeWeights.toArray)

  override def predictProbability(features: Vector): Double = {
    val treePredictions = Vectors.dense(trees.map(_.predict(features)).toArray)
    BLAS.dot(treePredictions, treeWeightsVector)
  }
}
