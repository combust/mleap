package ml.combust.mleap.core.classification

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.TreeEnsemble
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import ml.combust.mleap.core.tree.loss.LogLoss
import org.apache.spark.ml.linalg.mleap.BLAS
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

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
  private val treeWeightsVector = Vectors.dense(treeWeights.toArray)
  // hard coded loss, which is not meant to be changed in the model
  private val loss = LogLoss

  // Multiclass labels are not currently supported
  override val numClasses: Int = 2

  override def predict(features: Vector): Double = {
    if (thresholds.nonEmpty) {
      super.predict(features)
    } else {
      if (margin(features) > 0.0) 1.0 else 0.0
    }
  }

  /** Raw prediction for the positive class. */
  def margin(features: Vector): Double = {
    val treePredictions = Vectors.dense(trees.map(_.predict(features)).toArray)
    BLAS.dot(treePredictions, treeWeightsVector)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double).get

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        dv.values(0) = loss.computeProbability(dv.values(0))
        dv.values(1) = 1.0 - dv.values(0)
        dv
      case sv: SparseVector => throw new RuntimeException("GBTClassificationModel encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector): Vector = {
    val prediction: Double = margin(features)
    Vectors.dense(Array(-prediction, prediction))
  }
}
