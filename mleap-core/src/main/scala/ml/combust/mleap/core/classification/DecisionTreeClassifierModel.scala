package ml.combust.mleap.core.classification

import ml.combust.mleap.core.tree.{DecisionTree, Node}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

/** Class for decision tree classification models.
  *
  * @param rootNode root decision tree node
  * @param numFeatures number of features used in prediction
  * @param numClasses number of predictable classes
  */
case class DecisionTreeClassifierModel(override val rootNode: Node,
                                       numFeatures: Int,
                                       override val numClasses: Int,
                                       override val thresholds: Option[Array[Double]] = None)
  extends ProbabilisticClassificationModel with DecisionTree with Serializable {
  override def predictRaw(features: Vector): Vector = {
    rootNode.predictImpl(features).impurities
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in DecisionTreeClassifierModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }
}
