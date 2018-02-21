package ml.combust.mleap.core.classification

import ml.combust.mleap.core.tree.{DecisionTree, Node}
import breeze.linalg.{DenseVector, SparseVector, Vector}

/** Class for decision tree classification models.
  *
  * @param rootNode root decision tree node
  * @param numFeatures number of features used in prediction
  * @param numClasses number of predictable classes
  */
case class DecisionTreeClassifierModel(override val rootNode: Node,
                                       numFeatures: Int,
                                       override val numClasses: Int)
  extends ProbabilisticClassificationModel with DecisionTree with Serializable {
  override def predictRaw(features: Vector[Double]): Vector[Double] = {
    rootNode.predictImpl(features).impurities
  }

  override def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double] = {
    raw match {
      case dv: DenseVector[_] =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv.asInstanceOf[DenseVector[Double]])
        dv
      case _: SparseVector[_] =>
        throw new RuntimeException("Unexpected error in RandomForestClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }
}
