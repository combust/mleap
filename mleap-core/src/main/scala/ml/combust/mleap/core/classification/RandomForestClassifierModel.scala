package ml.combust.mleap.core.classification

import ml.combust.mleap.core.tree.TreeEnsemble
import org.apache.spark.ml.linalg.{Vector, Vectors}

object RandomForestClassifierModel {
  def apply(trees: Seq[DecisionTreeClassifierModel],
            numFeatures: Int,
            numClasses: Int): RandomForestClassifierModel = {
    RandomForestClassifierModel(trees,
      Array.fill[Double](trees.length)(1.0),
      numFeatures,
      numClasses)
  }
}

/** Class for random forest classification models.
  *
  * @param trees trees of the random forest
  * @param numFeatures number of features in feature vector
  * @param numClasses number of predictable classes
  */
case class RandomForestClassifierModel(override val trees: Seq[DecisionTreeClassifierModel],
                                       override val treeWeights: Seq[Double],
                                       numFeatures: Int,
                                       override val numClasses: Int)
  extends MultinomialClassificationModel with TreeEnsemble with Serializable {
  override def predictRaw(raw: Vector): Vector = {
    val votes = Array.fill[Double](numClasses)(0.0)
    trees.view.foreach { tree =>
      val classCounts: Array[Double] = tree.rootNode.predictImpl(raw).impurities.get.toArray
      val total = classCounts.sum
      if (total != 0) {
        var i = 0
        while (i < numClasses) {
          votes(i) += classCounts(i) / total
          i += 1
        }
      }
    }
    Vectors.dense(votes)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = raw
}
