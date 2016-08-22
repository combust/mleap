package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.{Vector, Vectors}

/** Class for random forest classification models.
  *
  * @param trees trees of the random forest
  * @param numFeatures number of features in feature vector
  * @param numClasses number of predictable classes
  */
case class RandomForestClassifierModel(trees: Seq[DecisionTreeClassifierModel],
                                       numFeatures: Int,
                                       override val numClasses: Int) extends MultinomialClassificationModel with Serializable {
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
