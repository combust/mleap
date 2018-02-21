package ml.combust.mleap.core.tree

import ml.combust.mleap.core.annotation.SparkCode
import breeze.linalg.Vector

/** Trait for a split logic.
  *
  * Normally used for splits in a decision tree.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Split.scala")
sealed trait Split extends Serializable {
  /** Index of the feature to split on.
    *
    * @return index of split feature
    */
  def featureIndex: Int

  /** Whether to go left or not.
    *
    * @param features features for split
    * @return true if features go left, false otherwise
    */
  def shouldGoLeft(features: Vector[Double]): Boolean

  /** Whether to go left or not.
    *
    * Utilized binned features as an optimization.
    *
    * @param binnedFeature binned features
    * @param splits array of splits
    * @return true if binned features for left, false otherwise
    */
  def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean
}

/** Class for splitting on a categorical feature.
  *
  * @param featureIndex index of the features
  * @param numCategories number of potential categories
  * @param categories array of categories
  * @param isLeft if this split goes left
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Split.scala")
final case class CategoricalSplit(featureIndex: Int,
                                  numCategories: Int,
                                  categories: Array[Double],
                                  isLeft: Boolean) extends Split {
  override def shouldGoLeft(features: Vector[Double]): Boolean = {
    if(isLeft) {
      categories.contains(features(featureIndex))
    } else {
      !categories.contains(features(featureIndex))
    }
  }

  override def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if(isLeft) {
      categories.contains(binnedFeature.toDouble)
    } else {
      !categories.contains(binnedFeature.toDouble)
    }
  }
}

/** Class for splitting on a continuous feature.
  *
  * @param featureIndex index of the features
  * @param threshold threshold for going left or right
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Split.scala")
final case class ContinuousSplit(featureIndex: Int,
                                 threshold: Double) extends Split {
  override def shouldGoLeft(features: Vector[Double]): Boolean = features(featureIndex) <= threshold

  override def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if(binnedFeature == splits.length) {
      false
    } else {
      val featureUpperBound = splits(binnedFeature).asInstanceOf[ContinuousSplit].threshold
      featureUpperBound <= threshold
    }
  }
}
