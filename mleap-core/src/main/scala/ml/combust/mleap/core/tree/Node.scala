package ml.combust.mleap.core.tree

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.Vector

/** Trait for a node in a decision tree.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala")
sealed trait Node extends Serializable {
  def predictImpl(features: Vector): LeafNode
}

/** Trait for a leaf node in a decision tree.
  *
  * @param prediction prediction for this leaf node
  * @param impurities options vector of impurities
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala")
final case class LeafNode(prediction: Double,
                          impurities: Option[Vector] = None) extends Node {
  override def predictImpl(features: Vector): LeafNode = this
}

/** Trait for internal node in a decision tree.
  *
  * @param left left child
  * @param right right child
  * @param split split logic to go left or right
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala")
final case class InternalNode(left: Node,
                              right: Node,
                              split: Split) extends Node {
  override def predictImpl(features: Vector): LeafNode = {
    if(split.shouldGoLeft(features)) {
      left.predictImpl(features)
    } else {
      right.predictImpl(features)
    }
  }
}
