package ml.combust.mleap.core.tree

import ml.combust.mleap.core.annotation.SparkCode
import breeze.linalg.{Vector, DenseVector}

/** Trait for a node in a decision tree.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala")
sealed trait Node extends Serializable {
  def predictImpl(features: Vector[Double]): LeafNode
}

object LeafNode {
  def apply(values: Seq[Double]): LeafNode = LeafNode(values = DenseVector[Double](values.toArray))
  def apply(value: Double): LeafNode = LeafNode(values = DenseVector[Double](Array(value)))
}

/** Trait for a leaf node in a decision tree.
  *
  * @param values values vector of impurities or single prediction
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala")
final case class LeafNode(values: Vector[Double]) extends Node {
  override def predictImpl(features: Vector[Double]): LeafNode = this

  def prediction: Double = values(0)
  def impurities: Vector[Double] = values
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
  override def predictImpl(features: Vector[Double]): LeafNode = {
    if(split.shouldGoLeft(features)) {
      left.predictImpl(features)
    } else {
      right.predictImpl(features)
    }
  }
}
