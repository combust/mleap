package ml.combust.mleap.bundle.tree

import ml.combust.mleap.core.tree
import ml.bundle.tree.Split.Split
import ml.bundle.tree.Split.Split.{CategoricalSplit, ContinuousSplit}
import ml.bundle.tree.Node.Node
import ml.bundle.tree.Node.Node.{InternalNode, LeafNode}
import ml.combust.bundle.tree.NodeWrapper
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapNodeWrapper extends NodeWrapper[ml.combust.mleap.core.tree.Node] {
  override def node(node: ml.combust.mleap.core.tree.Node, withImpurities: Boolean): Node = node match {
    case node: ml.combust.mleap.core.tree.InternalNode =>
      val split = node.split match {
        case split: ml.combust.mleap.core.tree.CategoricalSplit =>
          Split(Split.S.Categorical(CategoricalSplit(featureIndex = split.featureIndex,
            isLeft = split.isLeft,
            numCategories = split.numCategories,
            categories = split.categories)))
        case split: ml.combust.mleap.core.tree.ContinuousSplit =>
          Split(Split.S.Continuous(ContinuousSplit(featureIndex = split.featureIndex,
            threshold = split.threshold)))
      }
      Node(Node.N.Internal(Node.InternalNode(Some(split))))
    case node: ml.combust.mleap.core.tree.LeafNode =>
      val impurities = if(withImpurities) {
        node.impurities.get.toArray.toSeq
      } else { Seq() }
      Node(Node.N.Leaf(Node.LeafNode(node.prediction, impurities)))
  }

  override def isInternal(node: ml.combust.mleap.core.tree.Node): Boolean = node.isInstanceOf[ml.combust.mleap.core.tree.InternalNode]

  override def leaf(node: LeafNode, withImpurities: Boolean): ml.combust.mleap.core.tree.Node = {
    val impurities = if(withImpurities) {
      Some(Vectors.dense(node.impurities.toArray))
    } else {
      None
    }

    tree.LeafNode(prediction = node.prediction,
      impurities = impurities)
  }

  override def internal(node: InternalNode,
                        left: ml.combust.mleap.core.tree.Node,
                        right: ml.combust.mleap.core.tree.Node): ml.combust.mleap.core.tree.Node = {
    val bundleSplit = node.split.get
    val split = if(bundleSplit.s.isCategorical) {
      val s = bundleSplit.getCategorical
      tree.CategoricalSplit(featureIndex = s.featureIndex,
        isLeft = s.isLeft,
        numCategories = s.numCategories,
        categories = s.categories.toArray)
    } else if(bundleSplit.s.isContinuous) {
      val s = bundleSplit.getContinuous
      tree.ContinuousSplit(featureIndex = s.featureIndex,
        threshold = s.threshold)
    } else { throw new IllegalArgumentException("invalid split") }

    tree.InternalNode(split = split,
      left = left,
      right = right)
  }

  override def left(node: ml.combust.mleap.core.tree.Node): ml.combust.mleap.core.tree.Node = node match {
    case node: ml.combust.mleap.core.tree.InternalNode => node.left
    case _ => throw new IllegalArgumentException("not an internal node")
  }

  override def right(node: ml.combust.mleap.core.tree.Node): ml.combust.mleap.core.tree.Node = node match {
    case node: ml.combust.mleap.core.tree.InternalNode => node.right
    case _ => throw new IllegalArgumentException("not an internal node")
  }
}
