package org.apache.spark.ml.bundle.tree

import org.apache.spark.ml.tree
import ml.bundle.tree.Split.Split
import ml.bundle.tree.Split.Split.{CategoricalSplit, ContinuousSplit}
import ml.bundle.tree.Node.Node
import ml.bundle.tree.Node.Node.{InternalNode, LeafNode}
import ml.bundle.tree.NodeWrapper
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator

/**
  * Created by hollinwilkins on 8/22/16.
  */
object SparkNodeWrapper extends NodeWrapper[tree.Node] {
  override def node(node: tree.Node, withImpurities: Boolean): Node = node match {
    case node: tree.InternalNode =>
      val split = node.split match {
        case split: tree.CategoricalSplit =>
          val left = split.leftCategories
          val right = split.rightCategories
          val (isLeft, categories) = if(left.length < right.length) {
            (true, left)
          } else {
            (false, right)
          }
          Split(Split.S.Categorical(CategoricalSplit(featureIndex = split.featureIndex,
            isLeft = isLeft,
            numCategories = split.numCategories,
            categories = categories)))
        case split: tree.ContinuousSplit =>
          Split(Split.S.Continuous(ContinuousSplit(featureIndex = split.featureIndex,
            threshold = split.threshold)))
      }
      Node(Node.N.Internal(Node.InternalNode(split)))
    case node: tree.LeafNode =>
      val impurities = if(withImpurities) {
        node.impurityStats.stats
      } else { Array() }
      Node(Node.N.Leaf(Node.LeafNode(node.prediction, impurities)))
  }

  override def isInternal(node: tree.Node): Boolean = node.isInstanceOf[tree.InternalNode]

  override def leaf(node: LeafNode, withImpurities: Boolean): tree.Node = {
    val calc: ImpurityCalculator = if(withImpurities) {
      ImpurityCalculator.getCalculator("gini", node.impurities.toArray)
    } else {
      null
    }
    new tree.LeafNode(prediction = node.prediction,
      impurity = 0.0,
      impurityStats = calc)
  }

  override def internal(node: InternalNode,
                        left: tree.Node,
                        right: tree.Node): tree.Node = {
    val split = if(node.split.s.isCategorical) {
      val s = node.split.getCategorical
      val c = if(s.isLeft) {
        s.categories.toArray
      } else {
        ((0 to s.numCategories).map(_.toDouble).toSet -- s.categories).toArray
      }
      new tree.CategoricalSplit(featureIndex = s.featureIndex,
        numCategories = s.numCategories,
        _leftCategories = c)
    } else if(node.split.s.isContinuous) {
      val s = node.split.getContinuous
      new tree.ContinuousSplit(featureIndex = s.featureIndex,
        threshold = s.threshold)
    } else { throw new Error("invalid split") }

    new tree.InternalNode(split = split,
      leftChild = left,
      rightChild = right,
      prediction = 0.0,
      gain = 0.0,
      impurity = 0.0,
      impurityStats = null)
  }

  override def left(node: tree.Node): tree.Node = node match {
    case node: tree.InternalNode => node.leftChild
    case _ => throw new Error("not an internal node") // TODO: better error
  }

  override def right(node: tree.Node): tree.Node = node match {
    case node: tree.InternalNode => node.rightChild
    case _ => throw new Error("not an internal node") // TODO: better error
  }
}
