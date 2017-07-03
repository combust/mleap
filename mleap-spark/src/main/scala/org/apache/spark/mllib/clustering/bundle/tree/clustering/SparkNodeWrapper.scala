package org.apache.spark.mllib.clustering.bundle.tree.clustering

import ml.bundle.ctree.Node
import ml.combust.bundle.tree.cluster.NodeWrapper
import org.apache.spark.mllib.clustering.{ClusteringTreeNode, VectorWithNorm}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 12/27/16.
  */
object SparkNodeWrapper extends NodeWrapper[ClusteringTreeNode] {
  override def node(n: ClusteringTreeNode): Node = {
    Node(index = n.index,
      norm = n.centerWithNorm.norm,
      values = n.centerWithNorm.vector.toArray.toSeq,
      numChildren = n.children.length)
  }

  override def children(n: ClusteringTreeNode): Array[ClusteringTreeNode] = n.children

  override def create(node: Node, children: Seq[ClusteringTreeNode]): ClusteringTreeNode = {
    new ClusteringTreeNode(index = node.index,
      size = 0,
      centerWithNorm = new VectorWithNorm(Vectors.dense(node.values.toArray), node.norm),
      cost = 0.0,
      height = 0,
      children = children.toArray)
  }
}
