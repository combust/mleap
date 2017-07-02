package ml.combust.mleap.bundle.tree.clustering

import ml.bundle.ctree.ctree.Node
import ml.combust.bundle.tree.cluster.NodeWrapper
import ml.combust.mleap.core.clustering.ClusteringTreeNode
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.mleap.VectorWithNorm

/**
  * Created by hollinwilkins on 12/27/16.
  */
object MleapNodeWrapper extends NodeWrapper[ClusteringTreeNode] {
  override def node(n: ClusteringTreeNode): Node = {
    Node(index = n.index,
      norm = n.centerWithNorm.norm,
      values = n.centerWithNorm.vector.toArray.toSeq,
      numChildren = n.children.length)
  }

  override def children(n: ClusteringTreeNode): Array[ClusteringTreeNode] = n.children

  override def create(node: Node, children: Seq[ClusteringTreeNode]): ClusteringTreeNode = {
    ClusteringTreeNode(index = node.index,
      centerWithNorm = VectorWithNorm(Vectors.dense(node.values.toArray), node.norm),
      children = children.toArray)
  }
}
