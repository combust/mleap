package ml.combust.bundle.tree.cluster

import ml.bundle.ctree.Node

/**
  * Created by hollinwilkins on 12/26/16.
  */
trait NodeWrapper[N] {
  def node(n: N): Node
  def children(n: N): Array[N]

  def create(node: Node,
             children: Seq[N]): N
}
