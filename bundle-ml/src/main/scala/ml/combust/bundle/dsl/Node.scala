package ml.combust.bundle.dsl

import ml.bundle.NodeDef.NodeDef

/** Companion object for node.
  */
object Node {
  /** Create a node from a bundle node.
    *
    * @param nodeDef bundle node definition
    * @return dsl node
    */
  def fromBundle(nodeDef: NodeDef): Node = {
    Node(name = nodeDef.name,
      shape = Shape.fromBundle(nodeDef.shape.get))
  }
}

/** Class for storing a node in the Bundle.ML graph.
  *
  * Bundle.ML is composed of a set of [[Node]] objects,
  * each with their own inputs and outputs defined by
  * their [[Node#shape]].
  *
  * Every [[Node]] needs a unique identifier within a Bundle.ML
  * graph. This is the [[Node#name]].
  *
  * @param name unique identifier for the node
  * @param shape shape of the node
  */
case class Node(name: String,
                shape: Shape) {
  /** Convert to a bundle node.
    *
    * @return bundle node definition
    */
  def asBundle: NodeDef = NodeDef(name = name,
    shape = Some(shape.asBundle))
}
