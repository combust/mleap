package ml.combust.bundle.v07.dsl

/** Companion object for node.
  */
object Node {
  /** Create a node from a bundle node.
    *
    * @param node bundle node definition
    * @return dsl node
    */
  def fromBundle(node: ml.bundle.Node): Node = {
    Node(name = node.name,
      shape = NodeShape.fromBundle(node.shape.get))
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
                shape: NodeShape) {
  /** Convert to a bundle node.
    *
    * @return bundle node definition
    */
  def asBundle: ml.bundle.Node = ml.bundle.Node(name = name,
    shape = Some(shape.asBundle))
}
