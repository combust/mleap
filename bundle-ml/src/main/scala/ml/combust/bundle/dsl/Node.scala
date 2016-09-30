package ml.combust.bundle.dsl

import ml.bundle.NodeDef.NodeDef

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
  /** Create a protobuf node definition.
    *
    * @return protobuf node definition
    */
  def bundleNode: NodeDef = NodeDef(name = name, shape = Some(shape.bundleShape))
}
