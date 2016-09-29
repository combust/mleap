package ml.bundle.dsl

import ml.bundle.NodeDef.NodeDef

/** Trait for read-only interface to a [[Node]].
  *
  * Use this trait when deserializing node objects.
  */
trait ReadableNode {
  /** Create a protobuf node definition.
    *
    * @return protobuf node definition
    */
  def bundleNode: NodeDef

  /** Get the unique name of the node.
    *
    * This name must be unique for an entire Bundle.ML model.
    *
    * @return name of the node
    */
  def name: String

  /** Get the shape of the node.
    *
    * The shape defines how to connect input/outputs field
    * data to the underlying models.
    *
    * @return shape of the node
    */
  def shape: ReadableShape
}

/** Trait for writable interface to a [[Node]].
  *
  * There are no write operations for a [[Node]].
  * This trait is just here for completeness.
  */
trait WritableNode extends ReadableNode { }

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
case class Node(override val name: String,
                override val shape: Shape) extends WritableNode {
  override def bundleNode: NodeDef = NodeDef(name = name, shape = Some(shape.bundleShape))
}
