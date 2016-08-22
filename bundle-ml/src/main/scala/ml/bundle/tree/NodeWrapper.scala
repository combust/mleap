package ml.bundle.tree

/** Type class for decision-tree node-like objects.
  *
  * This type class is used to convert decision tree nodes
  * into the appropriate Bundle.ML node representations
  * for serializing and deserializing.
  *
  * @tparam N node type that can be serialized to Bundle.ML
  */
trait NodeWrapper[N] {
  /** Convert node to the Bundle.ML node for serialization.
    *
    * @param node node to convert
    * @param withImpurities flag to include impurities or not
    * @return Bundle.ML node
    */
  def node(node: N, withImpurities: Boolean): Node.Node

  /** Whether or not node is internal.
    *
    * @param node decision tree node
    * @return true if internal node, false otherwise
    */
  def isInternal(node: N): Boolean

  /** Whether or not node is a leaf node.
    *
    * @param node decision tree node
    * @return true if leaf, false otherwise
    */
  def isLeaf(node: N): Boolean = !isInternal(node)

  /** Get the left node for an internal node.
    *
    * @param node internal decision tree node
    * @return left child of the node
    */
  def left(node: N): N

  /** Get the right child for an internal node.
    *
    * @param node internal decision tree node
    * @return right child of the node
    */
  def right(node: N): N

  /** Create a leaf node from a Bundle.ML leaf node.
    *
    * @param node Bundle.ML leaf node
    * @param withImpurities whether to include impurities in leaf nodes
    * @return leaf node
    */
  def leaf(node: Node.Node.LeafNode, withImpurities: Boolean): N

  /** Create an internal node from a Bundle.ML internal node.
    *
    * @param node Bundle.ML internal node
    * @param left left child of internal node
    * @param right right child of internal node
    * @return internal node
    */
  def internal(node: Node.Node.InternalNode,
               left: N,
               right: N): N
}
