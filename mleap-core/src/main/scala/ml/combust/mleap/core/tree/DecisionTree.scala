package ml.combust.mleap.core.tree

/** Trait for a decision tree.
  */
trait DecisionTree extends Serializable {
  /** Root node of the decision tree.
    *
    * @return root node
    */
  def rootNode: Node
}
