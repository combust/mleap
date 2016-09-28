package ml.combust.mleap.core.tree

/** Trait for an ensemble of decision trees.
  */
trait TreeEnsemble {
  /** Trees in the ensemble.
    *
    * @return trees in the ensemble model
    */
  def trees: Seq[DecisionTree]

  /** Weights for each tree.
    */
  def treeWeights: Seq[Double]

  /** Number of trees in the ensemble
    */
  def numTrees: Int = trees.length
}
