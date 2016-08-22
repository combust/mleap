package ml.combust.mleap.core.tree

/** Trait for an ensemble of decision trees.
  */
trait TreeEnsemble extends Serializable {
  /** Trees in the ensemble.
    *
    * @return trees in the ensemble model
    */
  def trees: Seq[DecisionTree]

  /** Weights for each tree. Unused currently.
    */
  def treeWeights: Seq[Double]
}
