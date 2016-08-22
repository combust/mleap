package ml.combust.mleap.core.feature

/** Class for a reverse string indexer model.
  *
  * This model reverses the [[StringIndexerModel]] model.
  * Use this to go from an integer representation of a label to a string.
  *
  * @param labels labels for reverse string indexing
  */
case class ReverseStringIndexerModel(labels: Seq[String]) {
  private val indexToString: Map[Int, String] = labels.zipWithIndex.map(v => (v._2, v._1)).toMap

  /** Map an index to its string representation.
    *
    * @param index index to reverse index
    * @return string representation of index
    */
  def apply(index: Int): String = indexToString(index)
}
