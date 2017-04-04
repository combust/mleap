package ml.combust.mleap.core.feature

/** Class for string indexer model.
  *
  * String indexer converts a string into an integer representation.
  *
  * @param labels list of labels that can be indexed
  */
case class StringIndexerModel(labels: Seq[String]) extends Serializable {
  private val stringToIndex: Map[String, Int] = labels.zipWithIndex.toMap

  /** Convert a string into its integer representation.
    *
    * @param value label to index
    * @return index of label
    */
  def apply(value: Any): Double = value match {
    case opt: Option[_] => stringToIndex(opt.get.toString)
    case _ => stringToIndex(value.toString)
  }

  /** Create a [[ml.combust.mleap.core.feature.ReverseStringIndexerModel]] from this model.
    *
    * @return reverse string indexer of this string indexer
    */
  def toReverse: ReverseStringIndexerModel = ReverseStringIndexerModel(labels)
}
