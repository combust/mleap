package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/** Class for string indexer model.
  *
  * String indexer converts a string into an integer representation.
  *
  * @param labels list of labels that can be indexed
  * @param handleInvalid how to handle invalid values (unseen or NULL labels): 'error' (throw an error),
  *                      'skip' (skips invalid data)
  *                      or 'keep' (put invalid data in a special bucket at index labels.size
  */
case class StringIndexerModel(labels: Seq[String],
                              handleInvalid: HandleInvalid = HandleInvalid.Error) extends Model {
  val stringToIndex: Map[String, Int] = labels.zipWithIndex.toMap
  private val keepInvalid = handleInvalid == HandleInvalid.Keep

  /** Convert a string into its integer representation.
    *
    * @param value label to index
    * @return index of label
    */
  def apply(value: Any): Int = if (value == null) {
    if (keepInvalid) {
      labels.length
    } else {
      throw new NullPointerException("StringIndexer encountered NULL value. " +
        s"To handle NULLS, set handleInvalid to ${HandleInvalid.Keep.asParamString}")
    }
  } else {
    val label = value.toString
    if (stringToIndex.contains(label)) {
      stringToIndex(label)
    } else if (keepInvalid) {
      labels.length
    } else {
      throw new NoSuchElementException(s"Unseen label: $label. To handle unseen labels, " +
        s"set handleInvalid to ${HandleInvalid.Keep.asParamString}")
    }
  }

  /** Create a [[ml.combust.mleap.core.feature.ReverseStringIndexerModel]] from this model.
    *
    * @return reverse string indexer of this string indexer
    */
  def toReverse: ReverseStringIndexerModel = ReverseStringIndexerModel(labels)

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable).get
}
