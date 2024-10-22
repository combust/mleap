package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}

/** Class for string indexer model.
  *
  * String indexer converts a string into an integer representation.
  *
  * @param labels list of labels that can be indexed
  * @param handleInvalid how to handle invalid values (unseen or NULL labels): 'error' (throw an error),
  *                      'skip' (skips invalid data)
  *                      or 'keep' (put invalid data in a special bucket at index labels.size
  */
case class StringIndexerModel(labels: Seq[Seq[String]],
                              handleInvalid: HandleInvalid = HandleInvalid.Error) extends Model {
  private val stringToIndex: Array[Map[String, Int]] = labels.map(_.zipWithIndex.toMap).toArray
  private val keepInvalid = handleInvalid == HandleInvalid.Keep
  private val invalidValue = labels.map(_.length)

  /** Convert all strings into its integer representation.
   *
   * @param values labels to index
   * @return indexes of labels
   */
  def apply(values: Seq[Any]): Seq[Double] = values.zipWithIndex.map {
    case (v: Any, i: Int) => encoder(v, i).toDouble
    case (null, i: Int) => encoder(null, i).toDouble
  }

  def contains(values: Seq[Any]): Boolean = {
    values.zipWithIndex.forall {
      case (key, i) => stringToIndex(i).contains(key.toString)
    }
 }
  /** Convert a string into its integer representation.
   *
   * @param value label to index
   * @return index of label
   */
  private def encoder(value: Any, colIdx: Int): Int = if (value == null) {
    if (keepInvalid) {
      invalidValue(colIdx)
    } else {
      throw new NullPointerException("StringIndexer encountered NULL value. " +
        s"To handle NULLS, set handleInvalid to ${HandleInvalid.Keep.asParamString}")
    }
  } else {
    val label = value.toString
    stringToIndex(colIdx).get(label) match {
      case Some(v) => v
      case None => if (keepInvalid) {
        invalidValue(colIdx)
      } else {
        throw new NoSuchElementException(s"Unseen label: $label. To handle unseen labels, " +
          s"set handleInvalid to ${HandleInvalid.Keep.asParamString}")
      }
    }
  }

  /** Create a [[ml.combust.mleap.core.feature.ReverseStringIndexerModel]] from this model.
    *
    * @return reverse string indexer of this string indexer
    */
//  def toReverse: ReverseStringIndexerModel = ReverseStringIndexerModel(labels)

  override def inputSchema: StructType = {
    val f = labels.zipWithIndex.map {
      case (_, i) => StructField(s"input$i", ScalarType.String)
    }
    StructType(f).get
  }

  override def outputSchema: StructType = {
    val f = labels.zipWithIndex.map {
      case (_, i) => StructField(s"output$i", ScalarType.Double.nonNullable)
    }
    StructType(f).get
  }
}
