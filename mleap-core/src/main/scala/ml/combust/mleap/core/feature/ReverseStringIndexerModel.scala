package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/** Class for a reverse string indexer model.
  *
  * This model reverses the [[StringIndexerModel]] model.
  * Use this to go from an integer representation of a label to a string.
  *
  * @param labels labels for reverse string indexing
  */
case class ReverseStringIndexerModel(labels: Seq[String]) extends Model {
  private val indexToString: Map[Int, String] = labels.zipWithIndex.map(v => (v._2, v._1)).toMap

  /** Map an index to its string representation.
    *
    * @param index index to reverse index
    * @return string representation of index
    */
  def apply(index: Int): String = indexToString(index)

  override def inputSchema: StructType = StructType("input" -> ScalarType.Double).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.String).get
}
