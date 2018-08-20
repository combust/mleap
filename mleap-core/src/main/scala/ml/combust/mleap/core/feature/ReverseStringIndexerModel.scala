package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.Tensor

/** Class for a reverse string indexer model.
  *
  * This model reverses the [[StringIndexerModel]] model.
  *
  * Use this to go from an integer representation of a label to a string.
  * Alternatively, goes from a list of integers to a list of labels.
  *
  * @param labels labels for reverse string indexing
  * @param inputShape shape of the input, determines scalar/list output as well
  */
case class ReverseStringIndexerModel(labels: Seq[String],
                                     inputShape: DataShape = ScalarShape(false)) extends Model {
  require(inputShape.nonNullable, "cannot take null inputs")

  private val indexToString: Map[Int, String] = labels.zipWithIndex.map(v => (v._2, v._1)).toMap

  /** Map an index to its string representation.
    *
    * @param index index to reverse index
    * @return string representation of index
    */
  def apply(index: Int): String = indexToString(index)

  /** Map a list of indices to string representations.
    *
    * @param indices sequence of indices
    * @return sequence of labels
    */
  def apply(indices: Seq[Int]): Seq[String] = indices.map(indexToString)

  /** Map a tensor of indices to string representations.
    *
    * @param indices tensor of indices
    * @return tensor of label strings
    */
  def apply(indices: Tensor[Int]): Tensor[String] = indices.mapValues(indexToString)

  override def inputSchema: StructType = StructType("input" -> DataType(BasicType.Double, inputShape)).get

  override def outputSchema: StructType = StructType("output" -> DataType(BasicType.String, inputShape).asNullable).get
}
