package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.UserDefinedFunction

/** Trait for storing data in a [[ml.combust.mleap.runtime.LeapFrame]].
  */
trait Dataset extends Serializable {
  /** Update all rows in the dataset.
    *
    * @param f update function
    * @return dataset with updated rows
    */
  def update(f: (Row) => Row): Dataset

  /** Add a value to every row using a user defined function.
    *
    * @param selectors row selectors to generate inputs to UDF
    * @param udf user defined function
    * @return dataset with value calculated for all rows
    */
  def withValue(selectors: RowSelector *)
               (udf: UserDefinedFunction): Dataset = update(_.withValue(selectors: _*)(udf))

  /** Select given indices of every row.
    *
    * @param indices indices to select
    * @return dataset with only selected indices
    */
  def selectIndices(indices: Int *): Dataset = update(_.selectIndices(indices: _*))

  /** Drop an index of every row.
    *
    * @param index index to drop
    * @return dataset without index
    */
  def dropIndex(index: Int): Dataset = update(_.dropIndex(index))

  /** Convert dataset to a local dataset.
    *
    * @return local dataset
    */
  def toLocal: LocalDataset

  /** Convert dataset to an array of rows.
    *
    * @return array of rows
    */
  def toArray: Array[Row] = toLocal.data
}
