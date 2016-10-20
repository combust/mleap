package ml.combust.mleap.runtime

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
    * @param indices input indices to the udf
    * @param udf user defined function
    * @return dataset with value calculated for all rows
    */
  def withValue(indices: Array[Int])(udf: UserDefinedFunction): Dataset = update(_.withValue(indices)(udf))

  /** Add a value to every row.
    *
    * @param f function used to calculate value for a row
    * @return dataset with new value
    */
  def withValue(f: (Row) => Any): Dataset = update(_.withValue(f))

  /** Add values to every row.
    *
    * @param f function used to calculate additional values for every row
    * @return dataset with new values
    */
  def withValues(f: (Row) => Row): Dataset = update(_.withValues(f))

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
