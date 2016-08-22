package ml.combust.mleap.runtime

/** Class for holding local data as a [[ml.combust.mleap.runtime.Dataset]].
  *
  * @param data array of row data
  */
case class LocalDataset(data: Array[Row]) extends Dataset {
  /** Get a row at a given index.
    *
    * @param index index of row
    * @return row at index
    */
  def apply(index: Int): Row = data(index)

  override def update(f: (Row) => Row): LocalDataset = {
    LocalDataset(data = data.map(f))
  }

  override def toLocal: LocalDataset = this

  override def toArray: Array[Row] = data
}
