package ml.combust.mleap.runtime

/** Class for holding local data as a [[ml.combust.mleap.runtime.Dataset]].
  *
  * @param data array of row data
  */
case class LocalDataset(data: Seq[Row]) extends Dataset {
  override def apply(index: Int): Row = data(index)

  override def update(f: (Row) => Row): LocalDataset = {
    LocalDataset(data = data.map(f))
  }

  override def toLocal: LocalDataset = this

  override def toSeq: Seq[Row] = data

  override def iterator: Iterator[Row] = data.iterator
}
