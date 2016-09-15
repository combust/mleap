package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.types.StructType

/** Class for storing a leap frame locally.
  *
  * @param schema schema of leap frame
  * @param dataset dataset of leap frame
  */
case class DefaultLeapFrame(schema: StructType, dataset: Dataset) extends LeapFrame[DefaultLeapFrame] {
  /** Get this as underlying implementation.
    *
    * @return this as underlying implementation
    */
  override protected def lf: DefaultLeapFrame = this

  override protected def withSchemaAndDataset(schema: StructType,
                                              dataset: Dataset): DefaultLeapFrame = {
    copy(schema = schema, dataset = dataset)
  }
}
