package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DataType, StructField, StructType}

import scala.util.{Failure, Success, Try}

/** Trait for a LeapFrame implementation.
  *
  * @tparam LF self-referential type
  */
trait LeapFrame[LF <: LeapFrame[LF]] extends TransformBuilder[LeapFrame[LF]] with Serializable {
  /** Get the schema.
    *
    * @return schema
    */
  def schema: StructType

  /** Get the dataset.
    *
    * @return dataset
    */
  def dataset: Dataset

  /** Try to select fields to create a new LeapFrame.
    *
    * Returns a Failure if attempting to select any fields that don't exist.
    *
    * @param fieldNames field names to select
    * @return try new LeapFrame with selected fields
    */
  def select(fieldNames: String *): Try[LF] = {
    schema.indicesOf(fieldNames: _*).flatMap {
      indices =>
        schema.selectIndices(indices: _*).map {
          schema2 =>
            val dataset2 = dataset.selectIndices(indices: _*)
            withSchemaAndDataset(schema2, dataset2)
        }
    }
  }

  /** Try to add a field to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param name name of field
    * @param dataType data type of field
    * @param f function for calculating field value
    * @return try new LeapFrame with new field
    */
  def withField(name: String, dataType: DataType)
               (f: (Row) => Any): Try[LF] = withField(StructField(name, dataType))(f)

  /** Try to add a field to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param field field to add
    * @param f function for calculating field value
    * @return try new LeapFrame with new field
    */
  def withField(field: StructField)
               (f: (Row) => Any): Try[LF] = {
    schema.withField(field).map {
      schema2 =>
        val dataset2 = dataset.withValue(f)
        withSchemaAndDataset(schema2, dataset2)
    }
  }

  /** Try to add multiple fields to the LeapFrame.
    *
    * Returns a Failure if trying to add any existing fields.
    *
    * @param fields fields to add
    * @param f function for calculating new field values
    * @return try new LeapFrame with new fields
    */
  def withFields(fields: Seq[StructField])
                (f: (Row) => Row): Try[LF] = {
    schema.withFields(fields).map {
      schema2 =>
        val dataset2 = dataset.withValues(f)
        withSchemaAndDataset(schema2, dataset2)
    }
  }

  /** Try to drop a field from the LeapFrame.
    *
    * Returns a Failure if the field does not exist.
    *
    * @param name name of field to drop
    * @return try new LeapFrame with field dropped
    */
  def dropField(name: String): Try[LF] = {
    schema.indexOf(name).flatMap {
      index =>
        schema.dropIndex(index).map {
          schema2 =>
            val dataset2 = dataset.dropIndex(index)
            withSchemaAndDataset(schema2, dataset2)
        }
    }
  }

  /** Creates a new instance of this LeapFrame with new schema and dataset.
    *
    * @param schema new schema
    * @param dataset new dataset
    * @return
    */
  protected def withSchemaAndDataset(schema: StructType, dataset: Dataset): LF

  override def withInput(name: String): Try[(LeapFrame[LF], Int)] = {
    schema.indexOf(name).map((this, _))
  }

  override def withInput(name: String, dataType: DataType): Try[(LeapFrame[LF], Int)] = {
    schema.indexedField(name).flatMap {
      case (index, field) =>
        if(field.dataType.fits(dataType)) {
          Success(this, index)
        } else {
          Failure(new Error(s"Field $name expected data type ${field.dataType} but found $dataType"))
        }
    }
  }

  override def withOutput(name: String, dataType: DataType)(o: (Row) => Any): Try[LeapFrame[LF]] = {
    withField(name, dataType)(o)
  }

  override def withOutputs(fields: Seq[StructField])(o: (Row) => Row): Try[LeapFrame[LF]] = {
    withFields(fields)(o)
  }
}
