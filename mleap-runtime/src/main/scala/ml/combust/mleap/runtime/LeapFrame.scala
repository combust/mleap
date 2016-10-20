package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DataType, StructField, StructType}

import scala.util.{Failure, Success, Try}

object LeapFrame {
  def apply(schema: StructType, dataset: Dataset): DefaultLeapFrame = DefaultLeapFrame(schema, dataset)
}

/** Trait for a LeapFrame implementation.
  *
  * @tparam LF self-referential type
  */
trait LeapFrame[LF <: LeapFrame[LF]] extends TransformBuilder[LF] with Serializable {
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

  /** Get this as underlying implementation.
    *
    * @return this as underlying implementation
    */
  protected def lf: LF

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
    * @param fields input field names
    * @param udf user defined function for calculating field value
    * @return try new LeapFrame with new field
    */
  def withField(name: String, fields: String *)
               (udf: UserDefinedFunction): Try[LF] = {
    schema.withField(name, udf.returnType).flatMap {
      schema2 =>
        withInputs(fields.zip(udf.inputs)).map {
          case (frame, indices) =>
            val dataset2 = dataset.withValue(indices: _*)(udf)
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
    * @param field first field to add
    * @param fields fields to add
    * @param f function for calculating new field values
    * @return try new LeapFrame with new fields
    */
  def withFields(field: StructField, fields: StructField *)
                (f: (Row) => Row): Try[LF] = {
    withFields(field +: fields)(f)
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
    * @return new leap frame with schema and dataset
    */
  protected def withSchemaAndDataset(schema: StructType, dataset: Dataset): LF

  override def withInput(name: String, dataType: DataType): Try[(LF, Int)] = {
    schema.indexedField(name).flatMap {
      case (index, field) =>
        if(dataType.fits(field.dataType)) {
          Success(this.lf, index)
        } else {
          Failure(new IllegalArgumentException(s"field $name data type ${field.dataType} does not match $dataType"))
        }
    }
  }

  override def withInputs(fields: Seq[(String, DataType)]): Try[(LF, Seq[Int])] = {
    fields.foldLeft(Try((lf, Seq[Int]()))) {
      case (lfs, (name, dataType)) =>
        schema.indexedField(name).flatMap {
          case (index, field) =>
            if(dataType.fits(field.dataType)) {
              lfs.map {
                case (l, indices) => (lf, indices :+ index)
              }
            } else {
              Failure(new IllegalArgumentException(s"field $name data type ${field.dataType} does not match $dataType"))
            }
        }
    }
  }

  override def withOutput(name: String,
                          fields: String *)
                         (udf: UserDefinedFunction): Try[LF] = {
    withField(name, fields: _*)(udf)
  }

  override def withOutput(name: String, dataType: DataType)(o: (Row) => Any): Try[LF] = {
    withField(name, dataType)(o)
  }

  override def withOutputs(fields: Seq[StructField])(o: (Row) => Row): Try[LF] = {
    withFields(fields)(o)
  }
}
