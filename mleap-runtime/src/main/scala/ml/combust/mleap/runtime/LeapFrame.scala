package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.{ArraySelector, FieldSelector, Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DataType, ListType, StructField, StructType}

import scala.util.{Failure, Try}

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

  /** Create a row selector from a frame selector.
    *
    * @param selector frame selector
    * @return row selector
    */
  def createRowSelector(selector: Selector, dataType: DataType): Try[RowSelector] = selector match {
    case FieldSelector(name) =>
      schema.indexedField(name).flatMap {
        case (index, field) =>
          if(dataType.fits(field.dataType)) {
            Try(r => r.get(index))
          } else {
            Failure(new IllegalArgumentException(s"field $name data type ${field.dataType} does not match $dataType"))
          }
      }
    case ArraySelector(fields @ _*) =>
      (for(base <- Try(dataType.asInstanceOf[ListType].base)) yield {
        var i = 0
        fields.foldLeft(Try(new Array[Int](fields.length))) {
          case (indices, name) =>
            schema.indexedField(name).flatMap {
              case (index, field) =>
                if (base.fits(field.dataType)) {
                  indices.map {
                    ids =>
                      ids(i) = index
                      i = i + 1
                      ids
                  }
                } else {
                  Failure(new IllegalArgumentException(s"field $name data type ${field.dataType} does not match $base"))
                }
            }
        }.map(indices => (r: Row) => indices.map(i => r.get(i)))
      }).flatMap(identity)
  }

  /** Try to add a field to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param name name of field
    * @param selectors row selectors used to generate inputs to udf
    * @param udf user defined function for calculating field value
    * @return try new LeapFrame with new field
    */
  def withField(name: String, selectors: Selector *)
               (udf: UserDefinedFunction): Try[LF] = {
    var i = 0
    selectors.foldLeft(Try(Seq[RowSelector]())) {
      case (trss, s) =>
        val rs = createRowSelector(s, udf.inputs(i)).flatMap {
          rs => trss.map(trs => rs +: trs)
        }
        i = i + 1
        rs
    }.flatMap {
      rowSelectors =>
        schema.withField(name, udf.returnType).map {
          schema2 =>
            val dataset2 = dataset.withValue(rowSelectors: _*)(udf)
            withSchemaAndDataset(schema2, dataset2)
        }
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

  override def withOutput(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[LF] = {
    withField(name, selectors: _*)(udf)
  }
}
