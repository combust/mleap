package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.{ArrayRow, Row, RowUtil}
import ml.combust.mleap.runtime.Row._
import ml.combust.mleap.runtime.function.{ArraySelector, FieldSelector, Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.types.{AnyType, DataType, ListType, StructType}

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 10/30/16.
  */
object RowTransformBuilder {
  def apply(schema: StructType): RowTransformBuilder = RowTransformBuilder(schema, schema, Array())
}

case class RowTransformBuilder private (inputSchema: StructType,
                                        outputSchema: StructType,
                                        transforms: Array[(ArrayRow) => ArrayRow]) extends TransformBuilder[RowTransformBuilder] {
  def arraySize: Int = outputSchema.fields.length

  override def withOutput(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[RowTransformBuilder] = {
    val index = outputSchema.fields.length

    RowUtil.createRowSelectors(outputSchema, udf.inputs, selectors: _*).flatMap {
      rowSelectors =>
        outputSchema.withField(name, udf.returnType).map {
          schema2 =>
            val transform = (row: ArrayRow) => row.set(index, row.udfValue(rowSelectors: _*)(udf))
            copy(outputSchema = schema2, transforms = transforms :+ transform)
        }
    }
  }

  /** Transform an input row with the predetermined schema.
    *
    * @param row row to transform
    * @return transformed row
    */
  def transform(row: Row): ArrayRow = {
    val arr = new Array[Any](arraySize)
    row.toArray.copyToArray(arr)
    val arrRow = ArrayRow(arr)

    transforms.foldLeft(arrRow) {
      (r, transform) => transform(r)
    }
  }
}
