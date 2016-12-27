package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.{ArraySelector, FieldSelector, Selector}
import ml.combust.mleap.runtime.types.{AnyType, DataType, ListType, StructType}

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 10/30/16.
  */
object RowUtil {
  /** Create row selectors for a given schema and inputs.
    *
    * @param schema schema for inputs
    * @param inputs data type of desired outputs of selectors
    * @param selectors selectors for row
    * @return row selectors
    */
  def createRowSelectors(schema: StructType,
                         inputs: Seq[DataType],
                         selectors: Selector *): Try[Seq[RowSelector]] = {
    var i = 0
    selectors.foldLeft(Try(Seq[RowSelector]())) {
      case (trss, s) =>
        val rs = RowUtil.createRowSelector(schema, s, inputs(i)).flatMap {
          rs => trss.map(trs => rs +: trs)
        }
        i = i + 1
        rs
    }.map(_.reverse)
  }

  /** Create a row selector from a frame selector.
    *
    * @param schema schema for creating selectors
    * @param selector frame selector
    * @param dataType output data type of selector
    * @return row selector
    */
  def createRowSelector(schema: StructType,
                        selector: Selector,
                        dataType: DataType): Try[RowSelector] = selector match {
    case FieldSelector(name) =>
      schema.indexedField(name).flatMap {
        case (index, field) =>
          if (dataType.fits(field.dataType)) {
            Try(r => r.get(index))
          } else {
            Failure(new IllegalArgumentException(s"field $name data type ${field.dataType} does not match $dataType"))
          }
      }
    case ArraySelector(fields@_*) =>
      if (dataType == ListType(AnyType)) {
        schema.indicesOf(fields: _*).map {
          indices =>
            val indicesArr = indices
            r => indicesArr.map(r.get)
        }
      } else {
        Failure(new IllegalArgumentException(s"multiple field selector must be an Array[Any], found $dataType"))
      }
  }
}
