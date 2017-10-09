package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.Row.RowSelector
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, StructSelector, UserDefinedFunction}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 10/30/16.
  */
object IndexedRowUtil {
  /** Create row selectors for a given schema and inputs.
    *
    * @param schema schema for inputs
    * @param indices index lookup
    * @param selectors selectors for row
    * @return row selectors
    */
  def createRowSelectors(schema: StructType,
                         indices: Seq[Int],
                         selectors: Selector *)
                        (udf: UserDefinedFunction): Try[Seq[RowSelector]] = {
    var i = 0
    selectors.foldLeft(Try(Seq[RowSelector]())) {
      case (trss, s) =>
        val rs = IndexedRowUtil.createRowSelector(schema, indices, s, udf.inputs(i)).flatMap {
          rs => trss.map(trs => rs +: trs)
        }
        i = i + 1
        rs
    }.map(_.reverse)
  }

  /** Create a row selector from a frame selector.
    *
    * @param schema schema for creating selectors
    * @param indices index lookup
    * @param selector frame selector
    * @return row selector
    */
  def createRowSelector(schema: StructType,
                        indices: Seq[Int],
                        selector: Selector,
                        typeSpec: TypeSpec): Try[RowSelector] = selector match {
    case FieldSelector(name) =>
      schema.indexedField(name).flatMap {
        case (rawIndex, field) =>
          val index = indices(rawIndex)
          val dt = typeSpec.asInstanceOf[DataTypeSpec].dt
          Casting.cast(field.dataType, dt).map {
            _.map {
              c => (r: Row) => c(r.getRaw(index))
            }
          }.getOrElse {
            Success((r: Row) => r.getRaw(index))
          }
      }
    case StructSelector(fields) =>
      schema.indexedFields(fields: _*).flatMap {
        fields =>
          val dts = typeSpec.asInstanceOf[SchemaSpec].dts
          Try {
            dts.zip(fields).map {
              case (expDt, (rawIndex, field)) =>
                val index = indices(rawIndex)
                Casting.cast(field.dataType, expDt).map(_.get).map {
                  c => (r: Row) => c(r.getRaw(index))
                }.getOrElse {
                  (r: Row) => r.getRaw(index)
                }
            }
          }.map {
            selectors =>
              (r: Row) => Row(selectors.map(s => s(r)): _*)
          }
      }
  }
}
