package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, StructSelector, UserDefinedFunction}
import ml.combust.mleap.core.types._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 10/30/16.
  */
object RowUtil {
  /** Create row selectors for a given schema and inputs.
    *
    * @param schema schema for inputs
    * @param selectors selectors for row
    * @return row selectors
    */
  def createRowSelectors(schema: StructType,
                         selectors: Selector *)
                        (udf: UserDefinedFunction): Try[Seq[RowSelector]] = {
    var i = 0
    selectors.foldLeft(Try(Seq[RowSelector]())) {
      case (trss, s) =>
        val rs = RowUtil.createRowSelector(schema, s, udf.inputs(i)).flatMap {
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
    * @return row selector
    */
  def createRowSelector(schema: StructType,
                        selector: Selector,
                        typeSpec: TypeSpec): Try[RowSelector] = selector match {
    case FieldSelector(name) =>
      schema.indexedField(name).flatMap {
        case (index, field) =>
          val dt = typeSpec.asInstanceOf[DataTypeSpec].dt
          if(field.dataType.needsCast(dt)) {
            Casting.cast(field.dataType, dt).map {
              c => (r: Row) => c(r.get(index))
            }
          } else {
            Success((r: Row) => r.get(index))
          }
      }
    case StructSelector(fields) =>
      schema.indexedFields(fields: _*).flatMap {
        fields =>
          val dts = typeSpec.asInstanceOf[SchemaSpec].dts
          Try {
            dts.zip(fields).map {
              case (expDt, (index, field)) =>
                if (field.dataType.needsCast(expDt)) {
                  Casting.cast(field.dataType, expDt).map {
                    c => (r: Row) => c(r.get(index))
                  }.get
                } else {
                  (r: Row) => r.get(index)
                }
            }
          }.map {
            selectors =>
              (r: Row) => Row(selectors.map(s => s(r)): _*)
          }
      }
  }
}
