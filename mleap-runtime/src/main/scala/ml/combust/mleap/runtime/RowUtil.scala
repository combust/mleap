package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, StructSelector}
import ml.combust.mleap.core.types.StructType

import scala.util.Try

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
                         selectors: Selector *): Try[Seq[RowSelector]] = {
    var i = 0
    selectors.foldLeft(Try(Seq[RowSelector]())) {
      case (trss, s) =>
        val rs = RowUtil.createRowSelector(schema, s).flatMap {
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
                        selector: Selector): Try[RowSelector] = selector match {
    case FieldSelector(name) =>
      schema.indexOf(name).flatMap(index => Try(r => r.get(index)))
    case StructSelector(fields) =>
      schema.indicesOf(fields: _*).map {
        indices =>
          val indicesArr = indices
          (r: Row) => Row(indicesArr.map(r.get): _*)
      }
  }
}
