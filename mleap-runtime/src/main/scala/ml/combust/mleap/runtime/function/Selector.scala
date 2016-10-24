package ml.combust.mleap.runtime.function

import scala.language.implicitConversions

/** Trait for a LeapFrame selector.
  *
  * A selector generates values based on other values found
  * in a [[ml.combust.mleap.runtime.Row]]. The name parameters
  * to a selector specifies which column of the row to get
  * the values from.
  *
  * Currently there are two supported selectors: a field selector and
  * and array selector.
  *
  * [[FieldSelector]] selects the value of a given field.
  * [[ArraySelector]] creates an array from the values of a given set of fields.
  */
sealed trait Selector

/** Companion object for selectors.
  *
  * Provides implicit conversions for convenience.
  */
object Selector {
  /** Create a [[FieldSelector]] for a given name.
    *
    * @param name name of field
    * @return field selector
    */
  implicit def apply(name: String): FieldSelector = FieldSelector(name)

  /** Create an [[ArraySelector]] for a given list of names.
    *
    * @param names fields names used to construct the array
    * @return array selector
    */
  implicit def apply(names: Array[String]): ArraySelector = ArraySelector(names: _*)
}

/** Class for a selector that extracts the value of a field from a [[ml.combust.mleap.runtime.Row]].
  *
  * @param field name of field to extract
  */
case class FieldSelector(field: String) extends Selector

/** Class for a selector that constructs an array from values in a [[ml.combust.mleap.runtime.Row]].
  *
  * @param fields names of fields used to construct array
  */
case class ArraySelector(fields: String *) extends Selector
