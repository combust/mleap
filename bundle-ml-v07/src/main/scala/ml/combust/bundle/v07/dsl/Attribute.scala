package ml.combust.bundle.v07.dsl

import ml.combust.bundle.HasBundleRegistry

/** Companion object for attribute.
  */
object Attribute {
  /** Create DSL attribute from bundle attribute.
    *
    * @param attr bundle attribute
    * @param hr bundle registry for custom types
    * @return dsl attribute
    */
  def fromBundle(attr: ml.bundle.Attribute.Attribute)
                (implicit hr: HasBundleRegistry): Attribute = {
    Attribute(Value.fromBundle(attr.`type`.get, attr.value.get))
  }
}

/** Attribute class stores a named value.
  *
  * @param value stored value of the attribute
  */
case class Attribute(value: Value) {
  /** Convert to bundle attribute.
    *
    * @param hr bundle registry for custom types
    * @return bundle attribute
    */
  def asBundle(implicit hr: HasBundleRegistry): ml.bundle.Attribute.Attribute = {
    ml.bundle.Attribute.Attribute(`type` = Some(value.bundleDataType),
      value = Some(value.asBundle))
  }
}
