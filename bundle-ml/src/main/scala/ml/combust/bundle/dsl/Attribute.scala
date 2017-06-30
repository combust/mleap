package ml.combust.bundle.dsl

/** Companion object for attribute.
  */
object Attribute {
  /** Create DSL attribute from bundle attribute.
    *
    * @param attr bundle attribute
    * @return dsl attribute
    */
  def fromBundle(attr: ml.bundle.Attribute.Attribute): Attribute = {
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
    * @return bundle attribute
    */
  def asBundle: ml.bundle.Attribute.Attribute = {
    ml.bundle.Attribute.Attribute(`type` = Some(value.bundleDataType),
      value = Some(value.asBundle))
  }
}
