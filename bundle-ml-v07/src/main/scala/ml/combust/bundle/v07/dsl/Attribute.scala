package ml.combust.bundle.v07.dsl

/** Companion object for attribute.
  */
object Attribute {
  /** Create DSL attribute from bundle attribute.
    *
    * @param attr bundle attribute
    * @return dsl attribute
    */
  def fromBundle(attr: ml.bundle.v07.Attribute): Attribute = {
    Attribute(Value.fromBundle(attr.`type`.get, attr.value.get))
  }
}

/** Attribute class stores a named value.
  *
  * @param value stored value of the attribute
  */
case class Attribute(value: Value)
