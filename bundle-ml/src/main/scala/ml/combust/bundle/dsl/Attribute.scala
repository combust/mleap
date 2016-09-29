package ml.combust.bundle.dsl

import ml.combust.bundle.serializer.SerializationContext

/** Companion object for Attribute class.
  */
object Attribute {
  /** Construct an Attribute from a protobuf Attribute.
    *
    * @param attr protobuf attribute
    * @param context serialization context for decoding custom values
    * @return wrapped attribute
    */
  def apply(attr: ml.bundle.Attribute.Attribute)
           (implicit context: SerializationContext): Attribute = {
    Attribute(name = attr.name, value = Value.fromBundle(attr.`type`.get, attr.value.get))
  }
}

/** Attribute class stores a named value.
  *
  * @param name name of the value
  * @param value stored value of the attribute
  */
case class Attribute(name: String, value: Value) {
  /** Create the protobut attribute used for serialization.
    *
    * @param context serialization context for encoding custom values
    * @return protobuf attribute
    */
  def bundleAttribute(implicit context: SerializationContext): ml.bundle.Attribute.Attribute = {
    ml.bundle.Attribute.Attribute(name = name,
      `type` = Some(value.bundleDataType),
      value = Some(value.bundleValue))
  }
}
