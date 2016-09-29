package ml.combust.bundle.serializer.custom

/** Trait for a custom object serializer.
  *
  * @tparam T Scala class of custom object to serialize
  */
trait CustomSerializer[T] {
  /** Convert custom object to byte array.
    *
    * @param obj object to serialize
    * @return serialized bytes
    */
  def toBytes(obj: T): Array[Byte]

  /** Convert byte array to custom object.
    *
    * @param bytes bytes to deserialize
    * @return deserialized custom object
    */
  def fromBytes(bytes: Array[Byte]): T
}
