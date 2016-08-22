package ml.bundle.serializer.custom

/** Class for serializing a custom object to JSON.
  *
  * @param ct custom type type class
  * @tparam T custom type Scala class
  */
case class CustomJsonSerializer[T](ct: CustomType[T]) extends CustomSerializer[T] {
  /** Convert custom object to byte array JSON.
    *
    * @param obj object to serialize
    * @return byte array containing JSON
    */
  override def toBytes(obj: T): Array[Byte] = ct.toJsonBytes(obj)

  /** Convert JSON byte array to custom object.
    *
    * @param bytes JSON bytes
    * @return deserialized custom object
    */
  override def fromBytes(bytes: Array[Byte]): T = ct.fromJsonBytes(bytes)
}
