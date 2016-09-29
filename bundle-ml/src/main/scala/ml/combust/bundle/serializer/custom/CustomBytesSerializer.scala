package ml.combust.bundle.serializer.custom

/** Class for serializing a custom object to/from a byte array.
  *
  * @param ct custom type type class
  * @tparam T Scala class for custom type
  */
case class CustomBytesSerializer[T](ct: CustomType[T]) extends CustomSerializer[T] {
  /** Convert custom object to a byte array.
    *
    * Bytes can be in any format, but recommended to serialize using protobuf.
    *
    * @param obj object to serialize
    * @return serialized bytes
    */
  override def toBytes(obj: T): Array[Byte] = ct.toBytes(obj)

  /** Convert byte array to custom object.
    *
    * @param bytes bytes to read
    * @return deserialized custom object
    */
  override def fromBytes(bytes: Array[Byte]): T = ct.fromBytes(bytes)
}
