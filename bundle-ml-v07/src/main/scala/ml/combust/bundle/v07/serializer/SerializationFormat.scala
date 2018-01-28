package ml.combust.bundle.v07.serializer

/** Trait for defining which serialization format was used
  * to write a Bundle.ML model.
  *
  * Currently supported formats are:
  * <ol>
  *   <li>mixed - use JSON for smaller attributes, models and nodes and protobuf for large attributes, models and nodes</li>
  *   <li>json - used JSON for all attributes, models and nodes</li>
  *   <li>protobuf - use protobuf for all attributes, models and nodes</li>
  * </ol>
  */
sealed trait SerializationFormat

/** Trait for defining the actual serialization format being
  * used in a given context. This must be either JSON or Protobuf.
  */
sealed trait ConcreteSerializationFormat extends SerializationFormat with HasConcreteSerializationFormat

/** Trait inherited when a class has access to a concrete serialization format.
  */
trait HasConcreteSerializationFormat {
  /** Get the concrete serialization format.
    *
    * @return concrete serialization format contained by the object
    */
  def concrete: ConcreteSerializationFormat
}

/** Companion object for holding the three [[ml.combust.bundle.v07.serializer.SerializationFormat]] objects
  * as well as a helper method to convert to the protobuf serialization format enum.
  */
object SerializationFormat {
  object Json extends ConcreteSerializationFormat {
    override def concrete: ConcreteSerializationFormat = Json
    override def toString: String = "json"
  }

  object Protobuf extends ConcreteSerializationFormat {
    override def concrete: ConcreteSerializationFormat = Protobuf
    override def toString: String = "proto"
  }

  object Mixed extends SerializationFormat {
    override def toString: String = "mixed"
  }
}
