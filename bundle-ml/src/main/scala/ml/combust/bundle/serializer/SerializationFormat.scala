package ml.combust.bundle.serializer

import ml.bundle.BundleDef.BundleDef.Format

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
sealed trait SerializationFormat {
  /** Get the Bundle.ML protobuf representation of the serialization format
    *
    * @return Protobuf representation of serialization format
    */
  def bundleFormat: Format
}

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

/** Companion object for holding the three [[ml.combust.bundle.serializer.SerializationFormat]] objects
  * as well as a helper method to convert to the protobuf serialization format enum.
  */
object SerializationFormat {
  def apply(format: Format): SerializationFormat = format match {
    case Format.MIXED => SerializationFormat.Mixed
    case Format.JSON => SerializationFormat.Json
    case Format.PROTOBUF => SerializationFormat.Protobuf
    case _ => throw new Error("unknown format") // TODO: better error
  }

  object Json extends ConcreteSerializationFormat {
    override def concrete: ConcreteSerializationFormat = Json
    override def bundleFormat: Format = Format.JSON
  }

  object Protobuf extends ConcreteSerializationFormat {
    override def concrete: ConcreteSerializationFormat = Protobuf
    override def bundleFormat: Format = Format.PROTOBUF
  }

  object Mixed extends SerializationFormat {
    override def bundleFormat: Format = Format.MIXED
  }
}
