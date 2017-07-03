package ml.combust.bundle.serializer

import ml.bundle.Format

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
  def asBundle: Format
}

/** Companion object for holding the three [[ml.combust.bundle.serializer.SerializationFormat]] objects
  * as well as a helper method to convert to the protobuf serialization format enum.
  */
object SerializationFormat {
  def fromBundle(format: Format): SerializationFormat = {
    if(format.isJson) {
      SerializationFormat.Json
    } else {
      SerializationFormat.Protobuf
    }
  }

  object Json extends SerializationFormat {
    override def asBundle: Format = Format.JSON
    override def toString: String = "json"
  }

  object Protobuf extends SerializationFormat {
    override def asBundle: Format = Format.PROTOBUF
    override def toString: String = "proto"
  }
}
