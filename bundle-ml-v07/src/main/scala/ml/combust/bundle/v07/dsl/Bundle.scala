package ml.combust.bundle.v07.dsl

import java.util.UUID

import ml.combust.bundle.v07.serializer._

sealed trait SerializationFormat
object SerializationFormat {
  case object Json extends SerializationFormat
  case object Protobuf extends SerializationFormat
  case object Mixed extends SerializationFormat
}

/** Information data for a bundle.
  *
  * @param uid uid for the bundle
  * @param name name of the bundle
  * @param format serialization format of the bundle
  * @param version Bundle.ML version used for serializing
  */
case class BundleInfo(uid: UUID,
                      name: String,
                      format: SerializationFormat,
                      version: String)
