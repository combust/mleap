package ml.combust.bundle.v07.dsl

import java.util.UUID

import ml.combust.bundle.v07.serializer._

/** Information data for a bundle.
  *
  * @param uid uid for the bundle
  * @param name name of the bundle
  * @param format serialization format of the [[Bundle]]
  * @param version Bundle.ML version used for serializing
  */
case class BundleInfo(uid: UUID,
                      name: String,
                      format: SerializationFormat,
                      version: String)
