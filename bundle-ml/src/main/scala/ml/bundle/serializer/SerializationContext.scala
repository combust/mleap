package ml.bundle.serializer

/** Class for holding concrete serialization format and bundle registry.
  *
  * This class is used by methods that need to know the exact serialization
  * format being used as well as a bundle registry for all model, nodes and
  * custom type type classes.
  *
  * @param concrete a concrete serialization format (JSON or Protobuf)
  * @param bundleRegistry registry of custom types and ops
  */
case class SerializationContext(override val concrete: ConcreteSerializationFormat,
                                override val bundleRegistry: BundleRegistry)
  extends HasConcreteSerializationFormat
  with HasBundleRegistry
