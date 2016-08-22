package ml.bundle.serializer

import java.io.File

/** Class for holding serialization information for a [[ml.bundle.dsl.Bundle]].
  *
  * This class holds all contextual information for serializing components into Bundle.ML.
  *
  * @param format desired serialization format (Json, Protobuf, or Mixed)
  * @param bundleRegistry bundle registry of all supported operations
  * @param path path to the Bundle.ML model
  */
case class BundleContext(format: SerializationFormat,
                         bundleRegistry: BundleRegistry,
                         path: File) extends HasBundleRegistry {
  /** Create a new bundle context for a subfolder.
    *
    * @param file name of subfolder
    * @return bundle context for the subfolder
    */
  def bundleContext(file: String): BundleContext = copy(path = new File(path, file))

  /** Get a file in the current bundle folder.
    *
    * @param name name of the file
    * @return file in the bundle
    */
  def file(name: String): File = new File(path, name)

  /** Create a serialization context.
    *
    * Serialization context objects have a concrete serialization format.
    * This concrete format determines how to serialize Bundle.ML components.
    *
    * @param concrete actual serialization format for a Bundle.ML component
    * @return serialization context with bundle registry and concrete serialization format
    */
  def serializationContext(concrete: ConcreteSerializationFormat): SerializationContext = SerializationContext(concrete, this.bundleRegistry)

  /** Get a concrete serialization format from a preferrece concrete format.
    *
    * If the Bundle.ML serialization format is Mixed, this will return the preferred concrete format.
    * Otherwise, it will return the Bundle.ML serialization format.
    *
    * This method is used to determine how to serialize certain objects that have a preference for serializing
    * to JSON or Protobuf. For instance, a [[ml.bundle.dsl.Model]] prefers to be serialized as JSON. However,
    * if the serialization format for the bundle is Protobuf, then it will be serialized as protobuf.
    *
    * @param preferred preferred concrete serialization format
    * @return actual concrete serialization format for object
    */
  def preferredSerializationContext(preferred: ConcreteSerializationFormat): SerializationContext = {
    val scFormat = format match {
      case SerializationFormat.Mixed => preferred
      case SerializationFormat.Json => SerializationFormat.Json
      case SerializationFormat.Protobuf => SerializationFormat.Protobuf
    }
    SerializationContext(scFormat, bundleRegistry)
  }
}
