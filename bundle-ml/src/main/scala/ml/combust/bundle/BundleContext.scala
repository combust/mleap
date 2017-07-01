package ml.combust.bundle

import java.nio.file.{FileSystem, Path}

import ml.combust.bundle.serializer.SerializationFormat

/** Class for holding serialization information for a [[ml.combust.bundle.dsl.Bundle]].
  *
  * This class holds all contextual information for serializing components into Bundle.ML.
  *
  * @param format desired serialization format (Json, Protobuf, or Mixed)
  * @param bundleRegistry bundle registry of all supported operations
  * @param fs file system for bundle
  * @param path path to the Bundle.ML model
  * @tparam Context extra contextual information specific to implementation
  */
case class BundleContext[Context](context: Context,
                                  format: SerializationFormat,
                                  bundleRegistry: BundleRegistry,
                                  fs: FileSystem,
                                  path: Path) extends HasBundleRegistry {
  /** Create a new bundle context for a subfolder.
    *
    * @param file name of subfolder
    * @return bundle context for the subfolder
    */
  def bundleContext(file: String): BundleContext[Context] = copy(path = fs.getPath(path.toString, file))

  /** Get a file in the current bundle folder.
    *
    * @param name name of the file
    * @return file in the bundle
    */
  def file(name: String): Path = fs.getPath(path.toString, name)
}
