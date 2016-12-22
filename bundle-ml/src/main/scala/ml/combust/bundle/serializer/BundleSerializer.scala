package ml.combust.bundle.serializer

import java.io.File
import java.net.URI
import java.nio.file.{FileSystem, FileSystems, Files, Path}

import ml.combust.bundle.{BundleContext, HasBundleRegistry}
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.dsl.{Bundle, BundleMeta}
import spray.json._
import resource._

import scala.collection.JavaConverters._
import scala.io.Source

object BundleSerializer {
  def apply[Context](context: Context,
                     uri: String)
                    (implicit hr: HasBundleRegistry): BundleSerializer[Context] = {
    apply(context, new URI(uri))
  }

  def apply[Context](context: Context,
                     file: File)
                    (implicit hr: HasBundleRegistry): BundleSerializer[Context] = {
    val uri = if(file.getPath.endsWith(".zip")) {
      new URI(s"jar:file:${file.getPath}")
    } else {
      new URI(s"file:$file")
    }

    apply(context, uri)
  }

  def apply[Context](context: Context,
                     uri: URI)
                    (implicit hr: HasBundleRegistry): BundleSerializer[Context] = {
    val env = Map("create" -> "true").asJava

    val (fs, path) = uri.getScheme match {
      case "file" =>
        (FileSystems.getDefault, FileSystems.getDefault.getPath(uri.getPath))
      case "jar" =>
        val zfs = FileSystems.newFileSystem(uri, env)
        (zfs, zfs.getPath("/"))
    }

    apply(context, fs, path)
  }
}

/** Class for serializing/deserializing Bundle.ML [[ml.combust.bundle.dsl.Bundle]] objects.
  *
  * @param context context for implementation
  * @param fs file system used for writing/reading
  * @param path path to the Bundle.ML folder/zip file to serialize/deserialize
  * @param hr bundle registry for custom types and ops
  * @tparam Context context type for implementation
  */
case class BundleSerializer[Context](context: Context,
                                     fs: FileSystem,
                                     path: Path)
                                    (implicit hr: HasBundleRegistry) {
  /** Write a bundle to the path.
    *
    * @param bundle bundle to write
    */
  def write(bundle: Bundle): Unit = {
    val bundleContext = bundle.bundleContext(context, hr.bundleRegistry, fs, path)
    implicit val sc = bundleContext.serializationContext(SerializationFormat.Json)

    Files.createDirectories(path)
    GraphSerializer(bundleContext).write(bundle.nodes)
    val meta = bundle.meta

    for(out <- managed(Files.newOutputStream(bundleContext.file(Bundle.bundleJson)))) {
      val json = meta.toJson.prettyPrint.getBytes
      out.write(json)
    }
  }

  /** Read a bundle from the path.
    *
    * @return deserialized bundle
    */
  def read(): Bundle = {
    val meta = readMeta()
    val bundleContext = BundleContext(context,
      meta.format,
      hr.bundleRegistry,
      fs,
      path)
    implicit val sc = bundleContext.serializationContext(SerializationFormat.Json)

    val nodes = GraphSerializer(bundleContext).read(meta.nodes)
    Bundle(name = meta.name,
      format = meta.format,
      version = meta.version,
      attributes = meta.attributes,
      nodes = nodes)
  }

  /** Read bundle definition from the path.
    *
    * @return bundle definition
    */
  def readMeta(): BundleMeta = {
    val bundleJson = fs.getPath(path.toString, Bundle.bundleJson)
    (for(in <- managed(Files.newInputStream(bundleJson))) yield {
      val json = Source.fromInputStream(in).getLines.mkString
      json.parseJson.convertTo[BundleMeta]
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(meta) => meta
    }
  }
}
