package ml.combust.bundle.serializer

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream

import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.dsl.{AttributeList, Bundle, BundleMeta}
import spray.json._
import resource._

import scala.io.Source

/** Class for serializing/deserializing Bundle.ML [[ml.combust.bundle.dsl.Bundle]] objects.
  *
  * @param context context for implementation
  * @param path path to the Bundle.ML folder/zip file to serialize/deserialize
  * @param hr bundle registry for custom types and ops
  * @tparam Context context type for implementation
  */
case class BundleSerializer[Context](context: Context,
                                     path: File)
                                    (implicit hr: HasBundleRegistry) {
  val tmp: File = new File(s"/tmp/bundle.ml/${java.util.UUID.randomUUID().toString}")

  /** Write a bundle to the path.
    *
    * @param bundle bundle to write
    */
  def write(bundle: Bundle): Unit = {
    BundleDirectorySerializer(context, tmp).write(bundle)

    if(path.getPath.endsWith(".zip")) {
      FileUtil().zip(tmp, path)
    } else {
      tmp.renameTo(path)
    }
  }

  /** Read a bundle from the path.
    *
    * @return deserialized bundle
    */
  def read(): Bundle = {
    if(path.getPath.endsWith(".zip")) {
      FileUtil().extract(path, tmp)
      BundleDirectorySerializer(context, tmp).read()
    } else {
      BundleDirectorySerializer(context, path).read()
    }
  }

  /** Read bundle definition from the path.
    *
    * @return bundle definition
    */
  def readMeta(): BundleMeta = {
    if(path.getPath.endsWith(".zip")) {
      (for(in <- managed(new ZipInputStream(new FileInputStream(path)))) yield {
        var meta: Option[BundleMeta] = None
        var entry = in.getNextEntry
        while(entry != null) {
          if(entry.getName == Bundle.bundleJson) {
            val json = Source.fromInputStream(in).getLines.mkString
            meta = Some(json.parseJson.convertTo[BundleMeta])
          }
          entry = in.getNextEntry
        }

        meta.getOrElse(throw new IllegalArgumentException("bundle zip does not contain bundle.json file"))
      }).either.either match {
        case Left(errors) => throw errors.head
        case Right(meta) => meta
      }
    } else {
      BundleDirectorySerializer(context, path).readMeta()
    }
  }
}

/** Class for serializing/deserializing Bundle.ML [[ml.combust.bundle.dsl.Bundle]] objects.
  *
  * @param context for implementation
  * @param path path to the Bundle.ML folder to serialize/deserialize
  * @param hr bundle registry for custom types and ops
  * @tparam Context context class for implementation
  */
case class BundleDirectorySerializer[Context](context: Context,
                                              path: File)
                                             (implicit hr: HasBundleRegistry) {
  val registry = hr.bundleRegistry

  /** Write a bundle to the path.
    *
    * @param bundle bundle to write
    */
  def write(bundle: Bundle): Unit = {
    val bundleContext = bundle.bundleContext(context, registry, path)
    implicit val sc = bundleContext.serializationContext(SerializationFormat.Json)

    bundleContext.path.mkdirs()
    GraphSerializer(bundleContext).write(bundle.nodes)
    val meta = bundle.meta

    new FileOutputStream(bundleContext.file(Bundle.bundleJson))
    for(out <- managed(new FileOutputStream(bundleContext.file(Bundle.bundleJson)))) {
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
      registry,
      path)
    implicit val sc = bundleContext.serializationContext(SerializationFormat.Json)

    val nodes = GraphSerializer(bundleContext).read(meta.nodes)
    Bundle(name = meta.name,
      format = meta.format,
      version = meta.version,
      attributes = meta.attributes,
      nodes = nodes)
  }

  /** Read bundle meta data from the path.
    *
    * @return bundle meta
    */
  def readMeta(): BundleMeta = {
    val bundleJson = new File(path, Bundle.bundleJson)
    (for(in <- managed(new FileInputStream(bundleJson))) yield {
      val json = Source.fromInputStream(in).getLines.mkString
      json.parseJson.convertTo[BundleMeta]
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(meta) => meta
    }
  }
}
