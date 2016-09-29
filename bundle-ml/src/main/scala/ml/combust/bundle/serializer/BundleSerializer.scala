package ml.combust.bundle.serializer

import java.io.{File, FileInputStream, FileOutputStream}

import ml.bundle.BundleDef.BundleDef
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.dsl.{AttributeList, Bundle}
import spray.json._
import resource._

import scala.io.Source

/** Class for serializing/deserializing Bundle.ML [[ml.combust.bundle.dsl.Bundle]] objects.
  *
  * @param path path to the Bundle.ML folder to serialize/deserialize
  * @param hr bundle registry for custom types and ops
  */
case class BundleSerializer(path: File)
                           (implicit hr: HasBundleRegistry) {
  val registry = hr.bundleRegistry

  /** Write a bundle to the path.
    *
    * @param bundle bundle to write
    */
  def write(bundle: Bundle): Unit = {
    val context = bundle.bundleContext(registry, path)
    implicit val sc = context.serializationContext(SerializationFormat.Json)

    context.path.mkdirs()
    val names = GraphSerializer(context).write(bundle.nodes)
    val bundleDef = bundle.bundleDef(context).copy(nodes = names)

    new FileOutputStream(context.file(Bundle.bundleJson))
    for(out <- managed(new FileOutputStream(context.file(Bundle.bundleJson)))) {
      val json = bundleDef.toJson.prettyPrint.getBytes
      out.write(json)
    }
  }

  /** Read a bundle from the path.
    *
    * @return deserialized bundle
    */
  def read(): Bundle = {
    val bundleDef = readBundleDef()
    val context = BundleContext(SerializationFormat(bundleDef.format),
      registry,
      path)
    implicit val sc = context.serializationContext(SerializationFormat.Json)

    val nodes = GraphSerializer(context).read(bundleDef.nodes)

    Bundle(name = bundleDef.name,
      format = SerializationFormat(bundleDef.format),
      version = bundleDef.version,
      nodes = nodes,
      attributes = bundleDef.attributes.map(AttributeList.apply))
  }

  /** Read bundle definition from the path.
    *
    * @return bundle definition
    */
  def readBundleDef(): BundleDef = {
    val bundleJson = new File(path, Bundle.bundleJson)
    (for(in <- managed(new FileInputStream(bundleJson))) yield {
      val json = Source.fromInputStream(in).getLines.mkString
      json.parseJson.convertTo[BundleDef]
    }).opt.get
  }
}
