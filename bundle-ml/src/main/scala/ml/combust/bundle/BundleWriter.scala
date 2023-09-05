package ml.combust.bundle

import java.net.URI
import java.nio.file.{Files, Paths}
import ml.combust.bundle.dsl.Bundle
  import ml.combust.bundle.serializer.{BundleSerializer, SerializationFormat}

import scala.util.{Try, Using}

/**
  * Created by hollinwilkins on 12/24/16.
  */
case class BundleWriter[Context <: HasBundleRegistry,
Transformer <: AnyRef](root: Transformer,
                       name: Option[String] = None,
                       format: SerializationFormat = SerializationFormat.Json,
                       meta: Option[ml.bundle.Attributes] = None) {
  def name(value: String): BundleWriter[Context, Transformer] = copy(name = Some(value))
  def format(value: SerializationFormat): BundleWriter[Context, Transformer] = copy(format = value)
  def meta(value: ml.bundle.Attributes): BundleWriter[Context, Transformer] = copy(meta = Some(value))

  def save(file: BundleFile)
          (implicit context: Context): Try[Bundle[Transformer]] = {
    val n = name.getOrElse {
      context.bundleRegistry.opForObj[Any, Any, Any](root).name(root)
    }

    BundleSerializer(context, file).write(Bundle(name = n,
      format = format,
      root = root,
      meta = meta))
  }

  def save(uri: URI)
          (implicit context: Context): Try[Bundle[Transformer]] = uri.getScheme match {
    case "jar" | "file" =>
        Using(BundleFile(uri)) { bf =>
          save(bf).get
        }
    case _ =>
    val tmpDir = Files.createTempDirectory("bundle")
    val tmp = Paths.get(tmpDir.toString, "tmp.zip")

    Using(BundleFile(tmp.toFile)) { bf =>
      save(bf).get
    }.map {
      r =>
        context.bundleRegistry.fileSystemForUri(uri).save(uri, tmp.toFile)
        r
    }
  }
}
