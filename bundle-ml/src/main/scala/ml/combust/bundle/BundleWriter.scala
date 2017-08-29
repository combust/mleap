package ml.combust.bundle

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.serializer.{BundleSerializer, SerializationFormat}

import scala.util.Try

/**
  * Created by hollinwilkins on 12/24/16.
  */
case class BundleWriter[Context <: HasBundleRegistry,
Transformer <: AnyRef](root: Transformer,
                       name: Option[String] = None,
                       format: SerializationFormat = SerializationFormat.Json) {
  def name(value: String): BundleWriter[Context, Transformer] = copy(name = Some(value))
  def format(value: SerializationFormat): BundleWriter[Context, Transformer] = copy(format = value)

  def save(file: BundleFile)
          (implicit context: Context): Try[Bundle[Transformer]] = {
    val n = name.getOrElse {
      context.bundleRegistry.opForObj[Any, Any, Any](root).name(root)
    }

    BundleSerializer(context, file).write(Bundle(name = n,
      format = format,
      root = root))
  }
}
