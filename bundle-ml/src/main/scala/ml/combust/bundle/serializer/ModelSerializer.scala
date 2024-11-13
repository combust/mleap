package ml.combust.bundle.serializer

import java.nio.file.{Files, Path}

import ml.combust.bundle.{BundleContext, HasBundleRegistry}
import ml.combust.bundle.dsl.{Bundle, Model}
import ml.combust.bundle.json.JsonSupport._
import spray.json._

import scala.util.Try

/** Trait for serializing a protobuf model definition.
  */
trait FormatModelSerializer {
  /** Write a protobuf model definition.
    *
    * @param path path to write to
    * @param model model to write
    */
  def write(path: Path, model: Model): Unit

  /** Read a protobuf model definition.
    *
    * @param path path to read from
    * @return model that was read
    */
  def read(path: Path): Model
}

/** Companion object for utility methods related to model definition serialization.
  */
object FormatModelSerializer {
  /** Get the appropriate JSON or Protobuf serializer based on the serialization context.
    *
    * @param bc bundle context
    * @return JSON or Protobuf model definition serializer depending on serialization context
    */
  def serializer[T](implicit bc: BundleContext[T]): FormatModelSerializer = bc.format match {
    case SerializationFormat.Json => JsonFormatModelSerializer()
    case SerializationFormat.Protobuf => ProtoFormatModelSerializer()
  }
}

/** Object for serializing/deserializing model definitions with JSON.
  */
case class JsonFormatModelSerializer()(implicit hr: HasBundleRegistry) extends FormatModelSerializer {
  override def write(path: Path, model: Model): Unit = {
    Files.write(path, model.asBundle.toJson.prettyPrint.getBytes("UTF-8"))
  }

  override def read(path: Path): Model = {
    Model.fromBundle(new String(Files.readAllBytes(path), "UTF-8").parseJson.convertTo[ml.bundle.Model])
  }
}

/** Object for serializing/deserializing model definitions with Protobuf.
  */
case class ProtoFormatModelSerializer()(implicit hr: HasBundleRegistry) extends FormatModelSerializer {
  override def write(path: Path, model: Model): Unit = {
    Files.write(path, model.asBundle.toByteArray)
  }

  override def read(path: Path): Model = {
    Model.fromBundle(ml.bundle.Model.parseFrom(Files.readAllBytes(path)))
  }
}

/** Class for serializing Bundle.ML models.
  *
  * @param bundleContext bundle context for path and bundle registry
  * @tparam Context context class for implementation
  */
case class ModelSerializer[Context](bundleContext: BundleContext[Context]) {
  private implicit val bc = bundleContext
  private implicit val format = bundleContext.format

  /** Write a model to the current context path.
    *
    * This will write the model to the current path in the [[BundleContext]].
    *
    * @param obj model to write
    */
  def write(obj: Any): Try[Any] = Try {
    Files.createDirectories(bundleContext.path)
    val m = bundleContext.bundleRegistry.modelForObj[Context, Any](obj)
    val model = Model(op = m.modelOpName(obj))
    m.store(model, obj)(bundleContext)
  }.flatMap {
    model => Try(FormatModelSerializer.serializer.write(bundleContext.file(Bundle.modelFile), model))
  }

  /** Read a model from the current bundle context.
    *
    * @return deserialized model
    */
  def read(): Try[Any] = readWithModel().map(_._1)

  /** Read a model and return it along with its type class.
    *
    * @return (deserialized model, model type class)
    */
  def readWithModel(): Try[(Any, Model)] = {
    Try(FormatModelSerializer.serializer.read(bundleContext.file(Bundle.modelFile))).map {
      bundleModel =>
        val om = bundleContext.bundleRegistry.model[Context, Any](bundleModel.op)
        (om.load(bundleModel), bundleModel)
    }
  }
}
