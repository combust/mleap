package ml.combust.bundle.serializer

import java.nio.file.{Files, Path}

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.{BundleContext, HasBundleRegistry}
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.attr.{AttributeListSeparator, AttributeListSerializer}
import ml.combust.bundle.dsl.{Bundle, Model}
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
    * @param context serialization context for determining which serializer to return
    * @return JSON or Protobuf model definition serializer depending on serialization context
    */
  def serializer(implicit context: SerializationContext): FormatModelSerializer = context.concrete match {
    case SerializationFormat.Json => JsonFormatModelSerializer()
    case SerializationFormat.Protobuf => ProtoFormatModelSerializer()
  }
}

/** Object for serializing/deserializing model definitions with JSON.
  */
case class JsonFormatModelSerializer(implicit hr: HasBundleRegistry) extends FormatModelSerializer {
  override def write(path: Path, model: Model): Unit = {
    Files.write(path, model.toJson.prettyPrint.getBytes)
  }

  override def read(path: Path): Model = {
    new String(Files.readAllBytes(path)).parseJson.convertTo[Model]
  }
}

/** Object for serializing/deserializing model definitions with Protobuf.
  */
case class ProtoFormatModelSerializer(implicit hr: HasBundleRegistry) extends FormatModelSerializer {
  override def write(path: Path, model: Model): Unit = {
    Files.write(path, model.asBundle.toByteArray)
  }

  override def read(path: Path): Model = {
    Model.fromBundle(ModelDef.parseFrom(Files.readAllBytes(path)))
  }
}

/** Class for serializing Bundle.ML models.
  *
  * @param bundleContext bundle context for path and bundle registry
  * @tparam Context context class for implementation
  */
case class ModelSerializer[Context](bundleContext: BundleContext[Context]) {
  private implicit val sc = bundleContext.preferredSerializationContext(SerializationFormat.Json)

  /** Write a model to the current context path.
    *
    * This will write the model to the current path in the [[BundleContext]].
    *
    * @param obj model to write
    */
  def write(obj: Any): Try[Any] = Try {
    Files.createDirectories(bundleContext.path)
    val m = bundleContext.bundleRegistry.modelForObj[Context, Any](obj)
    var model = Model(op = m.opName)
    model = m.store(model, obj)(bundleContext)

    bundleContext.format match {
      case SerializationFormat.Mixed =>
        val (small, large) = AttributeListSeparator().separate(model.attributes)
        val m = model.replaceAttrList(small)
        (for (l <- large) yield {
          AttributeListSerializer(bundleContext.file("model.pb")).
            writeProto(l).
            map(_ => m)
        }).getOrElse(Try(m))
      case _ => Try(model)
    }
  }.flatMap(identity).flatMap {
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
    Try(FormatModelSerializer.serializer.read(bundleContext.file(Bundle.modelFile))).flatMap {
      bundleModel =>
        val m = bundleContext.bundleRegistry.model[Context, Any](bundleModel.op)

        val tbm = bundleContext.format match {
          case SerializationFormat.Mixed =>
            if (Files.exists(bundleContext.file("model.pb"))) {
              AttributeListSerializer(bundleContext.file("model.pb")).
                readProto().
                map(bundleModel.withAttrList)
            } else {
              Try(bundleModel)
            }
          case _ => Try(bundleModel)
        }

        tbm.map {
          bm =>
            (m.load(bm)(bundleContext), bm)
        }
    }
  }
}
