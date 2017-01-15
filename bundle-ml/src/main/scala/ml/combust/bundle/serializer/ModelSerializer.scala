package ml.combust.bundle.serializer

import java.io.{InputStream, OutputStream}
import java.nio.file.Files

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.{BundleContext, HasBundleRegistry}
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.attr.{AttributeListSeparator, AttributeListSerializer}
import ml.combust.bundle.dsl.{Bundle, Model}
import resource._
import spray.json._

import scala.io.Source
import scala.util.{Failure, Try}

/** Trait for serializing a protobuf model definition.
  */
trait FormatModelSerializer {
  /** Write a protobuf model definition.
    *
    * @param out stream to write to
    * @param model model to write
    */
  def write(out: OutputStream, model: Model): Unit

  /** Read a protobuf model definition.
    *
    * @param in stream to read from
    * @return model that was read
    */
  def read(in: InputStream): Model
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
  override def write(out: OutputStream, model: Model): Unit = {
    out.write(model.toJson.prettyPrint.getBytes)
  }

  override def read(in: InputStream): Model = {
    val json = Source.fromInputStream(in).getLines.mkString
    json.parseJson.convertTo[Model]
  }
}

/** Object for serializing/deserializing model definitions with Protobuf.
  */
case class ProtoFormatModelSerializer(implicit hr: HasBundleRegistry) extends FormatModelSerializer {
  override def write(out: OutputStream, model: Model): Unit = {
    model.asBundle.writeTo(out)
  }

  override def read(in: InputStream): Model = {
    Model.fromBundle(ModelDef.parseFrom(in))
  }
}

/** Class for serializing Bundle.ML models.
  *
  * @param bundleContext bundle context for path and bundle registry
  * @tparam Context context class for implementation
  */
case class ModelSerializer[Context](bundleContext: BundleContext[Context]) {
  implicit val sc = bundleContext.preferredSerializationContext(SerializationFormat.Json)

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
        for (l <- large) {
          AttributeListSerializer(bundleContext.file("model.pb")).writeProto(l)
        }
        model.replaceAttrList(small)
      case _ => model
    }
  }.flatMap {
    model =>
      (for(out <- managed(Files.newOutputStream(bundleContext.file(Bundle.modelFile)))) yield {
        FormatModelSerializer.serializer.write(out, model)
      }).either.either match {
        case Right(_) => Try(model)
        case Left(errors) =>
          Failure(errors.head)
      }
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
    ((for(in <- managed(Files.newInputStream(bundleContext.file(Bundle.modelFile)))) yield {
      FormatModelSerializer.serializer.read(in)
    }).either.either match {
      case Right(bm) => Try(bm)
      case Left(errors) => Failure(errors.head)
    }).map {
      bundleModel =>
        val m = bundleContext.bundleRegistry.model[Context, Any](bundleModel.op)

        val bm = bundleContext.format match {
          case SerializationFormat.Mixed =>
            if (Files.exists(bundleContext.file("model.pb"))) {
              val large = AttributeListSerializer(bundleContext.file("model.pb")).readProto()
              bundleModel.withAttrList(large)
            } else {
              bundleModel
            }
          case _ => bundleModel
        }

        (m.load(bm)(bundleContext), bm)
    }
  }
}
