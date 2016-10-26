package ml.combust.bundle.serializer

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.attr.{AttributeListSeparator, AttributeListSerializer}
import ml.combust.bundle.dsl.{AttributeList, Bundle, Model}
import resource._
import spray.json._

import scala.io.Source

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
  def write(obj: Any): Unit = {
    bundleContext.path.mkdirs()
    val m = bundleContext.bundleRegistry.modelForObj[Context, Any](obj)
    var model: Model = Model(op = m.opName)
    model = m.store(bundleContext, model, obj)

    model = bundleContext.format match {
      case SerializationFormat.Mixed =>
        val (small, large) = AttributeListSeparator().separate(model.attributes)
        for(l <- large) { AttributeListSerializer(bundleContext.file("model.pb")).writeProto(l) }
        model.replaceAttrList(small)
      case _ => model
    }

    for(out <- managed(new FileOutputStream(bundleContext.file(Bundle.modelFile)))) {
      FormatModelSerializer.serializer.write(out, model)
    }
  }

  /** Read a model from the current bundle context.
    *
    * @return deserialized model
    */
  def read(): Any = { readWithModel()._1 }

  /** Read a model and return it along with its type class.
    *
    * @return (deserialized model, model type class)
    */
  def readWithModel(): (Any, Model) = {
    var bundleModel = (for(in <- managed(new FileInputStream(bundleContext.file(Bundle.modelFile)))) yield {
      FormatModelSerializer.serializer.read(in)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bm) => bm
    }
    val m = bundleContext.bundleRegistry.model[Context, Any](bundleModel.op)

    bundleModel = bundleContext.format match {
      case SerializationFormat.Mixed =>
        if(bundleContext.file("model.pb").exists()) {
          val large = AttributeListSerializer(bundleContext.file("model.pb")).readProto()
          bundleModel.withAttrList(large)
        } else { bundleModel }
      case _ => bundleModel
    }

    (m.load(bundleContext, bundleModel), bundleModel)
  }
}
