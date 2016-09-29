package ml.combust.bundle.serializer

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.attr.{AttributeListSeparator, AttributeListSerializer}
import ml.combust.bundle.dsl.{AttributeList, Bundle, Model, WritableModel}
import resource._
import spray.json._

import scala.io.Source

/** Trait for serializing a protobuf model definition.
  */
trait ModelDefSerializer {
  /** Write a protobuf model definition.
    *
    * @param out stream to write to
    * @param m model definition to write
    */
  def write(out: OutputStream, m: ModelDef): Unit

  /** Read a protobuf model definition.
    *
    * @param in stream to read from
    * @return model definition that was read
    */
  def read(in: InputStream): ModelDef
}

/** Companion object for utility methods related to model definition serialization.
  */
object ModelDefSerializer {
  /** Get the appropriate JSON or Protobuf serializer based on the serialization context.
    *
    * @param context serialization context for determining which serializer to return
    * @return JSON or Protobuf model definition serializer depending on serialization context
    */
  def serializer(implicit context: SerializationContext): ModelDefSerializer = context.concrete match {
    case SerializationFormat.Json => JsonModelDefSerializer
    case SerializationFormat.Protobuf => ProtoModelDefSerializer
  }
}

/** Object for serializing/deserializing model definitions with JSON.
  */
object JsonModelDefSerializer extends ModelDefSerializer {
  override def write(out: OutputStream, m: ModelDef): Unit = {
    out.write(m.toJson.prettyPrint.getBytes)
  }

  override def read(in: InputStream): ModelDef = {
    val json = Source.fromInputStream(in).getLines.mkString
    json.parseJson.convertTo[ModelDef]
  }
}

/** Object for serializing/deserializing model definitions with Protobuf.
  */
object ProtoModelDefSerializer extends ModelDefSerializer {
  override def write(out: OutputStream, m: ModelDef): Unit = {
    m.writeTo(out)
  }

  override def read(in: InputStream): ModelDef = {
    ModelDef.parseFrom(in)
  }
}

/** Class for serializing Bundle.ML models.
  *
  * @param context bundle context for path and bundle registry
  */
case class ModelSerializer(context: BundleContext) {
  implicit val sc = context.preferredSerializationContext(SerializationFormat.Json)

  /** Write a model to the current context path.
    *
    * This will write the model to the current path in the [[BundleContext]].
    *
    * @param obj model to write
    */
  def write(obj: Any): Unit = {
    context.path.mkdirs()
    val m = context.bundleRegistry.modelForObj[Any](obj)
    var model: WritableModel = Model(op = m.opName)
    model = m.store(context, model, obj)

    model = context.format match {
      case SerializationFormat.Mixed =>
        val (small, large) = AttributeListSeparator().separate(model.attributes)
        for(l <- large) { AttributeListSerializer(context.file("model.pb")).writeProto(l) }
        small.map(model.replaceAttrList).getOrElse(model)
      case _ => model
    }

    for(out <- managed(new FileOutputStream(context.file(Bundle.modelFile)))) {
      ModelDefSerializer.serializer.write(out, model.bundleModel)
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
    val modelDef = (for(in <- managed(new FileInputStream(context.file(Bundle.modelFile)))) yield {
      ModelDefSerializer.serializer.read(in)
    }).opt.get
    val m = context.bundleRegistry.model(modelDef.op)
    var model = Model(op = modelDef.op,
      attributes = modelDef.attributes.map(AttributeList.apply))

    model = context.format match {
      case SerializationFormat.Mixed =>
        if(context.file("model.pb").exists()) {
          val large = AttributeListSerializer(context.file("model.pb")).readProto()
          model.withAttrList(large)
        } else { model }
      case _ => model
    }

    (m.load(context, model), model)
  }
}
