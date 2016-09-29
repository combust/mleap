package ml.bundle.serializer

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

import ml.bundle.NodeDef.NodeDef
import ml.bundle.dsl.{Bundle, Node, Shape}
import ml.bundle.json.JsonSupport._
import spray.json._
import resource._

import scala.io.Source

/** Trait for serializing node definitions.
  */
trait NodeDefSerializer {
  /** Write a node definition to an output stream.
    *
    * @param out stream to write to
    * @param obj node definition to write
    */
  def write(out: OutputStream, obj: NodeDef): Unit

  /** Read a node definition from an input stream.
    *
    * @param in stream to read from
    * @return node definition
    */
  def read(in: InputStream): NodeDef
}

/** Companion class for utility serializer methods for node definitions.
  */
object NodeDefSerializer {
  /** Get the serializer for a given serialization context.
    *
    * @param context serialization context for desired format
    * @return serializer for given concrete serialization format
    */
  def serializer(implicit context: SerializationContext): NodeDefSerializer = context.concrete match {
    case SerializationFormat.Json => JsonNodeDefSerializer
    case SerializationFormat.Protobuf => ProtoNodeDefSerializer
  }
}

/** Object for serializing/deserializing node definitions with JSON.
  */
object JsonNodeDefSerializer extends NodeDefSerializer {
  override def write(out: OutputStream, obj: NodeDef): Unit = {
    out.write(obj.toJson.prettyPrint.getBytes)
  }

  override def read(in: InputStream): NodeDef = {
    Source.fromInputStream(in).getLines.mkString.parseJson.convertTo[NodeDef]
  }
}

/** Object for serializing/deserializing node definitions with Protobuf.
  */
object ProtoNodeDefSerializer extends NodeDefSerializer {
  override def write(out: OutputStream, obj: NodeDef): Unit = {
    obj.writeTo(out)
  }

  override def read(in: InputStream): NodeDef = {
    NodeDef.parseFrom(in)
  }
}

/** Class for serializing a Bundle.ML node.
  *
  * @param context bundle context for custom types and serialization formats
  */
case class NodeSerializer(context: BundleContext) {
  implicit val sc = context.preferredSerializationContext(SerializationFormat.Json)

  /** Write a node to the current context path.
    *
    * @param obj node to write
    */
  def write(obj: Any): Unit = {
    context.path.mkdirs()
    val op = context.bundleRegistry.opForObj[Any, Any](obj)
    val modelSerializer = ModelSerializer(context)
    modelSerializer.write(op.model(obj))

    val name = op.name(obj)
    val shape = op.shape(obj)
    val node = Node(name = name, shape = shape)
    for(out <- managed(new FileOutputStream(context.file(Bundle.nodeFile)))) {
      NodeDefSerializer.serializer.write(out, node.bundleNode)
    }
  }

  /** Read a node from the current context path.
    *
    * @return deserialized node
    */
  def read(): Any = {
    val nodeDef = (for(in <- managed(new FileInputStream(context.file(Bundle.nodeFile)))) yield {
      NodeDefSerializer.serializer.read(in)
    }).opt.get
    val node = Node(nodeDef.name, Shape(nodeDef.shape.get))

    val (model, m) = ModelSerializer(context).readWithModel()
    val op = context.bundleRegistry[Any, Any](m.op)
    op.load(context, node, model)
  }
}
