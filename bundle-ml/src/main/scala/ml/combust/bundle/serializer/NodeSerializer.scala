package ml.combust.bundle.serializer

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

import ml.bundle.NodeDef.NodeDef
import ml.combust.bundle.dsl.{Bundle, Node, Shape}
import ml.combust.bundle.json.JsonSupport._
import spray.json._
import resource._

import scala.io.Source

/** Trait for serializing node definitions.
  */
trait FormatNodeSerializer {
  /** Write a node definition to an output stream.
    *
    * @param out stream to write to
    * @param obj node definition to write
    */
  def write(out: OutputStream, obj: Node): Unit

  /** Read a node definition from an input stream.
    *
    * @param in stream to read from
    * @return node definition
    */
  def read(in: InputStream): Node
}

/** Companion class for utility serializer methods for node definitions.
  */
object FormatNodeSerializer {
  /** Get the serializer for a given serialization context.
    *
    * @param context serialization context for desired format
    * @return serializer for given concrete serialization format
    */
  def serializer(implicit context: SerializationContext): FormatNodeSerializer = context.concrete match {
    case SerializationFormat.Json => JsonFormatNodeSerializer
    case SerializationFormat.Protobuf => ProtoFormatNodeSerializer
  }
}

/** Object for serializing/deserializing node definitions with JSON.
  */
object JsonFormatNodeSerializer extends FormatNodeSerializer {
  override def write(out: OutputStream, node: Node): Unit = {
    out.write(node.toJson.prettyPrint.getBytes)
  }

  override def read(in: InputStream): Node = {
    Source.fromInputStream(in).getLines.mkString.parseJson.convertTo[Node]
  }
}

/** Object for serializing/deserializing node definitions with Protobuf.
  */
object ProtoFormatNodeSerializer extends FormatNodeSerializer {
  override def write(out: OutputStream, node: Node): Unit = {
    node.asBundle.writeTo(out)
  }

  override def read(in: InputStream): Node = {
    Node.fromBundle(NodeDef.parseFrom(in))
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
      FormatNodeSerializer.serializer.write(out, node)
    }
  }

  /** Read a node from the current context path.
    *
    * @return deserialized node
    */
  def read(): Any = {
    val node = (for(in <- managed(new FileInputStream(context.file(Bundle.nodeFile)))) yield {
      FormatNodeSerializer.serializer.read(in)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(n) => n
    }

    val (model, m) = ModelSerializer(context).readWithModel()
    val op = context.bundleRegistry[Any, Any](m.op)
    op.load(context, node, model)
  }
}
