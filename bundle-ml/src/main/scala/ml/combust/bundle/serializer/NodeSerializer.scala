package ml.combust.bundle.serializer

import java.io.{InputStream, OutputStream}
import java.nio.file.Files

import ml.bundle.NodeDef.NodeDef
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Node}
import ml.combust.bundle.json.JsonSupport._
import spray.json._
import resource._

import scala.io.Source
import scala.util.{Failure, Try}

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
  * @param bundleContext bundle context for custom types and serialization formats
  * @tparam Context context class for implementation
  */
case class NodeSerializer[Context](bundleContext: BundleContext[Context]) {
  implicit val sc = bundleContext.preferredSerializationContext(SerializationFormat.Json)

  /** Write a node to the current context path.
    *
    * @param obj node to write
    */
  def write(obj: Any): Try[Any] = Try {
    Files.createDirectories(bundleContext.path)
    val op = bundleContext.bundleRegistry.opForObj[Context, Any, Any](obj)
    val modelSerializer = ModelSerializer(bundleContext)
    modelSerializer.write(op.model(obj))

    val name = op.name(obj)
    val shape = op.shape(obj)
    Node(name = name, shape = shape)
  }.flatMap {
    node =>
      (for(out <- managed(Files.newOutputStream(bundleContext.file(Bundle.nodeFile)))) yield {
        FormatNodeSerializer.serializer.write(out, node)
      }).either.either match {
        case Right(_) => Try(obj)
        case Left(errors) => Failure(errors.head)
      }
  }

  /** Read a node from the current context path.
    *
    * @return deserialized node
    */
  def read(): Try[Any] = {
    ((for(in <- managed(Files.newInputStream(bundleContext.file(Bundle.nodeFile)))) yield {
      FormatNodeSerializer.serializer.read(in)
    }).either.either match {
      case Right(n) => Try(n)
      case Left(errors) => Failure(errors.head)
    }).flatMap {
      node =>
        ModelSerializer(bundleContext).readWithModel().flatMap {
          case (model, m) =>
            Try {
              val op = bundleContext.bundleRegistry[Context, Any, Any](m.op)
              op.load(node, model)(bundleContext)
            }
        }
    }
  }
}
