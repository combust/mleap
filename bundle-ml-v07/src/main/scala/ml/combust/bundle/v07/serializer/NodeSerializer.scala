package ml.combust.bundle.v07.serializer

import java.nio.file.{Files, Path}

import ml.bundle.NodeDef.NodeDef
import ml.combust.bundle.BundleContext
import ml.combust.bundle.v07.dsl.{Bundle, Node}
import ml.combust.bundle.v07.json.JsonSupport._
import spray.json._

import scala.util.Try

/** Trait for serializing node definitions.
  */
trait FormatNodeSerializer {
  /** Write a node definition to an output stream.
    *
    * @param path path to write node to
    * @param obj node definition to write
    */
  def write(path: Path, obj: Node): Unit

  /** Read a node definition from an input stream.
    *
    * @param path path to read node from
    * @return node definition
    */
  def read(path: Path): Node
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
  override def write(path: Path, node: Node): Unit = {
    Files.write(path, node.toJson.prettyPrint.getBytes)
  }

  override def read(path: Path): Node = {
    new String(Files.readAllBytes(path)).parseJson.convertTo[Node]
  }
}

/** Object for serializing/deserializing node definitions with Protobuf.
  */
object ProtoFormatNodeSerializer extends FormatNodeSerializer {
  override def write(path: Path, node: Node): Unit = {
    Files.write(path, node.asBundle.toByteArray)
  }

  override def read(path: Path): Node = {
    val bytes = Files.readAllBytes(path)
    Node.fromBundle(NodeDef.parseFrom(bytes))
  }
}

/** Class for serializing a Bundle.ML node.
  *
  * @param bundleContext bundle context for custom types and serialization formats
  * @tparam Context context class for implementation
  */
case class NodeSerializer[Context](bundleContext: BundleContext[Context]) {
  private implicit val sc = bundleContext.preferredSerializationContext(SerializationFormat.Json)

  /** Write a node to the current context path.
    *
    * @param obj node to write
    */
  def write(obj: Any): Try[Any] = Try {
    Files.createDirectories(bundleContext.path)
    val op = bundleContext.bundleRegistry.opForObj[Context, Any, Any](obj)
    val modelSerializer = ModelSerializer(bundleContext)
    modelSerializer.write(op.model(obj)).map {
      _ =>
        val name = op.name(obj)
        val shape = op.shape(obj)
        Node(name = name, shape = shape)
    }
  }.flatMap(identity).flatMap {
    node => Try(FormatNodeSerializer.serializer.write(bundleContext.file(Bundle.nodeFile), node))
  }

  /** Read a node from the current context path.
    *
    * @return deserialized node
    */
  def read(): Try[Any] = {
    Try(FormatNodeSerializer.serializer.read(bundleContext.file(Bundle.nodeFile))).flatMap {
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
