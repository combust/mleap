package ml.combust.bundle.serializer

import java.nio.file.{Files, Path}

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Node}

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
    * @param format serialization format
    * @return serializer for given concrete serialization format
    */
  def serializer(implicit format: SerializationFormat): FormatNodeSerializer = format match {
    case SerializationFormat.Json => JsonFormatNodeSerializer
    case SerializationFormat.Protobuf => ProtoFormatNodeSerializer
  }
}

/** Object for serializing/deserializing node definitions with JSON.
  */
object JsonFormatNodeSerializer extends FormatNodeSerializer {
  override def write(path: Path, node: Node): Unit = {
    Files.write(path, node.asBundle.toString.getBytes("UTF-8"))
  }

  override def read(path: Path): Node = {
    Node.fromBundle(ml.bundle.bundle.Node.fromAscii(new String(Files.readAllBytes(path), "UTF-8")))
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
    Node.fromBundle(ml.bundle.bundle.Node.parseFrom(bytes))
  }
}

/** Class for serializing a Bundle.ML node.
  *
  * @param bundleContext bundle context for custom types and serialization formats
  * @tparam Context context class for implementation
  */
case class NodeSerializer[Context](bundleContext: BundleContext[Context]) {
  private implicit val format = bundleContext.format

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
