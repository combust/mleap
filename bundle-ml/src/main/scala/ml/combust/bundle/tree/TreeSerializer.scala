package ml.combust.bundle.tree

import java.io._
import java.nio.file.{Files, Path}

import JsonSupport._
import spray.json._
import ml.bundle.tree.Node.Node
import ml.combust.bundle.BundleContext
import ml.combust.bundle.serializer.{SerializationContext, SerializationFormat}
import resource._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object FormatTreeSerializer {
  def writer(context: SerializationContext,
             out: OutputStream): FormatTreeWriter = context.concrete match {
    case SerializationFormat.Json => JsonFormatTreeWriter(new BufferedWriter(new OutputStreamWriter(out)))
    case SerializationFormat.Protobuf => ProtoFormatTreeWriter(new DataOutputStream(out))
  }

  def reader(context: SerializationContext,
             in: InputStream): FormatTreeReader = context.concrete match {
    case SerializationFormat.Json => JsonFormatTreeReader(new BufferedReader(new InputStreamReader(in)))
    case SerializationFormat.Protobuf => ProtoFormatTreeReader(new DataInputStream(in))
  }
}

trait FormatTreeWriter extends Closeable {
  def write(node: Node): Unit
}

trait FormatTreeReader extends Closeable {
  def read(): Node
}

case class JsonFormatTreeWriter(out: BufferedWriter) extends FormatTreeWriter {
  override def write(node: Node): Unit = {
    out.write(s"${node.toJson.compactPrint}\n")
  }

  override def close(): Unit = out.close()
}

case class JsonFormatTreeReader(in: BufferedReader) extends FormatTreeReader {
  override def read(): Node = {
    in.readLine().parseJson.convertTo[Node]
  }

  override def close(): Unit = in.close()
}

case class ProtoFormatTreeWriter(out: DataOutputStream) extends FormatTreeWriter {
  override def write(node: Node): Unit = {
    val size = node.serializedSize
    for(writer <- managed(new ByteArrayOutputStream(size))) {
      node.writeTo(writer)
      out.writeInt(size)
      out.write(writer.toByteArray)
    }
  }

  override def close(): Unit = out.close()
}

case class ProtoFormatTreeReader(in: DataInputStream) extends FormatTreeReader {
  override def read(): Node = {
    val size = in.readInt()
    val bytes = new Array[Byte](size)
    in.readFully(bytes)
    Node.parseFrom(bytes)
  }

  override def close(): Unit = in.close()
}

case class TreeSerializer[N: NodeWrapper](path: Path,
                                          withImpurities: Boolean)
                                         (implicit bundleContext: BundleContext[_]) {
  implicit val sc = bundleContext.preferredSerializationContext(SerializationFormat.Protobuf)
  val extension = sc.concrete match {
    case SerializationFormat.Json => "json"
    case SerializationFormat.Protobuf => "pb"
  }
  val ntc = implicitly[NodeWrapper[N]]

  def write(node: N): Unit = {
    for(out <- managed(Files.newOutputStream(path.getFileSystem.getPath(s"${path.toString}.$extension")));
        writer <- managed(FormatTreeSerializer.writer(sc, out))) {
      write(node, writer)
    }
  }

  def write(node: N, writer: FormatTreeWriter): Unit = {
    val n = ntc.node(node, withImpurities)
    writer.write(n)

    if(ntc.isInternal(node)) {
      write(ntc.left(node), writer)
      write(ntc.right(node), writer)
    }
  }

  def read(): N = {
    (for(in <- managed(Files.newInputStream(path.getFileSystem.getPath(s"${path.toString}.$extension")))) yield {
      val reader = FormatTreeSerializer.reader(sc, in)
      read(reader)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(n) => n
    }
  }

  def read(reader: FormatTreeReader): N = {
    val node = reader.read()

    if(node.n.isInternal) {
      ntc.internal(node.getInternal,
        read(reader),
        read(reader))
    } else if(node.n.isLeaf) {
      ntc.leaf(node.getLeaf, withImpurities)
    } else { throw new IllegalArgumentException("invalid tree") }
  }
}
