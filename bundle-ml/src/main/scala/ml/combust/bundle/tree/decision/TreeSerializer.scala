package ml.combust.bundle.tree.decision

import java.io._
import java.nio.file.{Files, Path}
import ml.bundle.dtree.Node
import ml.combust.bundle.BundleContext
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.bundle.tree.JsonSupport._
import spray.json._

import scala.util.{Try, Using}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object FormatTreeSerializer {
  def writer(format: SerializationFormat,
             out: OutputStream): FormatTreeWriter = format match {
    case SerializationFormat.Json => JsonFormatTreeWriter(new BufferedWriter(new OutputStreamWriter(out)))
    case SerializationFormat.Protobuf => ProtoFormatTreeWriter(new DataOutputStream(out))
  }

  def reader(format: SerializationFormat,
             in: InputStream): FormatTreeReader = format match {
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
    out.write(node.toJson.compactPrint + "\n")
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
    Using(new ByteArrayOutputStream(size)) { writer =>
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
  val extension: String = bundleContext.format match {
    case SerializationFormat.Json => "json"
    case SerializationFormat.Protobuf => "pb"
  }
  val ntc: NodeWrapper[N] = implicitly[NodeWrapper[N]]

  def write(node: N): Unit = {
    Using(Files.newOutputStream(path.getFileSystem.getPath(s"${path.toString}.$extension"))) {
      out=> Using(FormatTreeSerializer.writer(bundleContext.format, out)) {
        writer =>  write(node, writer)
      }
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

  def read(): Try[N] = {
    Using(Files.newInputStream(path.getFileSystem.getPath(s"${path.toString}.$extension"))) {in =>
      val reader = FormatTreeSerializer.reader(bundleContext.format, in)
      read(reader)
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
