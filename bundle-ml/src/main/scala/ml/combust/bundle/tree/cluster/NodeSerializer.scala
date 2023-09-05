package ml.combust.bundle.tree.cluster

import java.io.{BufferedReader, DataInputStream, InputStreamReader, _}
import java.nio.file.{Files, Path}
import ml.bundle.ctree.Node
import ml.combust.bundle.BundleContext
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.bundle.tree.JsonSupport._
import spray.json._

import scala.util.{Try, Using}

/**
  * Created by hollinwilkins on 12/26/16.
  */
object FormatNodeSerializer {
  def writer(format: SerializationFormat,
             out: OutputStream): FormatNodeWriter = format match {
    case SerializationFormat.Json => JsonFormatNodeWriter(new BufferedWriter(new OutputStreamWriter(out)))
    case SerializationFormat.Protobuf => ProtoFormatNodeWriter(new DataOutputStream(out))
  }

  def reader(format: SerializationFormat,
             in: InputStream): FormatNodeReader = format match {
    case SerializationFormat.Json => JsonFormatNodeReader(new BufferedReader(new InputStreamReader(in)))
    case SerializationFormat.Protobuf => ProtoFormatNodeReader(new DataInputStream(in))
  }
}

trait FormatNodeWriter extends Closeable {
  def write(node: Node): Unit
}

trait FormatNodeReader extends Closeable {
  def read(): Node
}

case class JsonFormatNodeWriter(out: BufferedWriter) extends FormatNodeWriter {
  override def write(node: Node): Unit = {
    out.write(node.toJson.compactPrint + "\n")
  }

  override def close(): Unit = out.close()
}

case class JsonFormatNodeReader(in: BufferedReader) extends FormatNodeReader {
  override def read(): Node = {
    in.readLine().parseJson.convertTo[Node]
  }

  override def close(): Unit = in.close()
}

case class ProtoFormatNodeWriter(out: DataOutputStream) extends FormatNodeWriter {
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

case class ProtoFormatNodeReader(in: DataInputStream) extends FormatNodeReader {
  override def read(): Node = {
    val size = in.readInt()
    val bytes = new Array[Byte](size)
    in.readFully(bytes)
    Node.parseFrom(bytes)
  }

  override def close(): Unit = in.close()
}

case class NodeSerializer[N: NodeWrapper](path: Path)
                                         (implicit bundleContext: BundleContext[_]) {
  val extension: String = bundleContext.format match {
    case SerializationFormat.Json => "json"
    case SerializationFormat.Protobuf => "pb"
  }
  val ntc: NodeWrapper[N] = implicitly[NodeWrapper[N]]

  def write(node: N): Unit = {
    Using(Files.newOutputStream(path.getFileSystem.getPath(s"${path.toString}.$extension"))) {
      out =>
        Using(FormatNodeSerializer.writer(bundleContext.format, out)) {
          writer => write(node, writer)
        }
    }
  }

  def write(node: N, writer: FormatNodeWriter): Unit = {
    val n = ntc.node(node)
    writer.write(n)

    ntc.children(node).foreach {
      child => write(child, writer)
    }
  }

  def read(): Try[N] = {
    Using(Files.newInputStream(path.getFileSystem.getPath(s"${path.toString}.$extension"))) { in =>
      val reader = FormatNodeSerializer.reader(bundleContext.format, in)
      read(reader)
    }
  }

  def read(reader: FormatNodeReader): N = {
    val node = reader.read()
    val children = (0 until node.numChildren).map {
      _ => read(reader)
    }

    ntc.create(node, children)
  }
}
