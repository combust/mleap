package ml.combust.bundle.tree.cluster

import java.io.{BufferedReader, DataInputStream, InputStreamReader, _}
import java.nio.file.{Files, Path}

import ml.bundle.ctree.ctree.Node
import ml.combust.bundle.BundleContext
import ml.combust.bundle.serializer.SerializationFormat
import com.trueaccord.scalapb.json.JsonFormat
import resource._

import scala.util.Try

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
    out.write(JsonFormat.toJsonString(node) + "\n")
  }

  override def close(): Unit = out.close()
}

case class JsonFormatNodeReader(in: BufferedReader) extends FormatNodeReader {
  override def read(): Node = {
    JsonFormat.fromJsonString[Node](in.readLine())
  }

  override def close(): Unit = in.close()
}

case class ProtoFormatNodeWriter(out: DataOutputStream) extends FormatNodeWriter {
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
  val extension = bundleContext.format match {
    case SerializationFormat.Json => "json"
    case SerializationFormat.Protobuf => "pb"
  }
  val ntc = implicitly[NodeWrapper[N]]

  def write(node: N): Unit = {
    val open = () => Files.newOutputStream(path.getFileSystem.getPath(s"${path.toString}.$extension"))
    for(writer <- managed(FormatNodeSerializer.writer(bundleContext.format, open()))) {
      write(node, writer)
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
    (for(in <- managed(Files.newInputStream(path.getFileSystem.getPath(s"${path.toString}.$extension")))) yield {
      val reader = FormatNodeSerializer.reader(bundleContext.format, in)
      read(reader)
    }).tried
  }

  def read(reader: FormatNodeReader): N = {
    val node = reader.read()
    val children = (0 until node.numChildren).map {
      _ => read(reader)
    }

    ntc.create(node, children)
  }
}
