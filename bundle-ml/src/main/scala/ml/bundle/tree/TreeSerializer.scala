package ml.bundle.tree

import java.io._

import com.google.protobuf.CodedOutputStream
import resource._

/**
  * Created by hollinwilkins on 8/22/16.
  */
case class TreeSerializer[N: NodeWrapper](path: File, withImpurities: Boolean) {
  val ntc = implicitly[NodeWrapper[N]]

  def write(node: N): Unit = {
    for(out <- managed(new DataOutputStream(new FileOutputStream(s"$path.pb")))) {
      write(node, out)
    }
  }

  def write(node: N, out: DataOutputStream): Unit = {
    val n = ntc.node(node, withImpurities)
    val size = n.serializedSize
    for(writer <- managed(new ByteArrayOutputStream(size))) {
      n.writeTo(writer)
      out.writeInt(size)
      out.write(writer.toByteArray)
    }

    if(ntc.isInternal(node)) {
      write(ntc.left(node), out)
      write(ntc.right(node), out)
    }
  }

  def read(): N = {
    (for(in <- managed(new DataInputStream(new FileInputStream(s"$path.pb")))) yield {
      read(in)
    }).opt.get
  }

  def read(in: DataInputStream): N = {
    val size = in.readInt()
    val bytes = new Array[Byte](size)
    in.readFully(bytes)
    val node = Node.Node.parseFrom(bytes)

    if(node.n.isInternal) {
      ntc.internal(node.getInternal,
        read(in),
        read(in))
    } else if(node.n.isLeaf) {
      ntc.leaf(node.getLeaf, withImpurities)
    } else { throw new Error("invalid tree") } // TODO: better error
  }
}
