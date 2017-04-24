package ml.combust.bundle.serializer

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.Bundle

import scala.util.Try

/** Class for serializing a list of graph nodes.
  *
  * @param bundleContext bundle context for encoding/decoding custom types, op nodes and op models
  * @tparam Context context class for implementation
  */
case class GraphSerializer[Context](bundleContext: BundleContext[Context]) {
  /** Write a list of nodes to the path in the context.
    *
    * @param nodes list of nodes to write
    * @return list of names of the nodes written
    */
  def write(nodes: Seq[Any]): Try[Seq[String]] = {
    nodes.foldLeft(Try(Seq[String]())) {
      (s, n) => writeNode(n).flatMap(name => s.map(_ :+ name))
    }
  }

  /** Write a single node to the path in the context.
    *
    * @param node node to write
    * @return name of node written
    */
  def writeNode(node: Any): Try[String] = Try {
    val op = bundleContext.bundleRegistry.opForObj[Context, Any, Any](node)
    val name = op.name(node)
    val nodeContext = bundleContext.bundleContext(Bundle.node(name))
    NodeSerializer(nodeContext).write(node).get
    name
  }

  /** Read a list of nodes from the path in context.
    *
    * @param names list of names of the nodes to read
    * @return list of the deserialized nodes
    */
  def read(names: Seq[String]): Try[Seq[Any]] = {
    names.map(readNode).foldLeft(Try(Seq[Any]())) {
      (ts, tt) =>
        tt.flatMap(t => ts.map(tss => tss :+ t))
    }
  }

  /** Read a single node from the path in context.
    *
    * @param name name of the node to read
    * @return deserialized node
    */
  def readNode(name: String): Try[Any] = {
    Try(bundleContext.bundleContext(Bundle.node(name))).flatMap {
      nodeContext => NodeSerializer(nodeContext).read()
    }
  }
}
