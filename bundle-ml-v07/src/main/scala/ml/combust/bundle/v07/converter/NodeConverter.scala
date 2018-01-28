package ml.combust.bundle.v07.converter

import ml.bundle.Socket
import ml.combust.bundle.dsl.{Node, NodeShape}
import ml.combust.bundle.v07

/**
  * Created by hollinwilkins on 1/27/18.
  */
trait NodeConverter {
  def convert(node: v07.dsl.Node)
             (implicit cc: ConverterContext): Node
}

class BaseNodeConverter extends NodeConverter {
  override def convert(node: v07.dsl.Node)
                      (implicit cc: ConverterContext): Node = {
    Node(node.name, convertNodeShape(node.shape))
  }

  def convertNodeShape(shape: v07.dsl.Shape): NodeShape = {
    val newInputs = shape.inputs.map {
      socket => Socket(port = socket.port, name = socket.name)
    }

    val newOutputs = shape.outputs.map {
      socket => Socket(port = socket.port, name = socket.name)
    }

    NodeShape(inputs = newInputs, outputs = newOutputs)
  }
}
