package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.feature.VectorAssembler

/**
  * Created by hollinwilkins on 8/21/16.
  */
object VectorAssemblerOp extends OpNode[VectorAssembler, VectorAssembler] {
  override val Model: OpModel[VectorAssembler] = new OpModel[VectorAssembler] {
    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(context: BundleContext, model: Model, obj: VectorAssembler): Model = { model }

    override def load(context: BundleContext, model: Model): VectorAssembler = { new VectorAssembler(uid = "") }
  }

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssembler = node

  override def load(context: BundleContext, node: Node, model: VectorAssembler): VectorAssembler = {
    new VectorAssembler().
      setInputCols(node.shape.inputs.map(_.name).toArray).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorAssembler): Shape = {
    val s = Shape()
    var i = 0
    for(input <- node.getInputCols) {
      s.withInput(input, s"input$i")
      i = i + 1
    }
    s.withStandardOutput(node.getOutputCol)
  }
}
