package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
object VectorAssemblerOp extends OpNode[MleapContext, VectorAssembler, VectorAssemblerModel] {
  override val Model: OpModel[MleapContext, VectorAssemblerModel] = new OpModel[MleapContext, VectorAssemblerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(context: BundleContext[MleapContext], model: Model, obj: VectorAssemblerModel): Model = { model }

    override def load(context: BundleContext[MleapContext], model: Model): VectorAssemblerModel = VectorAssemblerModel.default
  }

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssemblerModel = VectorAssemblerModel.default

  override def load(context: BundleContext[MleapContext], node: Node, model: VectorAssemblerModel): VectorAssembler = {
    VectorAssembler(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      outputCol = node.shape.standardOutput.name)
  }

  override def shape(node: VectorAssembler): Shape = {
    var i = 0
    node.inputCols.foldLeft(Shape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.outputCol)
  }
}
