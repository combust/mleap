package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/22/16.
  */
class VectorAssemblerOp extends OpNode[MleapContext, VectorAssembler, VectorAssemblerModel] {
  override val Model: OpModel[MleapContext, VectorAssemblerModel] = new OpModel[MleapContext, VectorAssemblerModel] {
    override val klazz: Class[VectorAssemblerModel] = classOf[VectorAssemblerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(model: Model, obj: VectorAssemblerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("input_shapes", Value.dataShapeList(obj.inputShapes.map(mleapToBundleShape)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorAssemblerModel = {
      val inputShapes = model.value("input_shapes").getDataShapeList.map(bundleToMleapShape)
      VectorAssemblerModel(inputShapes)
    }
  }

  override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssemblerModel = node.model

  override def load(node: Node, model: VectorAssemblerModel)
                   (implicit context: BundleContext[MleapContext]): VectorAssembler = {
    VectorAssembler(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: VectorAssembler): NodeShape = {
    var i = 0
    node.inputCols.foldLeft(NodeShape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.outputCol)
  }
}
