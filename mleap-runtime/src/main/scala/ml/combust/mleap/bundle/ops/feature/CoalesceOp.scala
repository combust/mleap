package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Coalesce
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceOp extends OpNode[MleapContext, Coalesce, CoalesceModel] {
  override val Model: OpModel[MleapContext, CoalesceModel] = new OpModel[MleapContext, CoalesceModel] {
    override val klazz: Class[CoalesceModel] = classOf[CoalesceModel]

    override def opName: String = Bundle.BuiltinOps.feature.coalesce

    override def store(model: Model, obj: CoalesceModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("nullable_inputs", Value.booleanList(obj.nullableInputs))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): CoalesceModel = {
      CoalesceModel(model.value("nullable_inputs").getBooleanList)
    }
  }

  override val klazz: Class[Coalesce] = classOf[Coalesce]

  override def name(node: Coalesce): String = node.uid

  override def model(node: Coalesce): CoalesceModel = node.model

  override def load(node: Node, model: CoalesceModel)
                   (implicit context: BundleContext[MleapContext]): Coalesce = {
    Coalesce(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Coalesce): NodeShape = {
    var i = 0
    node.inputCols.foldLeft(NodeShape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.outputCol)
  }
}
