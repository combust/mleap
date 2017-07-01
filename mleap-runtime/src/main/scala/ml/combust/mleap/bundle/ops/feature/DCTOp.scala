package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.DCTModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.DCT

/**
  * Created by hollinwilkins on 12/28/16.
  */
class DCTOp extends OpNode[MleapContext, DCT, DCTModel] {
  override val Model: OpModel[MleapContext, DCTModel] = new OpModel[MleapContext, DCTModel] {
    override val klazz: Class[DCTModel] = classOf[DCTModel]

    override def opName: String = Bundle.BuiltinOps.feature.dct

    override def store(model: Model, obj: DCTModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("inverse", Value.boolean(obj.inverse))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): DCTModel = {
      DCTModel(inverse = model.value("inverse").getBoolean)
    }
  }

  override val klazz: Class[DCT] = classOf[DCT]

  override def name(node: DCT): String = node.uid

  override def model(node: DCT): DCTModel = node.model

  override def load(node: Node, model: DCTModel)
                   (implicit context: BundleContext[MleapContext]): DCT = {
    DCT(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: DCT): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
