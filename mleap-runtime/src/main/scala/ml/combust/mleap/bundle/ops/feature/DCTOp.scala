package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.DCTModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.DCT

/**
  * Created by hollinwilkins on 12/28/16.
  */
class DCTOp extends MleapOp[DCT, DCTModel] {
  override val Model: OpModel[MleapContext, DCTModel] = new OpModel[MleapContext, DCTModel] {
    override val klazz: Class[DCTModel] = classOf[DCTModel]

    override def opName: String = Bundle.BuiltinOps.feature.dct

    override def store(model: Model, obj: DCTModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("inverse", Value.boolean(obj.inverse))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): DCTModel = {
      DCTModel(inverse = model.value("inverse").getBoolean)
    }
  }

  override def model(node: DCT): DCTModel = node.model
}
