package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Coalesce

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceOp extends MleapOp[Coalesce, CoalesceModel] {
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

  override def model(node: Coalesce): CoalesceModel = node.model
}
