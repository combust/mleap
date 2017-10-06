package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.ChiSqSelectorModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.ChiSqSelector

/**
  * Created by hollinwilkins on 12/27/16.
  */
class ChiSqSelectorOp extends MleapOp[ChiSqSelector, ChiSqSelectorModel] {
  override val Model: OpModel[MleapContext, ChiSqSelectorModel] = new OpModel[MleapContext, ChiSqSelectorModel] {
    override val klazz: Class[ChiSqSelectorModel] = classOf[ChiSqSelectorModel]

    override def opName: String = Bundle.BuiltinOps.feature.chi_sq_selector

    override def store(model: Model, obj: ChiSqSelectorModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("filter_indices", Value.longList(obj.filterIndices.map(_.toLong))).
        withValue("input_size", Value.int(obj.inputSize))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ChiSqSelectorModel = {
      ChiSqSelectorModel(filterIndices = model.value("filter_indices").getLongList.map(_.toInt),
        inputSize = model.value("input_size").getInt)
    }
  }

  override def model(node: ChiSqSelector): ChiSqSelectorModel = node.model
}
