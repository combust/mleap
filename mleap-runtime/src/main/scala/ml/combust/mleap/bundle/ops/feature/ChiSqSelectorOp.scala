package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.ChiSqSelectorModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.ChiSqSelector

/**
  * Created by hollinwilkins on 12/27/16.
  */
class ChiSqSelectorOp extends OpNode[MleapContext, ChiSqSelector, ChiSqSelectorModel] {
  override val Model: OpModel[MleapContext, ChiSqSelectorModel] = new OpModel[MleapContext, ChiSqSelectorModel] {
    override val klazz: Class[ChiSqSelectorModel] = classOf[ChiSqSelectorModel]

    override def opName: String = Bundle.BuiltinOps.feature.chi_sq_selector

    override def store(model: Model, obj: ChiSqSelectorModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("filter_indices", Value.longList(obj.filterIndices.map(_.toLong)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ChiSqSelectorModel = {
      ChiSqSelectorModel(filterIndices = model.value("filter_indices").getLongList.map(_.toInt))
    }
  }

  override val klazz: Class[ChiSqSelector] = classOf[ChiSqSelector]

  override def name(node: ChiSqSelector): String = node.uid

  override def model(node: ChiSqSelector): ChiSqSelectorModel = node.model

  override def load(node: Node, model: ChiSqSelectorModel)
                   (implicit context: BundleContext[MleapContext]): ChiSqSelector = {
    ChiSqSelector(uid = node.name,
      featuresCol = node.shape.input("features").name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: ChiSqSelector): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withStandardOutput(node.outputCol)
}
