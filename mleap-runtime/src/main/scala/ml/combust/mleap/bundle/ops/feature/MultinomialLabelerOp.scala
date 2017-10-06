package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{MultinomialLabelerModel, ReverseStringIndexerModel}
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MultinomialLabeler

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabelerOp extends MleapOp[MultinomialLabeler, MultinomialLabelerModel] {
  override val Model: OpModel[MleapContext, MultinomialLabelerModel] = new OpModel[MleapContext, MultinomialLabelerModel] {
    override val klazz: Class[MultinomialLabelerModel] = classOf[MultinomialLabelerModel]

    override def opName: String = Bundle.BuiltinOps.feature.multinomial_labeler

    override def store(model: Model, obj: MultinomialLabelerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("threshold", Value.double(obj.threshold)).
        withValue("labels", Value.stringList(obj.indexer.labels))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MultinomialLabelerModel = {
      MultinomialLabelerModel(threshold = model.value("threshold").getDouble,
        indexer = ReverseStringIndexerModel(model.value("labels").getStringList))
    }
  }

  override def model(node: MultinomialLabeler): MultinomialLabelerModel = node.model
}
