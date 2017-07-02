package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.{MultinomialLabelerModel, ReverseStringIndexerModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MultinomialLabeler

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabelerOp extends OpNode[MleapContext, MultinomialLabeler, MultinomialLabelerModel] {
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

  override val klazz: Class[MultinomialLabeler] = classOf[MultinomialLabeler]

  override def name(node: MultinomialLabeler): String = node.uid

  override def model(node: MultinomialLabeler): MultinomialLabelerModel = node.model

  override def load(node: Node, model: MultinomialLabelerModel)
                   (implicit context: BundleContext[MleapContext]): MultinomialLabeler = {
    MultinomialLabeler(uid = node.name,
      featuresCol = node.shape.input("features").name,
      probabilitiesCol = node.shape.output("probabilities").name,
      labelsCol = node.shape.output("labels").name,
      model = model)
  }

  override def shape(node: MultinomialLabeler): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.probabilitiesCol, "probabilities").
    withOutput(node.labelsCol, "labels")
}
