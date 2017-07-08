package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.{MultinomialLabelerModel, ReverseStringIndexerModel}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.MultinomialLabeler

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabelerOp extends OpNode[SparkBundleContext, MultinomialLabeler, MultinomialLabelerModel] {
  override val Model: OpModel[SparkBundleContext, MultinomialLabelerModel] = new OpModel[SparkBundleContext, MultinomialLabelerModel] {
    override val klazz: Class[MultinomialLabelerModel] = classOf[MultinomialLabelerModel]

    override def opName: String = Bundle.BuiltinOps.feature.multinomial_labeler

    override def store(model: Model, obj: MultinomialLabelerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("threshold", Value.double(obj.threshold)).
        withValue("labels", Value.stringList(obj.indexer.labels))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MultinomialLabelerModel = {
      MultinomialLabelerModel(threshold = model.value("threshold").getDouble,
        indexer = ReverseStringIndexerModel(model.value("labels").getStringList))
    }
  }

  override val klazz: Class[MultinomialLabeler] = classOf[MultinomialLabeler]

  override def name(node: MultinomialLabeler): String = node.uid

  override def model(node: MultinomialLabeler): MultinomialLabelerModel = node.model

  override def load(node: Node, model: MultinomialLabelerModel)
                   (implicit context: BundleContext[SparkBundleContext]): MultinomialLabeler = {
    new MultinomialLabeler(uid = node.name,
      model = model).
      setFeaturesCol(node.shape.input("features").name).
      setProbabilitiesCol(node.shape.output("probabilities").name).
      setLabelsCol(node.shape.output("labels").name)
  }

  override def shape(node: MultinomialLabeler)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getProbabilitiesCol, "probabilities").
    withOutput(node.getLabelsCol, "labels")
}
