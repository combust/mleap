package ml.combust.mleap.bundle.ops.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.classification.DecisionTreeClassifier
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeClassifierOp extends OpNode[MleapContext, DecisionTreeClassifier, DecisionTreeClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, DecisionTreeClassifierModel] = new OpModel[MleapContext, DecisionTreeClassifierModel] {
    override val klazz: Class[DecisionTreeClassifierModel] = classOf[DecisionTreeClassifierModel]

    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(context: BundleContext[MleapContext], model: Model, obj: DecisionTreeClassifierModel): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses))
    }

    override def load(context: BundleContext[MleapContext], model: Model): DecisionTreeClassifierModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).read()
      DecisionTreeClassifierModel(rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override val klazz: Class[DecisionTreeClassifier] = classOf[DecisionTreeClassifier]

  override def name(node: DecisionTreeClassifier): String = node.uid

  override def model(node: DecisionTreeClassifier): DecisionTreeClassifierModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: DecisionTreeClassifierModel): DecisionTreeClassifier = {
    DecisionTreeClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
