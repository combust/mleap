package ml.combust.mleap.runtime.bundle.ops.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.runtime.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.classification.DecisionTreeClassifier
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeClassifierOp extends OpNode[DecisionTreeClassifier, DecisionTreeClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[DecisionTreeClassifierModel] = new OpModel[DecisionTreeClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(context: BundleContext, model: Model, obj: DecisionTreeClassifierModel): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("num_classes", Value.long(obj.numClasses)))
    }

    override def load(context: BundleContext, model: Model): DecisionTreeClassifierModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).read()
      DecisionTreeClassifierModel(rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeClassifier): String = node.uid

  override def model(node: DecisionTreeClassifier): DecisionTreeClassifierModel = node.model

  override def load(context: BundleContext, node: Node, model: DecisionTreeClassifierModel): DecisionTreeClassifier = {
    DecisionTreeClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
