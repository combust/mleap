package ml.combust.mleap.runtime.serialization.bundle.ops.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.tree.Node
import ml.combust.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.classification.DecisionTreeClassifier
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.tree.TreeSerializer
import ml.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeClassifierOp extends OpNode[DecisionTreeClassifier, DecisionTreeClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[DecisionTreeClassifierModel] = new OpModel[DecisionTreeClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(context: BundleContext, model: WritableModel, obj: DecisionTreeClassifierModel): WritableModel = {
      TreeSerializer[Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("num_classes", Value.long(obj.numClasses)))
    }

    override def load(context: BundleContext, model: ReadableModel): DecisionTreeClassifierModel = {
      val rootNode = TreeSerializer[Node](context.file("nodes"), withImpurities = true).read()
      DecisionTreeClassifierModel(rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeClassifier): String = node.uid

  override def model(node: DecisionTreeClassifier): DecisionTreeClassifierModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: DecisionTreeClassifierModel): DecisionTreeClassifier = {
    DecisionTreeClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
