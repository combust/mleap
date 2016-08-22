package ml.combust.mleap.runtime.serialization.bundle.ops.regression

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.Node
import ml.combust.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.regression.DecisionTreeRegression
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.tree.TreeSerializer
import ml.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeRegressionOp extends OpNode[DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[DecisionTreeRegressionModel] = new OpModel[DecisionTreeRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext, model: WritableModel, obj: DecisionTreeRegressionModel): WritableModel = {
      TreeSerializer[Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures)))
    }

    override def load(context: BundleContext, model: ReadableModel): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[Node](context.file("nodes"), withImpurities = false).read()
      DecisionTreeRegressionModel(rootNode, numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: DecisionTreeRegressionModel): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
