package ml.combust.mleap.runtime.bundle.ops.regression

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.runtime.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.regression.DecisionTreeRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeRegressionOp extends OpNode[DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[DecisionTreeRegressionModel] = new OpModel[DecisionTreeRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext, model: Model, obj: DecisionTreeRegressionModel): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures)))
    }

    override def load(context: BundleContext, model: Model): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("nodes"), withImpurities = false).read()
      DecisionTreeRegressionModel(rootNode, numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model

  override def load(context: BundleContext, node: Node, model: DecisionTreeRegressionModel): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
