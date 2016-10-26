package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.regression.DecisionTreeRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeRegressionOp extends OpNode[MleapContext, DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, DecisionTreeRegressionModel] = new OpModel[MleapContext, DecisionTreeRegressionModel] {
    override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext[MleapContext], model: Model, obj: DecisionTreeRegressionModel): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
      model.withAttr("num_features", Value.long(obj.numFeatures))
    }

    override def load(context: BundleContext[MleapContext], model: Model): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("nodes"), withImpurities = false).read()
      DecisionTreeRegressionModel(rootNode, numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override val klazz: Class[DecisionTreeRegression] = classOf[DecisionTreeRegression]

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: DecisionTreeRegressionModel): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
