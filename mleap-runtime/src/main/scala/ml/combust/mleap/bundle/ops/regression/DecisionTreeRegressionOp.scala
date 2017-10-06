package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree
import ml.combust.mleap.runtime.transformer.regression.DecisionTreeRegression
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.bundle.tree.decision.TreeSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.bundle.tree.decision.MleapNodeWrapper
import ml.combust.mleap.runtime.frame.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeRegressionOp extends MleapOp[DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, DecisionTreeRegressionModel] = new OpModel[MleapContext, DecisionTreeRegressionModel] {
    override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(model: Model, obj: DecisionTreeRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
      model.withValue("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("tree"), withImpurities = false).read().get
      DecisionTreeRegressionModel(rootNode, numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model
}
