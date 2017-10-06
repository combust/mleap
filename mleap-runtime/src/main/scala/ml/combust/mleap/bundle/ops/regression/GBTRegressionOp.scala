package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.regression.{DecisionTreeRegressionModel, GBTRegressionModel}
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.regression.GBTRegression

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTRegressionOp extends MleapOp[GBTRegression, GBTRegressionModel] {
  override val Model: OpModel[MleapContext, GBTRegressionModel] = new OpModel[MleapContext, GBTRegressionModel] {
    override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.gbt_regression

    override def store(model: Model, obj: GBTRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree).get
          i = i + 1
          name
      }
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("tree_weights", Value.doubleList(obj.treeWeights)).
        withValue("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GBTRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }

      GBTRegressionModel(trees = models,
        treeWeights = treeWeights,
        numFeatures = numFeatures)
    }
  }

  override def model(node: GBTRegression): GBTRegressionModel = node.model
}
