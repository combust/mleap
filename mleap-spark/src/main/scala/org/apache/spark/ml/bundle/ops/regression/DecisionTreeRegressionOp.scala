package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.bundle.tree.decision.TreeSerializer
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.bundle.tree.decision.SparkNodeWrapper
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, LinearRegressionModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeRegressionOp extends SimpleSparkOp[DecisionTreeRegressionModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, DecisionTreeRegressionModel] = new OpModel[SparkBundleContext, DecisionTreeRegressionModel] {
    override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(model: Model, obj: DecisionTreeRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      TreeSerializer[org.apache.spark.ml.tree.Node](context.file("tree"), withImpurities = false).write(obj.rootNode)
      model.withValue("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[org.apache.spark.ml.tree.Node](context.file("tree"), withImpurities = false).read().get
      new DecisionTreeRegressionModel(uid = "",
        rootNode = rootNode,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: DecisionTreeRegressionModel): DecisionTreeRegressionModel = {
    new DecisionTreeRegressionModel(uid = uid,
      rootNode = model.rootNode,
      numFeatures = model.numFeatures)
  }

  override def sparkInputs(obj: DecisionTreeRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: DecisionTreeRegressionModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
}
