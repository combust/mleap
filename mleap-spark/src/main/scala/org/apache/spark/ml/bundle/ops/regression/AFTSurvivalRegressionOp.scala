package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class AFTSurvivalRegressionOp extends OpNode[SparkBundleContext, AFTSurvivalRegressionModel, AFTSurvivalRegressionModel] {
  override val Model: OpModel[SparkBundleContext, AFTSurvivalRegressionModel] = new OpModel[SparkBundleContext, AFTSurvivalRegressionModel] {
    override val klazz: Class[AFTSurvivalRegressionModel] = classOf[AFTSurvivalRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.aft_survival_regression

    override def store(model: Model, obj: AFTSurvivalRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept)).
        withValue("quantile_probabilities", Value.doubleList(obj.getQuantileProbabilities)).
        withValue("scale", Value.double(obj.scale))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): AFTSurvivalRegressionModel = {
      new AFTSurvivalRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble,
        scale = model.value("scale").getDouble).
        setQuantileProbabilities(model.value("quantile_probabilities").getDoubleList.toArray)
    }
  }

  override val klazz: Class[AFTSurvivalRegressionModel] = classOf[AFTSurvivalRegressionModel]

  override def name(node: AFTSurvivalRegressionModel): String = node.uid

  override def model(node: AFTSurvivalRegressionModel): AFTSurvivalRegressionModel = node

  override def load(node: Node, model: AFTSurvivalRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): AFTSurvivalRegressionModel = {
    val r = new AFTSurvivalRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept,
      scale = model.scale).setQuantileProbabilities(model.getQuantileProbabilities).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    node.shape.getOutput("quantiles").foreach(q => r.setQuantilesCol(q.name))

    r
  }

  override def shape(node: AFTSurvivalRegressionModel): NodeShape = {
    var s = NodeShape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction")
    if(node.isSet(node.quantilesCol)) { s = s.withOutput(node.getQuantilesCol, "quantiles") }

    s
  }
}
