package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.AFTSurvivalRegression
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class AFTSurvivalRegressionOp extends OpNode[MleapContext, AFTSurvivalRegression, AFTSurvivalRegressionModel] {
  override val Model: OpModel[MleapContext, AFTSurvivalRegressionModel] = new OpModel[MleapContext, AFTSurvivalRegressionModel] {
    override val klazz: Class[AFTSurvivalRegressionModel] = classOf[AFTSurvivalRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.aft_survival_regression

    override def store(model: Model, obj: AFTSurvivalRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray.toSeq)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("quantile_probabilities", Value.doubleList(obj.quantileProbabilities)).
        withAttr("scale", Value.double(obj.scale))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): AFTSurvivalRegressionModel = {
      AFTSurvivalRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble,
        quantileProbabilities = model.value("quantile_probabilities").getDoubleList.toArray,
        scale = model.value("scale").getDouble)
    }
  }

  override val klazz: Class[AFTSurvivalRegression] = classOf[AFTSurvivalRegression]

  override def name(node: AFTSurvivalRegression): String = node.uid

  override def model(node: AFTSurvivalRegression): AFTSurvivalRegressionModel = node.model

  override def load(node: Node, model: AFTSurvivalRegressionModel)
                   (implicit context: BundleContext[MleapContext]): AFTSurvivalRegression = {
    AFTSurvivalRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      quantilesCol = node.shape.getOutput("quantiles").map(_.name),
      model = model)
  }

  override def shape(node: AFTSurvivalRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.quantilesCol, "quantiles")
}
