package ml.combust.mleap.runtime.bundle.ops.regression

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object LinearRegressionOp extends OpNode[LinearRegression, LinearRegressionModel] {
  override val Model: OpModel[LinearRegressionModel] = new OpModel[LinearRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(context: BundleContext, model: Model, obj: LinearRegressionModel): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept))
    }

    override def load(context: BundleContext, model: Model): LinearRegressionModel = {
      LinearRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)
    }
  }

  override def name(node: LinearRegression): String = node.uid

  override def model(node: LinearRegression): LinearRegressionModel = node.model

  override def load(context: BundleContext, node: Node, model: LinearRegressionModel): LinearRegression = {
    LinearRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: LinearRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
