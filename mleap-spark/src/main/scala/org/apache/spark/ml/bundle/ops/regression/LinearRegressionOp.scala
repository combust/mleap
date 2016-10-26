package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LinearRegressionOp extends OpNode[SparkBundleContext, LinearRegressionModel, LinearRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LinearRegressionModel] = new OpModel[SparkBundleContext, LinearRegressionModel] {
    override val klazz: Class[LinearRegressionModel] = classOf[LinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: LinearRegressionModel): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): LinearRegressionModel = {
      new LinearRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)
    }
  }

  override val klazz: Class[LinearRegressionModel] = classOf[LinearRegressionModel]

  override def name(node: LinearRegressionModel): String = node.uid

  override def model(node: LinearRegressionModel): LinearRegressionModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: LinearRegressionModel): LinearRegressionModel = {
    new LinearRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: LinearRegressionModel): Shape = Shape().
    withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
