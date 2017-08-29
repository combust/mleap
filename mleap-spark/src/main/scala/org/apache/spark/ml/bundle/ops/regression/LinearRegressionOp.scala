package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.regression.LinearRegressionModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LinearRegressionOp extends SimpleSparkOp[LinearRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LinearRegressionModel] = new OpModel[SparkBundleContext, LinearRegressionModel] {
    override val klazz: Class[LinearRegressionModel] = classOf[LinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(model: Model, obj: LinearRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): LinearRegressionModel = {
      new LinearRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: LinearRegressionModel): LinearRegressionModel = {
    new LinearRegressionModel(uid = uid,
      coefficients = model.coefficients,
      intercept = model.intercept)
  }

  override def sparkInputs(obj: LinearRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: LinearRegressionModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
}
