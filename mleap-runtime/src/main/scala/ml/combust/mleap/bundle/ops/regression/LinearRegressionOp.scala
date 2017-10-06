package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.frame.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
class LinearRegressionOp extends MleapOp[LinearRegression, LinearRegressionModel] {
  override val Model: OpModel[MleapContext, LinearRegressionModel] = new OpModel[MleapContext, LinearRegressionModel] {
    override val klazz: Class[LinearRegressionModel] = classOf[LinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(model: Model, obj: LinearRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): LinearRegressionModel = {
      LinearRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)
    }
  }

  override def model(node: LinearRegression): LinearRegressionModel = node.model
}
