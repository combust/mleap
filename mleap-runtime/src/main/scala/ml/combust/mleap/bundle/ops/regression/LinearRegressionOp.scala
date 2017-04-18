package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
class LinearRegressionOp extends OpNode[MleapContext, LinearRegression, LinearRegressionModel] {
  override val Model: OpModel[MleapContext, LinearRegressionModel] = new OpModel[MleapContext, LinearRegressionModel] {
    override val klazz: Class[LinearRegressionModel] = classOf[LinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(model: Model, obj: LinearRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): LinearRegressionModel = {
      LinearRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)
    }
  }

  override val klazz: Class[LinearRegression] = classOf[LinearRegression]

  override def name(node: LinearRegression): String = node.uid

  override def model(node: LinearRegression): LinearRegressionModel = node.model

  override def load(node: Node, model: LinearRegressionModel)
                   (implicit context: BundleContext[MleapContext]): LinearRegression = {
    LinearRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: LinearRegression)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withInput(node.featuresCol, "features").withOutput(node.predictionCol, "prediction")
  }
}
