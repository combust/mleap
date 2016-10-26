package ml.combust.bundle.test_ops

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.bundle.{BundleContext, dsl}

/**
  * Created by hollinwilkins on 8/21/16.
  */
case class LinearModel(coefficients: Seq[Double],
                       intercept: Double)
case class LinearRegression(uid: String,
                            input: String,
                            output: String,
                            model: LinearModel) extends Transformer

class LinearRegressionOp extends OpNode[Any, LinearRegression, LinearModel] {
  override val Model: OpModel[Any, LinearModel] = new OpModel[Any, LinearModel] {
    override val klazz: Class[LinearModel] = classOf[LinearModel]

    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(context: BundleContext[Any], model: Model, obj: LinearModel): Model = {
      model.withAttr(Attribute("coefficients", Value.doubleVector(obj.coefficients))).
        withAttr(Attribute("intercept", Value.double(obj.intercept)))
    }

    override def load(context: BundleContext[Any], model: Model): LinearModel = {
      LinearModel(coefficients = model.value("coefficients").getDoubleVector,
        intercept = model.value("intercept").getDouble)
    }
  }

  override val klazz: Class[LinearRegression] = classOf[LinearRegression]

  override def name(node: LinearRegression): String = node.uid

  override def model(node: LinearRegression): LinearModel = node.model

  override def load(context: BundleContext[Any], node: dsl.Node, model: LinearModel): LinearRegression = {
    LinearRegression(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: LinearRegression): Shape = Shape().withStandardIO(node.input, node.output)
}
