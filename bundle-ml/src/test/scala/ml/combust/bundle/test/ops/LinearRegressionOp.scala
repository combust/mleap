package ml.combust.bundle.test.ops

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
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

    override def store(model: Model, obj: LinearModel)
                      (implicit context: BundleContext[Any]): Model = {
      model.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept))
    }


    override def load(model: Model)
                     (implicit context: BundleContext[Any]): LinearModel = {
      LinearModel(coefficients = model.value("coefficients").getTensor[Double].toArray,
        intercept = model.value("intercept").getDouble)
    }
  }

  override val klazz: Class[LinearRegression] = classOf[LinearRegression]

  override def name(node: LinearRegression): String = node.uid

  override def model(node: LinearRegression): LinearModel = node.model


  override def load(node: dsl.Node, model: LinearModel)
                   (implicit context: BundleContext[Any]): LinearRegression = {
    LinearRegression(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: LinearRegression)(implicit context: BundleContext[Any]): Shape = {
    Shape().withStandardIO(node.input, node.output)
  }
}
