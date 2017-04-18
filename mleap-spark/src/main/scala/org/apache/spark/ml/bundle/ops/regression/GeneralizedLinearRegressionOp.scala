package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class GeneralizedLinearRegressionOp extends OpNode[SparkBundleContext, GeneralizedLinearRegressionModel, GeneralizedLinearRegressionModel] {
  override val Model: OpModel[SparkBundleContext, GeneralizedLinearRegressionModel] = new OpModel[SparkBundleContext, GeneralizedLinearRegressionModel] {
    override val klazz: Class[GeneralizedLinearRegressionModel] = classOf[GeneralizedLinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.generalized_linear_regression

    override def store(model: Model, obj: GeneralizedLinearRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("family", Value.string(obj.getFamily)).
        withAttr("link", Value.string(obj.getLink))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GeneralizedLinearRegressionModel = {
      val m = new GeneralizedLinearRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)
      m.set(m.family, model.value("family").getString)
      m.set(m.link, model.value("link").getString)
      m
    }
  }

  override val klazz: Class[GeneralizedLinearRegressionModel] = classOf[GeneralizedLinearRegressionModel]

  override def name(node: GeneralizedLinearRegressionModel): String = node.uid

  override def model(node: GeneralizedLinearRegressionModel): GeneralizedLinearRegressionModel = node

  override def load(node: Node, model: GeneralizedLinearRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): GeneralizedLinearRegressionModel = {
    val m = new GeneralizedLinearRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept)
    m.set(m.family, model.getFamily)
    m.set(m.link, model.getLink)
    m.setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    node.shape.getOutput("link_prediction").foreach(col => m.setLinkPredictionCol(col.name))
    m
  }

  override def shape(node: GeneralizedLinearRegressionModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val s = Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction")
    if(node.isSet(node.linkPredictionCol)) { s.withOutput(node.getLinkPredictionCol, "link_prediction") }
    s
  }
}
