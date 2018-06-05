package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class GeneralizedLinearRegressionOp extends SimpleSparkOp[GeneralizedLinearRegressionModel] {
  override val Model: OpModel[SparkBundleContext, GeneralizedLinearRegressionModel] = new OpModel[SparkBundleContext, GeneralizedLinearRegressionModel] {
    override val klazz: Class[GeneralizedLinearRegressionModel] = classOf[GeneralizedLinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.generalized_linear_regression

    override def store(model: Model, obj: GeneralizedLinearRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val modelWithoutLink = model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept)).
        withValue("family", Value.string(obj.getFamily))
      if (obj.isDefined(obj.link)) {
        modelWithoutLink.withValue("link", Value.string(obj.getLink))
      } else {
        modelWithoutLink
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GeneralizedLinearRegressionModel = {
      val m = new GeneralizedLinearRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)
      m.set(m.family, model.value("family").getString)
      if (model.getValue("link").isDefined) {
        m.set(m.link, model.value("link").getString)
      }
      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: GeneralizedLinearRegressionModel): GeneralizedLinearRegressionModel = {
    new GeneralizedLinearRegressionModel(uid = uid,
      coefficients = model.coefficients,
      intercept = model.intercept)
  }

  override def sparkInputs(obj: GeneralizedLinearRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: GeneralizedLinearRegressionModel): Seq[SimpleParamSpec] = {
    Seq("link_prediction" -> obj.linkPredictionCol,
      "prediction" -> obj.predictionCol)
  }
}
