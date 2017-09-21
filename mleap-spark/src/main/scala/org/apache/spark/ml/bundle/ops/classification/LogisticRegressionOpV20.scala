package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.bundle.util.ParamUtil

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LogisticRegressionOpV20 extends SimpleSparkOp[LogisticRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LogisticRegressionModel] = new OpModel[SparkBundleContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(obj.numClasses == 2, "This op only supports binary logistic regression")

      val m = model.withValue("num_classes", Value.long(obj.numClasses))
      m.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept)).
        withValue("threshold", Value.double(obj.getThreshold))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): LogisticRegressionModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("Only binary logistic regression supported in Spark")
      }

      val lr = new LogisticRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble)

      model.getValue("threshold").
        map(t => lr.setThreshold(t.getDouble)).
        getOrElse(lr)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: LogisticRegressionModel): LogisticRegressionModel = {
    val r = new LogisticRegressionModel(uid = uid,
      coefficients = model.coefficients,
      intercept = model.intercept)
    if(r.isDefined(r.threshold)) { r.setThreshold(r.getThreshold) }
    r
  }

  override def sparkInputs(obj: LogisticRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: LogisticRegressionModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
