package org.apache.spark.ml.bundle.extension.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
class SupportVectorMachineOp extends SimpleSparkOp[SVMModel] {
  override val Model: OpModel[SparkBundleContext, SVMModel] = new OpModel[SparkBundleContext, SVMModel] {
    override val klazz: Class[SVMModel] = classOf[SVMModel]

    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(model: Model, obj: SVMModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val thresholds = if(obj.isSet(obj.thresholds)) {
        Some(obj.getThresholds)
      } else None

      model.withValue("coefficients", Value.vector(obj.model.weights.toArray)).
        withValue("intercept", Value.double(obj.model.intercept)).
        withValue("num_classes", Value.long(2)).
        withValue("thresholds", thresholds.map(_.toSeq).map(Value.doubleList))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): SVMModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("only binary logistic regression supported in Spark")
      }

      val weights = Vectors.dense(model.value("coefficients").getTensor[Double].toArray)
      val svm = new org.apache.spark.mllib.classification.SVMModel(
        weights = weights,
        intercept = model.value("intercept").getDouble
      )
      val svmModel = new SVMModel(uid = "", model = svm)
      model.getValue("thresholds").
        map(t => svmModel.setThresholds(t.getDoubleList.toArray)).
        getOrElse(svmModel)
    }
  }

  override def sparkLoad(uid: String,
                         shape: NodeShape,
                         model: SVMModel): SVMModel = {
    val m = new SVMModel(uid = uid, model = model.model)
    if(model.isDefined(model.thresholds)) {
      m.setThresholds(model.getThresholds)
    }
    m
  }

  override def sparkInputs(obj: SVMModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: SVMModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
