package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.bundle.util.ParamUtil
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LogisticRegressionOpV20 extends OpNode[SparkBundleContext, LogisticRegressionModel, LogisticRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LogisticRegressionModel] = new OpModel[SparkBundleContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(obj.numClasses == 2, "This op only supports binary logistic regression")

      val m = model.withAttr("num_classes", Value.long(obj.numClasses))
      m.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("threshold", Value.double(obj.getThreshold))
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

  override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

  override def name(node: LogisticRegressionModel): String = node.uid

  override def model(node: LogisticRegressionModel): LogisticRegressionModel = node

  override def load(node: Node, model: LogisticRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): LogisticRegressionModel = {
    var lr = new LogisticRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    ParamUtil.setOptional(lr, model, lr.threshold, model.threshold)
    lr = node.shape.getOutput("probability").map(p => lr.setProbabilityCol(p.name)).getOrElse(lr)
    node.shape.getOutput("raw_prediction").map(rp => lr.setRawPredictionCol(rp.name)).getOrElse(lr)
  }

  override def shape(node: LogisticRegressionModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    val rawPrediction = if(node.isDefined(node.rawPredictionCol)) Some(node.getRawPredictionCol) else None
    val rawPredictionType = if(node.isDefined(node.rawPredictionCol)) fieldType(node.getRawPredictionCol, dataset) else None
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None
    val probabilityType = if(node.isDefined(node.probabilityCol)) fieldType(node.getProbabilityCol, dataset) else None

    Shape().withInput(node.getFeaturesCol, "features", fieldType(node.getFeaturesCol, dataset)).
      withOutput(node.getPredictionCol, "prediction", fieldType(node.getPredictionCol, dataset)).
      withOutput(rawPrediction, "raw_prediction", rawPredictionType).
      withOutput(probability, "probability", probabilityType)
  }
}
