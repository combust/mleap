package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LogisticRegressionOp extends OpNode[SparkBundleContext, LogisticRegressionModel, LogisticRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LogisticRegressionModel] = new OpModel[SparkBundleContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: LogisticRegressionModel): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("num_classes", Value.long(obj.numClasses)).
        withAttr("threshold", obj.get(obj.threshold).map(Value.double))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): LogisticRegressionModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("Only binary logistic regression supported in Spark")
      }

      val lr = new LogisticRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)

      model.getValue("threshold").
        map(t => lr.setThreshold(t.getDouble)).
        getOrElse(lr)
    }
  }

  override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

  override def name(node: LogisticRegressionModel): String = node.uid

  override def model(node: LogisticRegressionModel): LogisticRegressionModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: LogisticRegressionModel): LogisticRegressionModel = {
    val lr = new LogisticRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).copy(model.extractParamMap).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    node.shape.getOutput("probability").map(p => lr.setProbabilityCol(p.name)).getOrElse(lr)
  }

  override def shape(node: LogisticRegressionModel): Shape = {
    val s = Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction")
    node.get(node.probabilityCol).map(p => s.withOutput(p, "probability")).
      getOrElse(s)
  }
}
