package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LogisticRegressionOp extends OpNode[LogisticRegressionModel, LogisticRegressionModel] {
  override val Model: OpModel[LogisticRegressionModel] = new OpModel[LogisticRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext, model: Model, obj: LogisticRegressionModel): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("num_classes", Value.long(obj.numClasses)).
        withAttr("threshold", obj.get(obj.threshold).map(Value.double))
    }

    override def load(context: BundleContext, model: Model): LogisticRegressionModel = {
      // TODO: better error
      if(model.value("num_classes").getLong != 2) {
        throw new Error("Only binary logistic regression supported in Spark")
      }

      val lr = new LogisticRegressionModel(uid = "",
        coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)

      model.getValue("threshold").
        map(t => lr.setThreshold(t.getDouble)).
        getOrElse(lr)
    }
  }

  override def name(node: LogisticRegressionModel): String = node.uid

  override def model(node: LogisticRegressionModel): LogisticRegressionModel = node

  override def load(context: BundleContext, node: Node, model: LogisticRegressionModel): LogisticRegressionModel = {
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
