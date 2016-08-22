package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.dsl._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LogisticRegressionOp extends OpNode[LogisticRegressionModel, LogisticRegressionModel] {
  override val Model: OpModel[LogisticRegressionModel] = new OpModel[LogisticRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext, model: WritableModel, obj: LogisticRegressionModel): WritableModel = {
      val m = model.withAttr(Attribute("coefficients", Value.doubleVector(obj.coefficients.toArray))).
        withAttr(Attribute("intercept", Value.double(obj.intercept))).
        withAttr(Attribute("num_classes", Value.long(obj.numClasses)))

      obj.get(obj.threshold).map(t => m.withAttr(Attribute("threshold", Value.double(t)))).
        getOrElse(m)
    }

    override def load(context: BundleContext, model: ReadableModel): LogisticRegressionModel = {
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

  override def load(context: BundleContext, node: ReadableNode, model: LogisticRegressionModel): LogisticRegressionModel = {
    new LogisticRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).copy(model.extractParamMap).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: LogisticRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
