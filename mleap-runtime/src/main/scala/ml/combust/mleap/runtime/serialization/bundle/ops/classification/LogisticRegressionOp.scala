package ml.combust.mleap.runtime.serialization.bundle.ops.classification

import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.transformer.classification.LogisticRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/24/16.
  */
object LogisticRegressionOp extends OpNode[LogisticRegression, LogisticRegressionModel] {
  override val Model: OpModel[LogisticRegressionModel] = new OpModel[LogisticRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext, model: WritableModel, obj: LogisticRegressionModel): WritableModel = {
      val m = model.withAttr(Attribute("coefficients", Value.doubleVector(obj.coefficients.toArray))).
        withAttr(Attribute("intercept", Value.double(obj.intercept))).
        withAttr(Attribute("num_classes", Value.long(2)))
      obj.threshold.
        map(t => m.withAttr(Attribute("threshold", Value.double(t)))).
        getOrElse(m)
    }

    override def load(context: BundleContext, model: ReadableModel): LogisticRegressionModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new Error("MLeap only supports binary logistic regression")
      } // TODO: Better error
      LogisticRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble,
        threshold = model.getValue("threshold").map(_.getDouble))
    }
  }

  override def name(node: LogisticRegression): String = node.uid

  override def model(node: LogisticRegression): LogisticRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: LogisticRegressionModel): LogisticRegression = {
    LogisticRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: LogisticRegression): Shape = {
    val s = Shape().withInput(node.featuresCol, "features").
      withOutput(node.predictionCol, "prediction")
    node.probabilityCol.map(p => s.withOutput(p, "probability")).
      getOrElse(s)
  }
}
