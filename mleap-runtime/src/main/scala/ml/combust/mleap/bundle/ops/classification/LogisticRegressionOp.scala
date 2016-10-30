package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.transformer.classification.LogisticRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/24/16.
  */
class LogisticRegressionOp extends OpNode[MleapContext, LogisticRegression, LogisticRegressionModel] {
  override val Model: OpModel[MleapContext, LogisticRegressionModel] = new OpModel[MleapContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("num_classes", Value.long(2)).
        withAttr("threshold", obj.threshold.map(Value.double))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): LogisticRegressionModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary logistic regression")
      }
      LogisticRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble,
        threshold = model.getValue("threshold").map(_.getDouble))
    }
  }

  override val klazz: Class[LogisticRegression] = classOf[LogisticRegression]

  override def name(node: LogisticRegression): String = node.uid

  override def model(node: LogisticRegression): LogisticRegressionModel = node.model

  override def load(node: Node, model: LogisticRegressionModel)
                   (implicit context: BundleContext[MleapContext]): LogisticRegression = {
    LogisticRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: LogisticRegression): Shape = Shape().
    withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.probabilityCol, "probability")
}
