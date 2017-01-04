package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.{BinaryLogisticRegressionModel, LogisticRegressionModel, ProbabilisticLogisticsRegressionModel}
import ml.combust.mleap.runtime.transformer.classification.LogisticRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.{Matrices, Vectors}

/**
  * Created by hollinwilkins on 8/24/16.
  */
class LogisticRegressionOp extends OpNode[MleapContext, LogisticRegression, LogisticRegressionModel] {
  override val Model: OpModel[MleapContext, LogisticRegressionModel] = new OpModel[MleapContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val m = model.withAttr("num_classes", Value.long(obj.numClasses))
      if(obj.isMultinomial) {
        val mm = obj.multinomialModel
        val cm = mm.coefficientMatrix
        m.withAttr("coefficient_matrix", Value.tensor[Double](cm.toArray, Seq(cm.numRows, cm.numCols))).
          withAttr("intercept_vector", Value.doubleVector(mm.interceptVector.toArray)).
          withAttr("thresholds", mm.thresholds.map(_.toSeq).map(Value.doubleList))
      } else {
        m.withAttr("coefficients", Value.doubleVector(obj.binaryModel.coefficients.toArray)).
          withAttr("intercept", Value.double(obj.binaryModel.intercept)).
          withAttr("threshold", Value.double(obj.binaryModel.threshold))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): LogisticRegressionModel = {
      val numClasses = model.value("num_classes").getLong

      val lm = if(numClasses > 2) {
        val tensor = model.value("coefficient_matrix").getTensor[Double]
        val Seq(rows, cols) = model.value("coefficient_matrix").bundleDataType.getTensor.dimensions
        val cm = Matrices.dense(numRows = rows, numCols = cols, tensor.toArray)

        ProbabilisticLogisticsRegressionModel(coefficientMatrix = cm,
          interceptVector = Vectors.dense(model.value("intercept_vector").getDoubleVector.toArray),
          thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray))
      } else {
        BinaryLogisticRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
          intercept = model.value("intercept").getDouble,
          threshold = model.value("threshold").getDouble)
      }

      LogisticRegressionModel(lm)
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
      rawPredictionCol = node.shape.getOutput("raw_prediction").map(_.name),
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: LogisticRegression): Shape = Shape().
    withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.rawPredictionCol, "raw_prediction").
    withOutput(node.probabilityCol, "probability")
}
