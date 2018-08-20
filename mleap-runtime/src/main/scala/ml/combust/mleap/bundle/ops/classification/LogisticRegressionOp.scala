package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.{BinaryLogisticRegressionModel, LogisticRegressionModel, ProbabilisticLogisticsRegressionModel}
import ml.combust.mleap.runtime.transformer.classification.LogisticRegression
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.linalg.{Matrices, Vectors}

/**
  * Created by hollinwilkins on 8/24/16.
  */
class LogisticRegressionOp extends MleapOp[LogisticRegression, LogisticRegressionModel] {
  override val Model: OpModel[MleapContext, LogisticRegressionModel] = new OpModel[MleapContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val m = model.withValue("num_classes", Value.long(obj.numClasses))
      if(obj.isMultinomial) {
        val mm = obj.multinomialModel
        val cm = mm.coefficientMatrix
        m.withValue("coefficient_matrix", Value.tensor[Double](DenseTensor(cm.toArray, Seq(cm.numRows, cm.numCols)))).
          withValue("intercept_vector", Value.vector(mm.interceptVector.toArray)).
          withValue("thresholds", mm.thresholds.map(_.toSeq).map(Value.doubleList))
      } else {
        m.withValue("coefficients", Value.vector(obj.binaryModel.coefficients.toArray)).
          withValue("intercept", Value.double(obj.binaryModel.intercept)).
          withValue("threshold", Value.double(obj.binaryModel.threshold))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): LogisticRegressionModel = {
      val numClasses = model.value("num_classes").getLong

      val lm = if(numClasses > 2) {
        val tensor = model.value("coefficient_matrix").getTensor[Double]
        val cm = Matrices.dense(numRows = tensor.dimensions.head, numCols = tensor.dimensions(1), tensor.toArray)

        ProbabilisticLogisticsRegressionModel(coefficientMatrix = cm,
          interceptVector = Vectors.dense(model.value("intercept_vector").getTensor[Double].toArray),
          thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray))
      } else {
        // default threshold is 0.5 for both Spark and Scikit-learn
        val threshold = model.getValue("threshold")
                              .map(value => value.getDouble)
                              .getOrElse(0.5)
        BinaryLogisticRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
          intercept = model.value("intercept").getDouble,
          threshold = threshold)
      }

      LogisticRegressionModel(lm)
    }
  }

  override def model(node: LogisticRegression): LogisticRegressionModel = node.model
}
