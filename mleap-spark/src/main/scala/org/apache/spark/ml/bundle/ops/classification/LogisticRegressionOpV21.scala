package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Matrices, Vectors}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class LogisticRegressionOpV21 extends SimpleSparkOp[LogisticRegressionModel] {
  override val Model: OpModel[SparkBundleContext, LogisticRegressionModel] = new OpModel[SparkBundleContext, LogisticRegressionModel] {
    override val klazz: Class[LogisticRegressionModel] = classOf[LogisticRegressionModel]

    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(model: Model, obj: LogisticRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val m = model.withValue("num_classes", Value.long(obj.numClasses))
      if(obj.numClasses > 2) {
        val cm = obj.coefficientMatrix
        val thresholds = if(obj.isSet(obj.thresholds)) {
          Some(obj.getThresholds)
        } else None
        m.withValue("coefficient_matrix", Value.tensor[Double](DenseTensor(cm.toArray, Seq(cm.numRows, cm.numCols)))).
          withValue("intercept_vector", Value.vector(obj.interceptVector.toArray)).
          withValue("thresholds", thresholds.map(_.toSeq).map(Value.doubleList))
      } else {
        m.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
          withValue("intercept", Value.double(obj.intercept)).
          withValue("threshold", Value.double(obj.getThreshold))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): LogisticRegressionModel = {
      val numClasses = model.value("num_classes").getLong
      val r = if(numClasses > 2) {
        val cmTensor = model.value("coefficient_matrix").getTensor[Double]
        val coefficientMatrix = Matrices.dense(cmTensor.dimensions.head, cmTensor.dimensions(1), cmTensor.toArray)
        val lr = new LogisticRegressionModel(uid = "",
          coefficientMatrix = coefficientMatrix,
          interceptVector = Vectors.dense(model.value("intercept_vector").getTensor[Double].toArray),
          numClasses = numClasses.toInt,
          isMultinomial = true)
        model.getValue("thresholds").
          map(t => lr.setThresholds(t.getDoubleList.toArray)).
          getOrElse(lr)
      } else {
        val lr = new LogisticRegressionModel(uid = "",
          coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
          intercept = model.value("intercept").getDouble)
        model.getValue("threshold").
          map(t => lr.setThreshold(t.getDouble)).
          getOrElse(lr)
      }
      r
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: LogisticRegressionModel): LogisticRegressionModel = {
    val numClasses = model.numClasses
    val r = if (numClasses > 2) {
        val lr = new LogisticRegressionModel(uid = uid,
          coefficientMatrix = model.coefficientMatrix,
          interceptVector = model.interceptVector,
          numClasses = numClasses,
          isMultinomial = true)
        if(model.isDefined(model.thresholds)) { lr.setThresholds(model.getThresholds) }
        lr
    } else {
        val lr = new LogisticRegressionModel(uid = uid,
          coefficientMatrix = model.coefficientMatrix,
          interceptVector = model.interceptVector,
          numClasses = numClasses,
          isMultinomial = false)
        if(model.isDefined(model.threshold)) { lr.setThreshold(model.getThreshold) }
        lr
    }
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
