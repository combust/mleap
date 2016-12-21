package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.util.ParamUtil
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
class SupportVectorMachineOp extends OpNode[SparkBundleContext, SVMModel, SVMModel] {
  override val Model: OpModel[SparkBundleContext, SVMModel] = new OpModel[SparkBundleContext, SVMModel] {
    override val klazz: Class[SVMModel] = classOf[SVMModel]

    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(model: Model, obj: SVMModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.model.weights.toArray)).
        withAttr("intercept", Value.double(obj.model.intercept)).
        withAttr("num_classes", Value.long(2)).
        withAttr("threshold", obj.get(obj.threshold).map(Value.double))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): SVMModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("only binary logistic regression supported in Spark")
      }

      val svm = new classification.SVMModel(weights = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)
      val svmModel = new SVMModel(uid = "", model = svm)
      model.getValue("threshold").
        map(t => svmModel.setThreshold(t.getDouble)).
        getOrElse(svmModel)
    }
  }

  override val klazz: Class[SVMModel] = classOf[SVMModel]

  override def name(node: SVMModel): String = node.uid

  override def model(node: SVMModel): SVMModel = node

  override def load(node: Node, model: SVMModel)
                   (implicit context: BundleContext[SparkBundleContext]): SVMModel = {
    val svm = new SVMModel(uid = node.name,
      model = model.model).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    ParamUtil.setOptional(svm, model, svm.threshold, model.threshold)
    node.shape.getOutput("probability").map(s => svm.setProbabilityCol(s.name)).getOrElse(svm)
  }

  override def shape(node: SVMModel): Shape = {
    val rawPrediction = if(node.isDefined(node.rawPredictionCol)) Some(node.getRawPredictionCol) else None
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None

    Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(rawPrediction, "raw_prediction").
      withOutput(probability, "probability")
  }
}
