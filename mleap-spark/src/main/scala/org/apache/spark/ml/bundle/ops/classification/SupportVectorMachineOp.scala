package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SupportVectorMachineOp extends OpNode[SVMModel, SVMModel] {
  override val Model: OpModel[SVMModel] = new OpModel[SVMModel] {
    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(context: BundleContext, model: WritableModel, obj: SVMModel): WritableModel = {
      val m = model.withAttr(Attribute("coefficients", Value.doubleVector(obj.model.weights.toArray))).
        withAttr(Attribute("intercept", Value.double(obj.model.intercept))).
        withAttr(Attribute("num_classes", Value.long(2)))
      obj.get(obj.threshold).
        map(t => m.withAttr(Attribute("threshold", Value.double(t)))).
        getOrElse(m)
    }

    override def load(context: BundleContext, model: ReadableModel): SVMModel = {
      // TODO: better error
      if(model.value("num_classes").getLong != 2) {
        throw new Error("Only binary logistic regression supported in Spark")
      }

      val svm = new classification.SVMModel(weights = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble)
      val svmModel = new SVMModel(uid = "", model = svm)
      model.getValue("threshold").
        map(t => svmModel.setThreshold(t.getDouble)).
        getOrElse(svmModel)
    }
  }

  override def name(node: SVMModel): String = node.uid

  override def model(node: SVMModel): SVMModel = node

  override def load(context: BundleContext, node: ReadableNode, model: SVMModel): SVMModel = {
    new SVMModel(uid = node.name,
      model = model.model).copy(model.extractParamMap()).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: SVMModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
