package ml.combust.mleap.runtime.bundle.ops.classification

import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.runtime.transformer.classification.SupportVectorMachine
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object SupportVectorMachineOp extends OpNode[SupportVectorMachine, SupportVectorMachineModel] {
  override val Model: OpModel[SupportVectorMachineModel] = new OpModel[SupportVectorMachineModel] {
    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(context: BundleContext, model: Model, obj: SupportVectorMachineModel): Model = {
      model.withAttr(Attribute("coefficients", Value.doubleVector(obj.coefficients.toArray))).
        withAttr(Attribute("intercept", Value.double(obj.intercept))).
        withAttr(Attribute("num_classes", Value.long(2))).
        withAttr(obj.threshold.map(t => Attribute("threshold", Value.double(t))))
    }

    override def load(context: BundleContext, model: Model): SupportVectorMachineModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new Error("MLeap only supports binary SVM")
      } // TODO: Better error
      SupportVectorMachineModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble,
        threshold = model.getValue("threshold").map(_.getDouble))
    }
  }

  override def name(node: SupportVectorMachine): String = node.uid

  override def model(node: SupportVectorMachine): SupportVectorMachineModel = node.model

  override def load(context: BundleContext, node: Node, model: SupportVectorMachineModel): SupportVectorMachine = {
    SupportVectorMachine(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: SupportVectorMachine): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
