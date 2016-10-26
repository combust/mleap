package ml.combust.mleap.bundle.ops.classification

import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.runtime.transformer.classification.SupportVectorMachine
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
class SupportVectorMachineOp extends OpNode[MleapContext, SupportVectorMachine, SupportVectorMachineModel] {
  override val Model: OpModel[MleapContext, SupportVectorMachineModel] = new OpModel[MleapContext, SupportVectorMachineModel] {
    override val klazz: Class[SupportVectorMachineModel] = classOf[SupportVectorMachineModel]

    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(context: BundleContext[MleapContext], model: Model, obj: SupportVectorMachineModel): Model = {
      model.withAttr("coefficients", Value.doubleVector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("num_classes", Value.long(2)).
        withAttr("threshold", obj.threshold.map(Value.double))
    }

    override def load(context: BundleContext[MleapContext], model: Model): SupportVectorMachineModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary SVM")
      }
      SupportVectorMachineModel(coefficients = Vectors.dense(model.value("coefficients").getDoubleVector.toArray),
        intercept = model.value("intercept").getDouble,
        threshold = model.getValue("threshold").map(_.getDouble))
    }
  }

  override val klazz: Class[SupportVectorMachine] = classOf[SupportVectorMachine]

  override def name(node: SupportVectorMachine): String = node.uid

  override def model(node: SupportVectorMachine): SupportVectorMachineModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: SupportVectorMachineModel): SupportVectorMachine = {
    SupportVectorMachine(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: SupportVectorMachine): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
