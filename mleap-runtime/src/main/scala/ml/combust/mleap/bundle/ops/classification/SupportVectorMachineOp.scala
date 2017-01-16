package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.runtime.transformer.classification.SupportVectorMachine
import ml.combust.bundle.op.{OpModel, OpNode}
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

    override def store(model: Model, obj: SupportVectorMachineModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("num_classes", Value.long(2)).
        withAttr("thresholds", obj.thresholds.map(_.toSeq).map(Value.doubleList))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): SupportVectorMachineModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary SVM")
      }
      SupportVectorMachineModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble,
        thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray))
    }
  }

  override val klazz: Class[SupportVectorMachine] = classOf[SupportVectorMachine]

  override def name(node: SupportVectorMachine): String = node.uid

  override def model(node: SupportVectorMachine): SupportVectorMachineModel = node.model

  override def load(node: Node, model: SupportVectorMachineModel)
                   (implicit context: BundleContext[MleapContext]): SupportVectorMachine = {
    SupportVectorMachine(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      rawPredictionCol = node.shape.getOutput("raw_prediction").map(_.name),
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: SupportVectorMachine): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.rawPredictionCol, "raw_prediction").
    withOutput(node.probabilityCol, "probability")
}
