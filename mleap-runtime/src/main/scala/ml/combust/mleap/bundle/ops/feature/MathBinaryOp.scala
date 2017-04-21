package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.{BinaryOperation, MathBinaryModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MathBinary

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinaryOp extends OpNode[MleapContext, MathBinary, MathBinaryModel] {
  override val Model: OpModel[MleapContext, MathBinaryModel] = new OpModel[MleapContext, MathBinaryModel] {
    override val klazz: Class[MathBinaryModel] = classOf[MathBinaryModel]

    override def opName: String = Bundle.BuiltinOps.feature.math_binary

    override def store(model: Model, obj: MathBinaryModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("operation", Value.string(obj.operation.name)).
        withAttr("da", obj.da.map(Value.double)).
        withAttr("db", obj.db.map(Value.double))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MathBinaryModel = {
      MathBinaryModel(operation = BinaryOperation.forName(model.value("operation").getString),
        da = model.getValue("da").map(_.getDouble),
        db = model.getValue("db").map(_.getDouble))
    }
  }

  override val klazz: Class[MathBinary] = classOf[MathBinary]

  override def name(node: MathBinary): String = node.uid

  override def model(node: MathBinary): MathBinaryModel = node.model

  override def load(node: Node, model: MathBinaryModel)
                   (implicit context: BundleContext[MleapContext]): MathBinary = {
    MathBinary(uid = node.name,
      inputA = node.shape.getInput("input_a").map(_.name),
      inputB = node.shape.getInput("input_b").map(_.name),
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: MathBinary)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withInput(node.inputA, "input_a").
      withInput(node.inputB, "input_b").
      withStandardOutput(node.outputCol)
  }
}
