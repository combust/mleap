package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.{BinaryOperation, MathBinaryModel}
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.mleap.feature.MathBinary

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
    val mb = new MathBinary(uid = node.name,
      model = model).setOutputCol(node.shape.standardOutput.name)
    node.shape.getInput("input_a").foreach(a => mb.setInputA(a.name))
    node.shape.getInput("input_b").foreach(b => mb.setInputB(b.name))
    mb
  }

  override def shape(node: MathBinary): Shape = {
    var shape = Shape().withStandardOutput(node.getOutputCol)

    if(node.isSet(node.inputA)) {
      shape = shape.withInput(node.getInputA, "input_a")
    }

    if(node.isSet(node.inputB)) {
      shape = shape.withInput(node.getInputB, "input_b")
    }

    shape
  }
}
