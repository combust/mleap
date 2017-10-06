package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{BinaryOperation, MathBinaryModel}
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MathBinary

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinaryOp extends MleapOp[MathBinary, MathBinaryModel] {
  override val Model: OpModel[MleapContext, MathBinaryModel] = new OpModel[MleapContext, MathBinaryModel] {
    override val klazz: Class[MathBinaryModel] = classOf[MathBinaryModel]

    override def opName: String = Bundle.BuiltinOps.feature.math_binary

    override def store(model: Model, obj: MathBinaryModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("operation", Value.string(obj.operation.name)).
        withValue("da", obj.da.map(Value.double)).
        withValue("db", obj.db.map(Value.double))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MathBinaryModel = {
      MathBinaryModel(operation = BinaryOperation.forName(model.value("operation").getString),
        da = model.getValue("da").map(_.getDouble),
        db = model.getValue("db").map(_.getDouble))
    }
  }

  override def model(node: MathBinary): MathBinaryModel = node.model
}
