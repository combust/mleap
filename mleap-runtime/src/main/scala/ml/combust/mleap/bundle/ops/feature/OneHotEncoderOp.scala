package ml.combust.mleap.bundle.ops.feature

import ml.bundle.Socket
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{HandleInvalid, OneHotEncoderModel}
import ml.combust.mleap.core.types
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.OneHotEncoder
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 10/24/16.
  */
class OneHotEncoderOp extends MleapOp[OneHotEncoder, OneHotEncoderModel] {
  override val Model: OpModel[MleapContext, OneHotEncoderModel] = new OpModel[MleapContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("category_sizes", Value.intList(obj.categorySizes))
        .withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))
        .withValue("drop_last", Value.boolean(obj.dropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneHotEncoderModel = {
      if (model.getValue("size").nonEmpty) {
        // Old version of 1HE.
        OneHotEncoderModel(categorySizes = Array(model.value("size").getLong.toInt),
          dropLast = model.value("drop_last").getBoolean)
      } else {
        // New version of 1HE.
        OneHotEncoderModel(
          categorySizes = model.value("category_sizes").getIntList.toArray,
          handleInvalid = HandleInvalid.fromString(model.value("handle_invalid").getString),
          dropLast = model.value("drop_last").getBoolean)
      }
    }
  }

  override def load(node: Node, model: OneHotEncoderModel)
                   (implicit context: BundleContext[MleapContext]): OneHotEncoder = {
    val ns = node.shape.getInput(NodeShape.standardInputPort) match {
      // Old version of 1HE -- need to translate serialized port names to new expectation (input -> input0)
      case Some(_) ⇒ translateLegacyShape(node.shape)

      // New version of 1HE
      case None ⇒ node.shape
    }
    klazz.getConstructor(classOf[String], classOf[types.NodeShape], Model.klazz).
      newInstance(node.name, ns.asBundle: types.NodeShape, model)
  }

  override def model(node: OneHotEncoder): OneHotEncoderModel = node.model

  private def translateLegacyShape(ns: NodeShape): NodeShape = {
    val i = ns.getInput(NodeShape.standardInputPort).get
    val o = ns.getOutput(NodeShape.standardOutputPort).get
    NodeShape(inputs = Seq(Socket(i.port + "0", i.name)), outputs = Seq(Socket(o.port + "0", o.name)))
  }
}
