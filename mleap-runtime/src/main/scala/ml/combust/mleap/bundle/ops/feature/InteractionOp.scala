package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Interaction
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 4/26/17.
  */
class InteractionOp extends MleapOp[Interaction, InteractionModel] {
  override val Model: OpModel[MleapContext, InteractionModel] = new OpModel[MleapContext, InteractionModel] {
    override val klazz: Class[InteractionModel] = classOf[InteractionModel]

    override def opName: String = Bundle.BuiltinOps.feature.interaction

    override def store(model: Model, obj: InteractionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val m = model.withValue("input_shapes", Value.dataShapeList(obj.inputShapes.map(mleapToBundleShape))).
        withValue("num_inputs", Value.int(obj.featuresSpec.length))

      obj.featuresSpec.zipWithIndex.foldLeft(m) {
        case (m2, (numFeatures, index)) => m2.withValue(s"num_features$index", Value.intList(numFeatures))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): InteractionModel = {
      val numInputs = model.value("num_inputs").getInt
      val spec = (0 until numInputs).map {
        index => model.value(s"num_features$index").getIntList.toArray
      }.toArray

      val inputShapes = model.value("input_shapes").getDataShapeList.map(bundleToMleapShape)

      InteractionModel(spec, inputShapes)
    }
  }

  override def model(node: Interaction): InteractionModel = node.model
}

