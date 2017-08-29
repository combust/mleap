package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/22/16.
  */
class VectorAssemblerOp extends MleapOp[VectorAssembler, VectorAssemblerModel] {
  override val Model: OpModel[MleapContext, VectorAssemblerModel] = new OpModel[MleapContext, VectorAssemblerModel] {
    override val klazz: Class[VectorAssemblerModel] = classOf[VectorAssemblerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(model: Model, obj: VectorAssemblerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("input_shapes", Value.dataShapeList(obj.inputShapes.map(mleapToBundleShape)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorAssemblerModel = {
      val inputShapes = model.value("input_shapes").getDataShapeList.map(bundleToMleapShape)
      VectorAssemblerModel(inputShapes)
    }
  }

  override def model(node: VectorAssembler): VectorAssemblerModel = node.model
}
