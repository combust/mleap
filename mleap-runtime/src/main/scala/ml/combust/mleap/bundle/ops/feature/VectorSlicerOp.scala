package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.VectorSlicerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.VectorSlicer

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorSlicerOp extends OpNode[MleapContext, VectorSlicer, VectorSlicerModel] {
  override val Model: OpModel[MleapContext, VectorSlicerModel] = new OpModel[MleapContext, VectorSlicerModel] {
    override val klazz: Class[VectorSlicerModel] = classOf[VectorSlicerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_slicer

    override def store(model: Model, obj: VectorSlicerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("indices", Value.longList(obj.indices.map(_.toLong).toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorSlicerModel = {
      VectorSlicerModel(indices = model.value("indices").getLongList.map(_.toInt).toArray)
    }
  }

  override val klazz: Class[VectorSlicer] = classOf[VectorSlicer]

  override def name(node: VectorSlicer): String = node.uid

  override def model(node: VectorSlicer): VectorSlicerModel = node.model

  override def load(node: Node, model: VectorSlicerModel)
                   (implicit context: BundleContext[MleapContext]): VectorSlicer = {
    VectorSlicer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: VectorSlicer)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withStandardIO(node.inputCol, node.outputCol)
  }
}
