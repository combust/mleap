package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.VectorSlicerModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.VectorSlicer

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorSlicerOp extends MleapOp[VectorSlicer, VectorSlicerModel] {
  override val Model: OpModel[MleapContext, VectorSlicerModel] = new OpModel[MleapContext, VectorSlicerModel] {
    override val klazz: Class[VectorSlicerModel] = classOf[VectorSlicerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_slicer

    override def store(model: Model, obj: VectorSlicerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (names, namedIndices) = obj.namedIndices.unzip
      model.withValue("indices", Value.longList(obj.indices.map(_.toLong).toSeq)).
        withValue("names", Value.stringList(names)).
        withValue("named_indices", Value.intList(namedIndices)).
        withValue("input_size", Value.int(obj.inputSize))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorSlicerModel = {
      val names = model.value("names").getStringList
      val namedIndices = model.value("named_indices").getIntList
      val namedIndicesMap = names.zip(namedIndices)
      VectorSlicerModel(indices = model.value("indices").getLongList.map(_.toInt).toArray,
        namedIndices = namedIndicesMap.toArray, inputSize = model.value("input_size").getInt)
    }
  }

  override def model(node: VectorSlicer): VectorSlicerModel = node.model
}
