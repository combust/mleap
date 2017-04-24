package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.VectorIndexerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.VectorIndexer

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorIndexerOp extends OpNode[MleapContext, VectorIndexer, VectorIndexerModel] {
  override val Model: OpModel[MleapContext, VectorIndexerModel] = new OpModel[MleapContext, VectorIndexerModel] {
    override val klazz: Class[VectorIndexerModel] = classOf[VectorIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_indexer

    override def store(model: Model, obj: VectorIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val keys = obj.categoryMaps.keys
      val mapValues = obj.categoryMaps.toSeq.map {
        case (key, value) =>
          val (vKeys, vValues) = value.toSeq.unzip
          (s"key_$key", vKeys, vValues)
      }

      mapValues.foldLeft(model) {
        case (m, (key, vKeys, vValues)) =>
          m.withAttr(s"${key}_keys", Value.doubleList(vKeys)).
            withAttr(s"${key}_values", Value.longList(vValues.map(_.toLong)))
      }.withAttr("keys", Value.longList(keys.map(_.toLong).toSeq)).
        withAttr("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorIndexerModel = {
      val keys = model.value("keys").getLongList.map(_.toInt)
      val categoryMaps = keys.map {
        key =>
          val kKeys = model.value(s"key_${key}_keys").getDoubleList
          val kValues = model.value(s"key_${key}_values").getLongList.map(_.toInt)
          (key, kKeys.zip(kValues).toMap)
      }.toMap

      VectorIndexerModel(numFeatures = model.value("num_features").getLong.toInt,
        categoryMaps = categoryMaps)
    }
  }

  override val klazz: Class[VectorIndexer] = classOf[VectorIndexer]

  override def name(node: VectorIndexer): String = node.uid

  override def model(node: VectorIndexer): VectorIndexerModel = node.model

  override def load(node: Node, model: VectorIndexerModel)
                   (implicit context: BundleContext[MleapContext]): VectorIndexer = {
    VectorIndexer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: VectorIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
