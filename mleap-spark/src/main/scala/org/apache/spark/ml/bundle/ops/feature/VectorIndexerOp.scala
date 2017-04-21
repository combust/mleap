package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorIndexerOp extends OpNode[SparkBundleContext, VectorIndexerModel, VectorIndexerModel] {
  override val Model: OpModel[SparkBundleContext, VectorIndexerModel] = new OpModel[SparkBundleContext, VectorIndexerModel] {
    override val klazz: Class[VectorIndexerModel] = classOf[VectorIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_indexer

    override def store(model: Model, obj: VectorIndexerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
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
                     (implicit context: BundleContext[SparkBundleContext]): VectorIndexerModel = {
      val keys = model.value("keys").getLongList.map(_.toInt)
      val categoryMaps = keys.map {
        key =>
          val kKeys = model.value(s"key_${key}_keys").getDoubleList
          val kValues = model.value(s"key_${key}_values").getLongList.map(_.toInt)
          (key, kKeys.zip(kValues).toMap)
      }.toMap

      new VectorIndexerModel(uid = "",
        numFeatures = model.value("num_features").getLong.toInt,
        categoryMaps = categoryMaps)
    }
  }

  override val klazz: Class[VectorIndexerModel] = classOf[VectorIndexerModel]

  override def name(node: VectorIndexerModel): String = node.uid

  override def model(node: VectorIndexerModel): VectorIndexerModel = node

  override def load(node: Node, model: VectorIndexerModel)
                   (implicit context: BundleContext[SparkBundleContext]): VectorIndexerModel = {
    new VectorIndexerModel(uid = node.name,
      numFeatures = model.numFeatures,
      categoryMaps = model.categoryMaps).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorIndexerModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withStandardIO(node.getInputCol, fieldType(node.getInputCol, dataset),
      node.getOutputCol, fieldType(node.getOutputCol, dataset))
  }
}
