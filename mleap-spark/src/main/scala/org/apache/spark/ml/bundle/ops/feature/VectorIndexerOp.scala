package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.core.feature.HandleInvalid
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.VectorIndexerModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorIndexerOp extends SimpleSparkOp[VectorIndexerModel] {
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
          m.withValue(s"${key}_keys", Value.doubleList(vKeys)).
            withValue(s"${key}_values", Value.longList(vValues.map(_.toLong)))
      }.withValue("keys", Value.longList(keys.map(_.toLong).toSeq)).
        withValue("num_features", Value.long(obj.numFeatures)).
        withValue("handle_invalid", Value.string(obj.getHandleInvalid))
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
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).getOrElse(HandleInvalid.default.asParamString)

      val m = new VectorIndexerModel(uid = "",
        numFeatures = model.value("num_features").getLong.toInt,
        categoryMaps = categoryMaps)
      m.set(m.handleInvalid, handleInvalid)
      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: VectorIndexerModel): VectorIndexerModel = {
    val m = new VectorIndexerModel(uid = uid, numFeatures = model.numFeatures, categoryMaps = model.categoryMaps)
    m.set(m.handleInvalid, model.getHandleInvalid)
    m
  }

  override def sparkInputs(obj: VectorIndexerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: VectorIndexerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
