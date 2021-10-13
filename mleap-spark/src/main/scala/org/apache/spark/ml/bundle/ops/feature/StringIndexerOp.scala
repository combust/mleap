package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.bundle.ops.OpsUtils
import org.apache.spark.ml.feature.StringIndexerModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class StringIndexerOp extends SimpleSparkOp[StringIndexerModel] with MultiInOutFormatSparkOp[StringIndexerModel] {
  override val Model: OpModel[SparkBundleContext, StringIndexerModel] = new OpModel[SparkBundleContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var result = model.
        withValue("labels_length", Value.int(obj.labelsArray.length)).
        withValue("handle_invalid", Value.string(obj.getHandleInvalid))
      obj.labelsArray.indices.foreach(
        i => result = result.withValue(s"labels_array_$i", Value.stringList(obj.labelsArray(i)))
      )
      saveMultiInOutFormat(result, obj)
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StringIndexerModel = {
      val label_length = model.getValue("labels_length").map(_.getInt).getOrElse(-1)
      val labelsArray: Array[Array[String]] = if (label_length == -1) {
        // backwards compatibility with spark v2
         Array[Array[String]](model.value("labels").getStringList.toArray)
      }
      else {
        val collectedLabels = new Array[Array[String]](label_length)
        for ( i <- 0 to label_length - 1) {
          collectedLabels(i) = model.value(s"labels_array_$i").getStringList.toArray
        }
        collectedLabels
      }

      val obj = new StringIndexerModel(labelsArray = labelsArray).
        setHandleInvalid(model.value("handle_invalid").getString)
      loadMultiInOutFormat(model, obj)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StringIndexerModel): StringIndexerModel = {
    val m = new StringIndexerModel(uid, model.labelsArray)
    OpsUtils.copySparkStageParams(model, m)
    m
  }
}
