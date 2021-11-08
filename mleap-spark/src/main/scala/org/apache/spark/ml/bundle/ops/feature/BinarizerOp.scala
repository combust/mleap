package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.bundle.ops.OpsUtils
import org.apache.spark.sql.mleap.TypeConverters._
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.ml.param.ParamValidators

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerOp extends SimpleSparkOp[Binarizer] with MultiInOutFormatSparkOp[Binarizer] {
  override val Model: OpModel[SparkBundleContext, Binarizer] = new OpModel[SparkBundleContext, Binarizer] {
    override val klazz: Class[Binarizer] = classOf[Binarizer]

    override def opName: String = Bundle.BuiltinOps.feature.binarizer

    override def store(model: Model, obj: Binarizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      var result = {
        ParamValidators.checkSingleVsMultiColumnParams(obj, Seq(obj.inputCol), Seq(obj.inputCols))
        if(obj.isSet(obj.inputCols)) {
          val inputShapes = obj.getInputCols.map(i => sparkToMleapDataShape(dataset.schema(i), dataset): DataShape)
          model.withValue("input_shapes_list", Value.dataShapeList(inputShapes))
        } else {
          model.withValue("input_shapes", Value.dataShape(sparkToMleapDataShape(dataset.schema(obj.getInputCol), dataset)))
        }
      }

      if (obj.isSet(obj.threshold)) result = result.withValue("threshold", Value.double(obj.getThreshold))
      if (obj.isSet(obj.thresholds)) result = result.withValue("thresholds", Value.doubleList(obj.getThresholds))
      saveMultiInOutFormat(result, obj)
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Binarizer = {
      val threshold: Option[Double] = model.getValue("threshold").map(_.getDouble)
      val thresholds: Option[Seq[Double]] = model.getValue("thresholds").map(_.getDoubleList)
      val binarizer = new Binarizer()
      val obj = (threshold, thresholds) match {
        case (None, None) => throw new IllegalArgumentException("Neither threshold nor thresholds were found")
        case (Some(v), None) => binarizer.setThreshold(v)
        case (None, Some(v)) => binarizer.setThresholds(v.toArray)
        case (_, _) => throw new IllegalArgumentException("Both thresholds and threshold were found")
      }
      loadMultiInOutFormat(model, obj)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Binarizer): Binarizer = {
    val m = new Binarizer(uid)
    OpsUtils.copySparkStageParams(model, m)
    m
  }
}
