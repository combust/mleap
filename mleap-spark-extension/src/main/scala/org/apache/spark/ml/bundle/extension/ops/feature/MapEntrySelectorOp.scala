package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.runtime.types.BundleTypeConverters.{bundleToMleapBasicType, mleapToBundleBasicType}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.mleap.feature.MapEntrySelectorModel
import org.apache.spark.sql.mleap.TypeConverters.{mleapBasicTypeToSparkType, sparkTypeToMleapBasicType}
import org.apache.spark.sql.types.{DoubleType, StringType}

class MapEntrySelectorOp extends SimpleSparkOp[MapEntrySelectorModel[_,_]] {
  override  val Model: OpModel[SparkBundleContext, MapEntrySelectorModel[_,_]] = new OpModel[SparkBundleContext, MapEntrySelectorModel[_,_]] {
    override val klazz: Class[MapEntrySelectorModel[_,_]] = classOf[MapEntrySelectorModel[_,_]]
    override def opName: String = Bundle.BuiltinOps.feature.map_entry_selector

    override def store(model: Model, obj: MapEntrySelectorModel[_,_])(implicit context: BundleContext[SparkBundleContext]): Model = {
      val keyType = mleapToBundleBasicType(sparkTypeToMleapBasicType(obj.keyType))
      val valueType = mleapToBundleBasicType(sparkTypeToMleapBasicType(obj.valueType))
      model.withValue("key_type", Value.basicType(keyType))
        .withValue("value_type", Value.basicType(valueType))
        .withValue("default_value", Value.anyAsType(obj.getDefaultValue, valueType))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MapEntrySelectorModel[_,_] = {
      val keySparkType = mleapBasicTypeToSparkType(bundleToMleapBasicType(model.value("key_type").getBasicType))
      val valueBasicType = model.value("value_type").getBasicType
      val valueSparkType = mleapBasicTypeToSparkType(bundleToMleapBasicType(valueBasicType))
      val defaultValue = model.value("default_value").getAnyFromType(valueBasicType)
      val result = (keySparkType, valueSparkType) match {
        case(StringType, DoubleType) => new MapEntrySelectorModel[String, Double](keySparkType, valueSparkType)
      }
      result.setDefaultValue(defaultValue)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MapEntrySelectorModel[_, _]): MapEntrySelectorModel[_,_] = {
    val result = (model.keyType, model.valueType) match {
      case(StringType, DoubleType) => new MapEntrySelectorModel[String, Double](model.keyType, model.valueType, uid)
    }
    result.setDefaultValue(model.getDefaultValue)
  }

  override def sparkInputs(obj: MapEntrySelectorModel[_,_]): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol, "key" -> obj.keyCol)
  }

  override def sparkOutputs(obj: MapEntrySelectorModel[_,_]): Seq[ParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
