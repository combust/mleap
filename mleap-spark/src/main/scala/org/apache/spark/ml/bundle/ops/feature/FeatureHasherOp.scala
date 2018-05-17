package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.FeatureHasher
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.sql.mleap.TypeConverters._

class FeatureHasherOp extends SimpleSparkOp[FeatureHasher] {
  override val Model: OpModel[SparkBundleContext, FeatureHasher] = new OpModel[SparkBundleContext, FeatureHasher] {
    override val klazz: Class[FeatureHasher] = classOf[FeatureHasher]

    override def opName: String = Bundle.BuiltinOps.feature.feature_hasher

    override def store(model: Model, obj: FeatureHasher)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val categoricals = if (obj.isSet(obj.categoricalCols)) {
        obj.getCategoricalCols
      } else {
        Array.empty[String]
      }
      val dataset = context.context.dataset.get
      val inputShapes = obj.getInputCols.map(i ⇒ sparkToMleapDataShape(dataset.schema(i), dataset): DataShape)
      val basicTypes = obj.getInputCols.map(i ⇒ sparkFieldToMleapField(dataset, dataset.schema(i)).dataType.base).map(mleapToBundleBasicType)

      model.withValue("num_features", Value.long(obj.getNumFeatures))
        .withValue("categorical_cols", Value.stringList(categoricals))
        .withValue("input_shapes", Value.dataShapeList(inputShapes))
        .withValue("basic_types", Value.basicTypeList(basicTypes))
        .withValue("input_names", Value.stringList(obj.getInputCols))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): FeatureHasher = {
      new FeatureHasher(uid = "").setNumFeatures(model.value("num_features").getLong.toInt).
        setCategoricalCols(model.value("categorical_cols").getStringList.toArray)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: FeatureHasher): FeatureHasher = {
    new FeatureHasher(uid = uid)
  }

  override def sparkInputs(obj: FeatureHasher): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCols)
  }

  override def sparkOutputs(obj: FeatureHasher): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
