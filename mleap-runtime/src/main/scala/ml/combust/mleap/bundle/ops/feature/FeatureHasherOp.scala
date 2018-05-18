package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.FeatureHasherModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.FeatureHasher
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.combust.mleap.core.types.DataType

class FeatureHasherOp extends MleapOp[FeatureHasher, FeatureHasherModel] {
  override val Model: OpModel[MleapContext, FeatureHasherModel] = new OpModel[MleapContext, FeatureHasherModel] {
    override val klazz: Class[FeatureHasherModel] = classOf[FeatureHasherModel]

    override def opName: String = Bundle.BuiltinOps.feature.feature_hasher

    override def store(model: Model, obj: FeatureHasherModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("num_features", Value.long(obj.numFeatures))
        .withValue("categorical_cols", Value.stringList(obj.categoricalCols))
        .withValue("input_shapes", Value.dataShapeList(obj.inputTypes.map(_.shape).map(mleapToBundleShape)))
        .withValue("basic_types", Value.basicTypeList(obj.inputTypes.map(_.base).map(mleapToBundleBasicType)))
        .withValue("input_names", Value.stringList(obj.inputNames))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): FeatureHasherModel = {
      val shapes = model.value("input_shapes").getDataShapeList.map(bundleToMleapShape)
      val types = model.value("basic_types").getBasicTypeList.map(bundleToMleapBasicType)
      val inputTypes = shapes.zip(types).map(tup ⇒ DataType(tup._2, tup._1))

      FeatureHasherModel(numFeatures = model.value("num_features").getLong.toInt,
        categoricalCols = model.value("categorical_cols").getStringList,
        inputTypes = inputTypes,
        inputNames = model.value("input_names").getStringList
      )
    }
  }

  override def model(node: FeatureHasher): FeatureHasherModel = node.model
}
