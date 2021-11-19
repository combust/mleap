package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Node, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.MapEntrySelectorModel
import ml.combust.mleap.core.types.BasicType
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters.{mleapToBundleBasicType, bundleToMleapBasicType, bundleToMleapNodeShape}
import ml.combust.mleap.runtime.transformer.feature.MapEntrySelector

class MapEntrySelectorOp extends MleapOp[MapEntrySelector[_, _], MapEntrySelectorModel[_, _]]{
  override val Model: OpModel[MleapContext, MapEntrySelectorModel[_, _]] = new OpModel[MleapContext, MapEntrySelectorModel[_, _]] {

    override val klazz: Class[MapEntrySelectorModel[_, _]] = classOf[MapEntrySelectorModel[_, _]]
    override def opName: String = Bundle.BuiltinOps.feature.map_entry_selector

    override def store(model: Model, obj: MapEntrySelectorModel[_, _])(implicit context: BundleContext[MleapContext]): Model = {
      val keyType = mleapToBundleBasicType(obj.inputSchema.getField("key").get.dataType.base)
      val valueType = mleapToBundleBasicType(obj.outputSchema.getField("output").get.dataType.base)

      model.withValue("key_type", Value.basicType(keyType))
        .withValue("value_type", Value.basicType(valueType))
        .withValue("default_value", Value.anyAsType(obj.defaultValue, valueType))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): MapEntrySelectorModel[_, _] = {
      val keyBasicType = bundleToMleapBasicType(model.value("key_type").getBasicType)
      val valueBasicType = model.value("value_type").getBasicType
      val defaultValue = model.value("default_value").getAnyFromType(valueBasicType)
      (keyBasicType, bundleToMleapBasicType(valueBasicType)) match {
        case (BasicType.String, BasicType.Double) => MapEntrySelectorModel[String, Double](defaultValue)
        case (k, v) => throw new UnsupportedOperationException(s"Can not load bundle of types $k, $v")
      }
    }
  }

  override def load(node: Node, model: MapEntrySelectorModel[_, _])(implicit context: BundleContext[MleapContext]): MapEntrySelector[_, _] = {
    val keyType = model.inputSchema.getField("key").get.dataType.base
    val valueType = model.outputSchema.getField("output").get.dataType.base
    (keyType, valueType) match {
      case (BasicType.String, BasicType.Double) => new MapEntrySelector[String, Double](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (k, v) => throw new UnsupportedOperationException(s"Can not load bundle of types $k, $v")
    }
  }

  override def model(node: MapEntrySelector[_, _]): MapEntrySelectorModel[_, _] = node.model
}
