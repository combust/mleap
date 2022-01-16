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
        case (BasicType.String, BasicType.Double) => MapEntrySelectorModel[String, Double](defaultValue.asInstanceOf[Double])
        case (BasicType.String, BasicType.Float) => MapEntrySelectorModel[String, Float](defaultValue.asInstanceOf[Float])
        case (BasicType.String, BasicType.Long) => MapEntrySelectorModel[String, Long](defaultValue.asInstanceOf[Long])
        case (BasicType.String, BasicType.Int) => MapEntrySelectorModel[String, Int](defaultValue.asInstanceOf[Int])
        case (BasicType.String, BasicType.Short) => MapEntrySelectorModel[String, Short](defaultValue.asInstanceOf[Short])
        case (BasicType.String, BasicType.String) => MapEntrySelectorModel[String, String](defaultValue.asInstanceOf[String])
        case (BasicType.String, BasicType.Byte) => MapEntrySelectorModel[String, Byte](defaultValue.asInstanceOf[Byte])
        case (BasicType.String, BasicType.Boolean) => MapEntrySelectorModel[String, Boolean](defaultValue.asInstanceOf[Boolean])
        case (k, v) => throw new UnsupportedOperationException(s"Can not load bundle of types $k, $v")
      }
    }
  }

  override def load(node: Node, model: MapEntrySelectorModel[_, _])(implicit context: BundleContext[MleapContext]): MapEntrySelector[_, _] = {
    val keyType = model.inputSchema.getField("key").get.dataType.base
    val valueType = model.outputSchema.getField("output").get.dataType.base
    (keyType, valueType) match {
      case (BasicType.String, BasicType.Double) => new MapEntrySelector[String, Double](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Float) => new MapEntrySelector[String, Float](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Long) => new MapEntrySelector[String, Long](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Int) => new MapEntrySelector[String, Int](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Short) => new MapEntrySelector[String, Short](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.String) => new MapEntrySelector[String, String](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Byte) => new MapEntrySelector[String, Byte](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (BasicType.String, BasicType.Boolean) => new MapEntrySelector[String, Boolean](node.name, bundleToMleapNodeShape(node.shape.asBundle), model)
      case (k, v) => throw new UnsupportedOperationException(s"Can not load bundle of types $k, $v")
    }
  }

  override def model(node: MapEntrySelector[_, _]): MapEntrySelectorModel[_, _] = node.model
}
