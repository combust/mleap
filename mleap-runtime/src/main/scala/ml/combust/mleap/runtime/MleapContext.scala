package ml.combust.mleap.runtime

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.runtime.types.CustomType
import ml.combust.bundle
import ml.combust.mleap.runtime.serialization.FrameSerializerContext
import ml.combust.mleap.runtime.serialization.json.DefaultFrameSerializer

/**
  * Created by hollinwilkins on 10/25/16.
  */
object MleapContext {
  lazy val defaultContext: MleapContext = MleapContext()

  def apply(registry: BundleRegistry = BundleRegistry("mleap")): MleapContext = {
    val context = new MleapContext(registry, Map(), Map())
    registry.customTypes.values.foldLeft(context) {
      (ctx, ct) => ctx.withCustomType(ct.asInstanceOf[bundle.custom.CustomType[Any]])
    }
  }
}

case class MleapContext private (registry: BundleRegistry,
                                 customTypes: Map[String, CustomType],
                                 customTypeAliases: Map[String, CustomType]) extends HasBundleRegistry {
  override def bundleRegistry: BundleRegistry = registry
  val classLoader: ClassLoader = registry.classLoader

  def withCustomType[T](customType: CustomType): MleapContext = {
    copy(customTypes = customTypes + (customType.klazz.getCanonicalName -> customType),
      customTypeAliases = customTypeAliases + (customType.name -> customType))
  }

  def hasCustomType(klazz: String): Boolean = customTypes.contains(klazz)
  def customTypeForClass(klazz: String): CustomType = customTypes(klazz)

  def hasCustomTypeAlias(alias: String): Boolean = customTypeAliases.contains(alias)
  def customTypeForAlias(alias: String): CustomType = customTypeAliases(alias)

  def serializer(format: String = "ml.combust.mleap.runtime.serialization.json"): FrameSerializerContext = {
    FrameSerializerContext(format)(this)
  }
}
