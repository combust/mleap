package ml.combust.mleap.runtime

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.runtime.types.CustomType
import ml.combust.bundle

/**
  * Created by hollinwilkins on 10/25/16.
  */
object MleapContext {
  lazy val defaultContext: MleapContext = MleapContext()

  def apply(implicit hr: HasBundleRegistry = BundleRegistry("mleap")): MleapContext = {
    val context = new MleapContext(hr, Map(), Map())
    hr.bundleRegistry.customTypes.values.foldLeft(context) {
      (ctx, ct) => ctx.withCustomType(ct.asInstanceOf[bundle.custom.CustomType[Any]])
    }
  }
}

case class MleapContext private (hr: HasBundleRegistry,
                                 customTypes: Map[String, CustomType],
                                 customTypeAliases: Map[String, CustomType]) extends HasBundleRegistry {
  override def bundleRegistry: BundleRegistry = hr.bundleRegistry

  def withCustomType[T](customType: CustomType): MleapContext = {
    copy(customTypes = customTypes + (customType.klazz.getCanonicalName -> customType),
      customTypeAliases = customTypeAliases + (customType.name -> customType))
  }

  def hasCustomType(klazz: String): Boolean = customTypes.contains(klazz)
  def customTypeForClass[T](klazz: String): CustomType = customTypes(klazz).asInstanceOf[CustomType]

  def hasCustomTypeAlias(alias: String): Boolean = customTypeAliases.contains(alias)
  def customTypeForAlias[T](alias: String): CustomType = customTypeAliases(alias).asInstanceOf[CustomType]
}
