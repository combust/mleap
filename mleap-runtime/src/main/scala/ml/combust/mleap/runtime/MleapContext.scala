package ml.combust.mleap.runtime

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.runtime.types.CustomType
import ml.combust.bundle
import ml.combust.bundle.util.ClassLoaderUtil

/**
  * Created by hollinwilkins on 10/25/16.
  */
object MleapContext {
  implicit lazy val defaultContext: MleapContext = MleapContext(Some(classOf[MleapContext].getClassLoader))

  def apply(): MleapContext = apply(None)

  def apply(clOption: Option[ClassLoader]): MleapContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[MleapContext].getCanonicalName))
    apply(BundleRegistry("mleap", Some(cl)))
  }

  def apply(registry: BundleRegistry): MleapContext = {
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
}
