package ml.combust.mleap.runtime

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.bundle.util.ClassLoaderUtil

/**
  * Created by hollinwilkins on 10/25/16.
  */
object MleapContext {
  implicit lazy val defaultContext: MleapContext = MleapContext(Some(classOf[MleapContext].getClassLoader))

  def apply(): MleapContext = apply(None)

  def apply(clOption: Option[ClassLoader]): MleapContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[MleapContext].getCanonicalName))
    apply(BundleRegistry("ml.combust.mleap.registry.default", Some(cl)))
  }
}

case class MleapContext(registry: BundleRegistry) extends HasBundleRegistry {
  override def bundleRegistry: BundleRegistry = registry
  val classLoader: ClassLoader = registry.classLoader
}
