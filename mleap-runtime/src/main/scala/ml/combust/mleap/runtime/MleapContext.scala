package ml.combust.mleap.runtime

import com.typesafe.config.{Config, ConfigFactory}
import ml.combust.mleap.runtime.types.CustomType
import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 10/25/16.
  */
object MleapContext {
  lazy val defaultContext: MleapContext = MleapContext()

  def apply(): MleapContext = {
    val cl = Thread.currentThread().getContextClassLoader
    MleapContext(ConfigFactory.load(), cl)
  }

  def apply(config: Config): MleapContext = apply(config, Thread.currentThread().getContextClassLoader)

  def apply(config: Config, cl: ClassLoader): MleapContext = {
    val context = new MleapContext()
    config.getStringList("ml.combust.mleap.context.customTypes").asScala.foldLeft(context) {
      (ctx, klazz) =>
        val ct = cl.loadClass(klazz).newInstance().asInstanceOf[CustomType[_]]
        ctx.withCustomType(ct)
    }
  }
}

case class MleapContext(customTypes: Map[String, CustomType[_]] = Map(),
                        customTypeAliases: Map[String, CustomType[_]] = Map()) {
  def withCustomType[T](customType: CustomType[T]): MleapContext = {
    copy(customTypes = customTypes + (customType.klazz.getCanonicalName -> customType),
      customTypeAliases = customTypeAliases + (customType.name -> customType))
  }

  def hasCustomType(klazz: String): Boolean = customTypes.contains(klazz)
  def customTypeForClass[T](klazz: String): CustomType[T] = customTypes(klazz).asInstanceOf[CustomType[T]]

  def hasCustomTypeAlias(alias: String): Boolean = customTypeAliases.contains(alias)
  def customTypeForAlias[T](alias: String): CustomType[T] = customTypeAliases(alias).asInstanceOf[CustomType[T]]
}
