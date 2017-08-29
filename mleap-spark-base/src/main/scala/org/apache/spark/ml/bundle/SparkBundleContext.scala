package org.apache.spark.ml.bundle

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/26/16.
  */
object SparkBundleContext {
  private val sparkVersion = org.apache.spark.SPARK_VERSION
  private val versionedRegistryKey: String = {
    if(sparkVersion.startsWith("2.0")) {
      "ml.combust.mleap.spark.registry.v20"
    } else if(sparkVersion.startsWith("2.1")) {
      "ml.combust.mleap.spark.registry.v21"
    } else if(sparkVersion.startsWith("2.2")) {
      "ml.combust.mleap.spark.registry.v22"
    } else {
      throw new IllegalStateException(s"unsupported Spark version: $sparkVersion")
    }
  }

  implicit lazy val defaultContext: SparkBundleContext = SparkBundleContext(None, Some(classOf[SparkBundleContext].getClassLoader))

  def apply(dataset: Option[DataFrame] = None): SparkBundleContext = apply(dataset, None)

  def apply(dataset: Option[DataFrame], clOption: Option[ClassLoader]): SparkBundleContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[SparkBundleContext].getCanonicalName))
    apply(dataset, BundleRegistry(versionedRegistryKey, Some(cl)))
  }
}

case class SparkBundleContext(dataset: Option[DataFrame],
                              override val bundleRegistry: BundleRegistry) extends HasBundleRegistry {
  def withDataset(dataset: DataFrame): SparkBundleContext = copy(dataset = Some(dataset))
}
