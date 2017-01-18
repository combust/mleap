package org.apache.spark.ml.bundle

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/26/16.
  */
object SparkBundleContext {
  implicit lazy val defaultContext: SparkBundleContext = SparkBundleContext(None, Some(classOf[SparkBundleContext].getClassLoader))

  def apply(dataset: Option[DataFrame] = None): SparkBundleContext = apply(dataset, None)

  def apply(dataset: Option[DataFrame], clOption: Option[ClassLoader]): SparkBundleContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[SparkBundleContext].getCanonicalName))
    apply(dataset, BundleRegistry("ml.combust.mleap.spark.registry.default", Some(cl)))
  }
}

case class SparkBundleContext(dataset: Option[DataFrame],
                              override val bundleRegistry: BundleRegistry) extends HasBundleRegistry {
  def withDataset(dataset: DataFrame): SparkBundleContext = copy(dataset = Some(dataset))
}
