package org.apache.spark.ml.bundle

import ml.bundle.hdfs.HadoopBundleFileSystem
import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.ClassLoaderUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/26/16.
  */
object SparkBundleContext {
  val DEFAULT_REGISTRY_KEY: String = "ml.combust.mleap.spark.registry.default"

  implicit lazy val defaultContext: SparkBundleContext = SparkBundleContext(None, Some(classOf[SparkBundleContext].getClassLoader))

  def apply(dataset: Option[DataFrame] = None): SparkBundleContext = apply(dataset, None)

  def apply(dataset: Option[DataFrame], clOption: Option[ClassLoader]): SparkBundleContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[SparkBundleContext].getCanonicalName))
    apply(dataset, BundleRegistry(DEFAULT_REGISTRY_KEY, Some(cl)))
  }
}

case class SparkBundleContext(dataset: Option[DataFrame],
                              override val bundleRegistry: BundleRegistry) extends HasBundleRegistry {
  def withDataset(dataset: DataFrame): SparkBundleContext = withDataset(dataset, registerHdfs = true)

    def withDataset(dataset: DataFrame, registerHdfs: Boolean): SparkBundleContext = {
    val bundleRegistry2 = if (registerHdfs) {
      bundleRegistry.registerFileSystem(
        new HadoopBundleFileSystem(FileSystem.get(
          dataset.sqlContext.sparkSession.sparkContext.hadoopConfiguration)))
    } else { bundleRegistry }

    copy(dataset = Some(dataset),
      bundleRegistry = bundleRegistry2)
  }
}
