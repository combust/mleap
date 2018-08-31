package org.apache.spark.ml.bundle

import com.typesafe.config.ConfigFactory
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

  private val sparkVersion = org.apache.spark.SPARK_VERSION
  private val versionedRegistryKey: String = {
    if(sparkVersion.startsWith("2.0")) {
      "ml.combust.mleap.spark.registry.v20"
    } else if(sparkVersion.startsWith("2.1")) {
      "ml.combust.mleap.spark.registry.v21"
    } else if(sparkVersion.startsWith("2.2")) {
      "ml.combust.mleap.spark.registry.v22"
    } else if(sparkVersion.startsWith("2.3")) {
      "ml.combust.mleap.spark.registry.v23"
    } else {
      throw new IllegalStateException(s"unsupported Spark version: $sparkVersion")
    }
  }

  implicit lazy val defaultContext: SparkBundleContext = SparkBundleContext(None, Some(classOf[SparkBundleContext].getClassLoader))

  def apply(dataset: Option[DataFrame] = None): SparkBundleContext = apply(dataset, None)

  def apply(dataset: Option[DataFrame], clOption: Option[ClassLoader]): SparkBundleContext = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[SparkBundleContext].getCanonicalName))
    val config = ConfigFactory.load(cl)
    val registryKey = if(config.hasPath(DEFAULT_REGISTRY_KEY)) {
      DEFAULT_REGISTRY_KEY
    } else { versionedRegistryKey }
    apply(dataset, BundleRegistry(registryKey, Some(cl)))
  }
}

case class SparkBundleContext(dataset: Option[DataFrame],
                              override val bundleRegistry: BundleRegistry) extends HasBundleRegistry {
  def withDataset(dataset: DataFrame, registerHdfs: Boolean = true): SparkBundleContext = {
    val bundleRegistry2 = if (registerHdfs) {
      bundleRegistry.registerFileSystem(
        new HadoopBundleFileSystem(FileSystem.get(
          dataset.sqlContext.sparkSession.sparkContext.hadoopConfiguration)))
    } else { bundleRegistry }

    copy(dataset = Some(dataset),
      bundleRegistry = bundleRegistry2)
  }
}
