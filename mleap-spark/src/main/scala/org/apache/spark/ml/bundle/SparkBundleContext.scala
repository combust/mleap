package org.apache.spark.ml.bundle

import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/26/16.
  */
case class SparkBundleContext(dataset: Option[DataFrame] = None)
