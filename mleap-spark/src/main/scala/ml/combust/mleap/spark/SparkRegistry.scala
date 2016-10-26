package ml.combust.mleap.spark

import ml.combust.bundle.serializer.BundleRegistry
import org.apache.spark.ml.bundle.ops

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SparkRegistry {
  implicit val defaultRegistry: BundleRegistry = create()

  def create(): BundleRegistry = BundleRegistry("spark")
}
