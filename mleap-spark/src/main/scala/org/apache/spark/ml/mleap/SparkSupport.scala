package org.apache.spark.ml.mleap

import java.io.File

import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer._
import org.apache.spark.ml.bundle.SparkBundle
import org.apache.spark.ml.Transformer

/**
  * Created by hollinwilkins on 8/22/16.
  */
object SparkSupport {
  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit hr: HasBundleRegistry): Unit = {
      SparkBundle.writeTransformer(transformer, path, list, format)(hr)
    }
  }

  implicit class FileOps(path: File) {
    def deserializeBundleMeta()
                             (implicit hr: HasBundleRegistry): BundleMeta = BundleSerializer(path).readMeta()

    def deserializeBundle()
                         (implicit hr: HasBundleRegistry): (Bundle, Transformer) = SparkBundle.readTransformer(path)
  }
}
