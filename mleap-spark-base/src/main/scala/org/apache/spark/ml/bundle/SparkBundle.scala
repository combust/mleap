package org.apache.spark.ml.bundle

import java.io.File

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.serializer.{BundleSerializer, SerializationFormat}
import org.apache.spark.ml.Transformer

/**
  * Created by hollinwilkins on 9/19/16.
  */
object SparkBundle {
  def readTransformer(path: File)
                     (implicit context: SparkBundleContext = SparkBundleContext()): Bundle[Transformer] = {
    BundleSerializer(context, path).read()
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit context: SparkBundleContext = SparkBundleContext()): Unit = {
    BundleSerializer(context, path).write(Bundle(name = transformer.uid,
      format = format,
      root = transformer))
  }
}