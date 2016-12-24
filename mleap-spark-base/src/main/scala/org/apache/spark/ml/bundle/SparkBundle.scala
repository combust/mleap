package org.apache.spark.ml.bundle

import java.io.File

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.serializer.{BundleSerializer, SerializationFormat}
import org.apache.spark.ml.Transformer
import resource._

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 9/19/16.
  */
object SparkBundle {
  def readTransformer(path: File)
                     (implicit context: SparkBundleContext = SparkBundleContext()): Try[Bundle[Transformer]] = {
    (for(serializer <- managed(BundleSerializer(context, path))) yield {
      serializer.read[Transformer]()
    }).either.either match {
      case Right(bundle) => Try(bundle)
      case Left(errors) => Failure(errors.head)
    }
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit context: SparkBundleContext = SparkBundleContext()): Try[Bundle[Transformer]] = {
    (for(serializer <- managed(BundleSerializer(context, path))) yield {
      val bundle = Bundle(name = transformer.uid,
        format = format,
        root = transformer)
      serializer.write(bundle)
      bundle
    }).either.either match {
      case Right(bundle) => Try(bundle)
      case Left(errors) => Failure(errors.head)
    }
  }
}