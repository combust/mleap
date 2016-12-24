package ml.combust.mleap.bundle

import java.io.File

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer._
import ml.combust.mleap.runtime.MleapContext
import resource._

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 8/23/16.
  */
object MleapBundle {
  def readTransformer(path: File)
                     (implicit context: MleapContext): Try[Bundle[Transformer]] = {
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
                      (implicit context: MleapContext): Try[Bundle[Transformer]] = {
    (for(serializer <- managed(BundleSerializer(context, path))) yield {
      val bundle = Bundle(transformer.uid,
        format,
        transformer)
      serializer.write(bundle)
      bundle
    }).either.either match {
      case Right(bundle) => Try(bundle)
      case Left(errors) => Failure(errors.head)
    }
  }
}
