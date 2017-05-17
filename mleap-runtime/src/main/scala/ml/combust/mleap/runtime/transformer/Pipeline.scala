package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.StructField

import scala.util.{Failure, Success, Try}

/**
 * Created by hwilkins on 11/8/15.
 */
case class Pipeline(uid: String = Transformer.uniqueName("pipeline"),
                    transformers: Seq[Transformer]) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override def close(): Unit = transformers.foreach(_.close())

  override def getFields(): Try[Seq[StructField]] = {
    val missingSchemaTransformers = transformers.map(transformer => (transformer.uid, transformer.getFields()))
                                .filter(transformerWithSchema => transformerWithSchema._2.isFailure)
                                .map(transformerWithSchema => transformerWithSchema._1)
    missingSchemaTransformers match {
      case Nil => Success(transformers.map(transformer => transformer.getFields().get).flatten)
      case x :: _ => Failure(new RuntimeException(s"Cannot determine schema for transformers ${missingSchemaTransformers}"))
    }
  }
}
