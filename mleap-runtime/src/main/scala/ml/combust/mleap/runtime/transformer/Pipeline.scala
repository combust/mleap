package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.StructField

import scala.util.{Success, Try}

/**
 * Created by hwilkins on 11/8/15.
 */
case class Pipeline(uid: String = Transformer.uniqueName("pipeline"),
                    transformers: Seq[Transformer]) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override def close(): Unit = transformers.foreach(_.close())

  override def getSchema(): Try[Seq[StructField]] = Success(Seq())
}
