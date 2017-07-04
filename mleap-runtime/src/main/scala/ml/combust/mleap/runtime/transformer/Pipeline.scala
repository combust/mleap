package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class Pipeline(override val uid: String = Transformer.uniqueName("pipeline"),
                    transformers: Seq[Transformer]) extends Transformer {
  override val shape: NodeShape = NodeShape(Seq(), Seq())

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override def close(): Unit = transformers.foreach(_.close())
}
