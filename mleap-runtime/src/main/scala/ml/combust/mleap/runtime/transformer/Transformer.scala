package ml.combust.mleap.runtime.transformer

import java.util.UUID

import ml.combust.mleap.runtime.transformer.builder.{LeapFrameBuilder, TransformBuilder}
import ml.combust.mleap.runtime.LeapFrame

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
object Transformer {
  def uniqueName(base: String): String = s"${base}_${UUID.randomUUID().toString}"
}

trait Transformer {
  val uid: String
  def transform[L <: LeapFrame[L]](frame: L): Try[L] = build(LeapFrameBuilder(frame)).map(_.frame)
  def build[TB <: TransformBuilder[TB]](builder: TB): Try[TB]
}
