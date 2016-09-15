package ml.combust.mleap.runtime.transformer

import java.util.UUID

import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/** Companion class for transformer.
  */
object Transformer {
  /** Generate a unique name with a base string.
    *
    * @param base base string
    * @return unique name from base string
    */
  def uniqueName(base: String): String = s"${base}_${UUID.randomUUID().toString}"
}

/** Trait for implementing an MLeap transformer.
  */
trait Transformer {
  /** Unique identifier for this transformer.
    */
  val uid: String

  /** Transform a builder using this MLeap transformer.
    *
    * @param builder builder to transform
    * @tparam TB underlying class of builder
    * @return try new builder with transformation applied
    */
  def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB]
}
