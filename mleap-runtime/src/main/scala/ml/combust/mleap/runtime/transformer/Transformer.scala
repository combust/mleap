package ml.combust.mleap.runtime.transformer

import java.util.UUID

import ml.combust.mleap.core.types.{NodeShape, StructType}
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, UserDefinedFunction}
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
trait Transformer extends AutoCloseable {
  /** Unique identifier for this transformer.
    */
  val uid: String

  /** Shape of inputs/outputs */
  val shape: NodeShape

  lazy val inputSchema: StructType = shape.inputSchema
  lazy val outputSchema: StructType = shape.outputSchema

  /** Transform a builder using this MLeap transformer.
    *
    * @param builder builder to transform
    * @tparam TB underlying class of builder
    * @return try new builder with transformation applied
    */
  def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB]

  /** Get the full schema of this transformer (inputs ++ outputs).
    *
    * @return full schema of this transformer
    */
  def schema: StructType = {
    StructType(inputSchema.fields ++ outputSchema.fields).get
  }

  override def close(): Unit = { /* do nothing by default */ }
}

trait BaseTransformer extends Transformer {
  val exec: UserDefinedFunction

  val inputs: Seq[String] = inputSchema.fields.map(_.name)
  val selectors: Seq[Selector] = inputs.map(FieldSelector)
}

trait SimpleTransformer extends BaseTransformer {
  lazy val typedExec: UserDefinedFunction = {
    exec.withInputs(inputSchema).
      withOutput(outputSchema.fields.head.dataType)
  }
  val output: String = outputSchema.fields.head.name

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(output, selectors: _*)(typedExec)
  }
}

trait MultiTransformer extends BaseTransformer {
  lazy val typedExec: UserDefinedFunction = {
    exec.withInputs(inputSchema).
      withOutput(outputSchema)
  }
  val outputs: Seq[String] = outputSchema.fields.map(_.name)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutputs(outputs, selectors: _*)(typedExec)
  }
}
