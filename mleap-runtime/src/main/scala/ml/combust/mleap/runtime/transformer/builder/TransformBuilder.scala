package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[B <: TransformBuilder[B]] extends Serializable {
  def withOutput(output: String, inputs: Selector *)
                 (udf: UserDefinedFunction): Try[B]

  def withOutputs(outputs: Seq[String], inputs: Selector *)
                 (udf: UserDefinedFunction): Try[B]
}
