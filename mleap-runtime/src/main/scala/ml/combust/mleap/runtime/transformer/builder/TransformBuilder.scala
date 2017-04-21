package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.types.StructType

import scala.util.Try

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[B <: TransformBuilder[B]] extends Serializable {
  def withOutput(name: String, selectors: Selector *)
                (udf: UserDefinedFunction): Try[B]

  def withOutput(name: String, input: String, inputs: String *)
                (udf: UserDefinedFunction): Try[B] = {
    withOutput(name: String, Selector(input) +: inputs.map(Selector.apply): _*)(udf)
  }

  def schema: StructType
}
