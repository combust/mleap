package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.types.{AnyType, DataType, StructField}
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.UserDefinedFunction

import scala.util.{Failure, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[B <: TransformBuilder[B]] extends Serializable {
  def withOutput(name: String, inputs: String *)
                (f: UserDefinedFunction): Try[B]
}
