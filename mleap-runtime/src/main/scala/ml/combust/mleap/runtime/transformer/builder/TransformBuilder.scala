package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.types.{DataType, StructField}
import ml.combust.mleap.runtime.Row

import scala.util.{Failure, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[B <: TransformBuilder[B]] extends Serializable {
  def withInput(name: String): Try[(B, Int)]
  def withInput(name: String, dataType: DataType): Try[(B, Int)]

  def withOutput(name: String, dataType: DataType)
                (o: (Row) => Any): Try[B]
  def withOutputs(fields: Seq[StructField])
                 (o: (Row) => Row): Try[B]
}
