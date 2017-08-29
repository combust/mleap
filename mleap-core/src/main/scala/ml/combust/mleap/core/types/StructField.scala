package ml.combust.mleap.core.types

import scala.language.implicitConversions

/**
 * Created by hwilkins on 10/23/15.
 */
object StructField {
  implicit def apply(t: (String, DataType)): StructField = {
    StructField(t._1, t._2)
  }
}

case class StructField(name: String,
                       dataType: DataType) extends Serializable
