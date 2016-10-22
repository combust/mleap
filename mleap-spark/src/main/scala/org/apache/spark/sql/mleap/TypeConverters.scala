package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.reflection.MleapReflection
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType
import ml.combust.mleap.runtime.types.{DataType => MleapDataType}
import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  implicit def sparkType(dataType: MleapDataType): DataType = {
    ScalaReflection.schemaFor(MleapReflection.scalaType(dataType)).dataType
  }
}
object TypeConverters extends TypeConverters
