package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.function.{UserDefinedFunction => MleapUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import ml.combust.mleap.runtime.types
import org.apache.spark.sql.types.DataType

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait UserDefinedFunctionConverters {
  import TypeConverters._

  implicit def udfToSpark(udf: MleapUDF): UserDefinedFunction = {
    UserDefinedFunction(f = udf.f,
      dataType = sparkType(udf.returnType).get,
      inputTypes = sparkInputs(udf.inputs))
  }

  private def sparkInputs(inputs: Seq[types.DataType]): Option[Seq[DataType]] = {
    inputs.foldLeft(Option(Seq[DataType]())) {
      case (optI, dt) =>
        optI.flatMap { i => sparkType(dt).map { sdt => i :+ sdt } }
    }
  }
}
object UserDefinedFunctionConverters extends UserDefinedFunctionConverters
