package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.function.{UserDefinedFunction => MleapUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait UserDefinedFunctionConverters {
  import TypeConverters._

  implicit def udfToSpark(udf: MleapUDF): UserDefinedFunction = {
    UserDefinedFunction(f = udf.f,
      dataType = udf.returnType,
      inputTypes = Some(udf.inputs.map(sparkType)))
  }
}
object UserDefinedFunctionConverters extends UserDefinedFunctionConverters
