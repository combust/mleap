package org.apache.spark.sql.mleap

import ml.combust.mleap.core.types
import ml.combust.mleap.core.types.{DataTypeSpec, SchemaSpec}
import ml.combust.mleap.runtime
import ml.combust.mleap.runtime.function.{UserDefinedFunction => MleapUDF}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait UserDefinedFunctionConverters {
  import TypeConverters._

  implicit def udfToSpark(udf: MleapUDF): UserDefinedFunction = {
    UserDefinedFunction(f = convertFunction(udf),
      dataType = mleapToSparkTypeSpec(udf.output),
      inputTypes = Some(sparkInputs(udf.inputs)))
  }

  private def convertFunction(udf: MleapUDF): AnyRef = {
    val c = udf.inputs.map(converter)
      udf.inputs.length match {
      case 0 =>
        udf.f
      case 1 =>
        val f = udf.f.asInstanceOf[(Any) => Any]
        (a1: Any) => f(c.head(a1))
      case 2 =>
        val f = udf.f.asInstanceOf[(Any, Any) => Any]
        (a1: Any, a2: Any) => f(c.head(a1), c(1)(a2))
      case 3 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any) => Any]
        (a1: Any, a2: Any, a3: Any) => f(c.head(a1), c(1)(a2), c(2)(a3))
      case 4 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any, Any) => Any]
        (a1: Any, a2: Any, a3: Any, a4: Any) => f(c.head(a1), c(1)(a2), c(2)(a3), c(3)(a4))
      case 5 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        (a1: Any, a2: Any, a3: Any, a4: Any, a5: Any) => f(c.head(a1), c(1)(a2), c(2)(a3), c(3)(a4), c(4)(a5))
    }
  }

  private def converter(spec: types.TypeSpec): (Any) => Any = spec match {
    case DataTypeSpec(dt) =>
      if(dt.isNullable) {
        (v) => Option[Any](v)
      } else { identity }
    case SchemaSpec(dts) =>
      val converters: Seq[(Any) => Any] = dts.map {
        dt =>
          if (dt.isNullable) {
            (v: Any) => Option[Any](v)
          } else {
            (v: Any) => v
          }
      }

      (v: Any) => {
        val values = converters.zip(v.asInstanceOf[Row].toSeq).map {
          case (c, value) => c(value)
        }

        runtime.Row(values: _*)
      }
  }

  private def sparkInputs(inputs: Seq[types.TypeSpec]): Seq[DataType] = {
    inputs.map(mleapToSparkTypeSpec)
  }
}
object UserDefinedFunctionConverters extends UserDefinedFunctionConverters
