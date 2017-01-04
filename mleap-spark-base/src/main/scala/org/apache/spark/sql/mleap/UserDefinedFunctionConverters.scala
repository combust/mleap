package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.function.{UserDefinedFunction => MleapUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import ml.combust.mleap.runtime.types
import ml.combust.mleap.runtime.types.{AnyType, ListType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait UserDefinedFunctionConverters {
  import TypeConverters._

  implicit def udfToSpark(udf: MleapUDF): UserDefinedFunction = {
    UserDefinedFunction(f = convertFunction(udf),
      dataType = sparkType(udf.returnType).get,
      inputTypes = sparkInputs(udf.inputs))
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

  private def converter(dataType: types.DataType): (Any) => Any = dataType match {
    case lt: ListType if lt.base == AnyType(false) => (row: Any) => row.asInstanceOf[Row].toSeq
    case _ => identity
  }

  private def sparkInputs(inputs: Seq[types.DataType]): Option[Seq[DataType]] = {
    inputs.foldLeft(Option(Seq[DataType]())) {
      case (optI, dt) =>
        optI.flatMap { i => sparkType(dt).map { sdt => i :+ sdt } }
    }
  }
}
object UserDefinedFunctionConverters extends UserDefinedFunctionConverters
