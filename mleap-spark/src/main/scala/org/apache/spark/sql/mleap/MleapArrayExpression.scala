package org.apache.spark.sql.mleap

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, ObjectType}

/**
  * Created by hollinwilkins on 10/22/16.
  */
object MleapArrayExpression {
  def col(cols: Seq[Column]): Column = Column(MleapArrayExpression(cols.map(_.expr)))
}

case class MleapArrayExpression(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function array")

  override def dataType: DataType = {
    new ObjectType(classOf[Array[Any]])
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"this.$values = null;")

    ev.copy(code =
      s"""
      final boolean ${ev.isNull} = false;
      this.$values = new Object[${children.size}];""" +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        children.zipWithIndex.map { case (e, i) =>
          val eval = e.genCode(ctx)
          eval.code +
            s"""
            if (${eval.isNull}) {
              $values[$i] = null;
            } else {
              $values[$i] = ${eval.value};
            }
           """
        }) +
      s"""
        final Object[] ${ev.value} = $values;
        this.$values = null;
      """)
  }

  override def prettyName: String = "mleap_array"
}

