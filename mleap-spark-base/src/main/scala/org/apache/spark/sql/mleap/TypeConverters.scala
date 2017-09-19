package org.apache.spark.sql.mleap

import ml.combust.mleap.core.types
import ml.combust.mleap.core.types.{BasicType, Casting}
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{Matrices, Matrix, MatrixUDT, Vector, VectorUDT, Vectors}
import ml.combust.mleap.core.util.VectorConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  def sparkToMleapValue(dataType: DataType,
                        isNullable: Boolean): (Any) => Any = dataType match {
    case _: VectorUDT =>
      (v: Any) =>
        v.asInstanceOf[Vector]: Tensor[Double]
    case _: MatrixUDT =>
      (v: Any) =>
        v.asInstanceOf[Matrix]: Tensor[Double]
    case at: ArrayType if at.elementType == new VectorUDT =>
      (v: Any) =>
        val t = v.asInstanceOf[mutable.WrappedArray[Vector]]
        val s = t.head.size
        val values = t.flatMap(_.toArray).toArray
        DenseTensor(values, Seq(t.size, s))
    case StringType if isNullable =>
      (v: Any) => Option(v.asInstanceOf[String])
    case _ => (v) => v
  }

  def sparkFieldToMleapField(dataset: DataFrame,
                             field: StructField): (types.StructField, (Any) => Any) = {
    val dt = field.dataType match {
      case BooleanType => types.ScalarType.Boolean
      case ByteType => types.ScalarType.Byte
      case ShortType => types.ScalarType.Short
      case IntegerType => types.ScalarType.Int
      case LongType => types.ScalarType.Long
      case FloatType => types.ScalarType.Float
      case DoubleType => types.ScalarType.Double
      case StringType => types.ScalarType.String.setNullable(field.nullable)
      case ArrayType(ByteType, _) => types.ScalarType.ByteString
      case ArrayType(BooleanType, _) => types.ListType.Boolean
      case ArrayType(ShortType, _) => types.ListType.Short
      case ArrayType(IntegerType, _) => types.ListType.Int
      case ArrayType(LongType, _) => types.ListType.Long
      case ArrayType(FloatType, _) => types.ListType.Float
      case ArrayType(DoubleType, _) => types.ListType.Double
      case ArrayType(StringType, _) => types.ListType.String
      case ArrayType(ArrayType(ByteType, _), _) => types.ListType.ByteString
      case _: VectorUDT =>
        val size = dataset.select(field.name).head.getAs[Vector](0).size
        types.TensorType.Double(size)
      case _: MatrixUDT =>
        val m = dataset.select(field.name).head.getAs[Matrix](0)
        types.TensorType.Double(m.numRows, m.numCols)
      case ArrayType(elementType, _) if elementType == new VectorUDT =>
        val a = dataset.select(field.name).head.getAs[mutable.WrappedArray[Vector]](0)
        types.TensorType.Double(a.length, a.head.size)
    }

    (types.StructField(field.name, dt), sparkToMleapValue(field.dataType, field.nullable))
  }

  def mleapBasicTypeToSparkType(base: BasicType): DataType = base match {
    case BasicType.Boolean => BooleanType
    case BasicType.Byte => ByteType
    case BasicType.Short => ShortType
    case BasicType.Int => IntegerType
    case BasicType.Long => LongType
    case BasicType.Float => FloatType
    case BasicType.Double => DoubleType
    case BasicType.String => StringType
    case BasicType.ByteString => ArrayType(ByteType, containsNull = false)
  }

  def mleapToSparkValue(dataType: types.DataType): (Any) => Any = dataType match {
    case types.ScalarType(BasicType.String, true) => (v: Any) => v.asInstanceOf[Option[String]].orNull
    case types.ScalarType(_, true) => (v: Any) => v.asInstanceOf[Option[Any]].get
    case tt: types.TensorType =>
      if(tt.dimensions.isEmpty) {
        (v: Any) => v.asInstanceOf[Tensor[_]](0)
      } else if(tt.dimensions.size == 1) {
        if(tt.base == BasicType.Double) {
          (v: Any) => v.asInstanceOf[Tensor[Double]]: Vector
        } else {
          val c = Casting.basicCast(tt.base, BasicType.Double).asInstanceOf[(Any) => Double]
          (v: Any) => v.asInstanceOf[Tensor[_]].mapValues(c): Vector
        }
      } else if(tt.dimensions.size == 2) {
        if(tt.base == BasicType.Double) {
          (v: Any) => v.asInstanceOf[Tensor[Double]]: Matrix
        } else {
          val c = Casting.basicCast(tt.base, BasicType.Double).asInstanceOf[(Any) => Double]
          (v: Any) => v.asInstanceOf[Tensor[_]].mapValues(c): Matrix
        }
      } else {
        throw new IllegalArgumentException("cannot convert tensor for non-scalar, vector or matrix tensor")
      }
    case _ => (v: Any) => v
  }

  def mleapFieldToSparkField(field: types.StructField): (StructField, (Any) => Any) = {
    val dt = field.dataType match {
      case types.ScalarType(base, _) => mleapBasicTypeToSparkType(base)
      case types.ListType(base, _) => ArrayType(mleapBasicTypeToSparkType(base), containsNull = false)
      case tt: types.TensorType => mleapTensorToSpark(tt)
    }

    (StructField(field.name, dt), mleapToSparkValue(field.dataType))
  }

  def mleapTensorToSpark(tt: types.TensorType): DataType = {
    assert(TypeConverters.VECTOR_BASIC_TYPES.contains(tt.base),
      s"cannot convert tensor with base ${tt.base} to vector")
    assert(tt.dimensions.isDefined, "cannot convert tensor with undefined dimensions")

    if(tt.dimensions.isEmpty) {
      mleapBasicTypeToSparkType(tt.base)
    } else if(tt.dimensions.size == 1) {
      new VectorUDT
    } else if(tt.dimensions.size == 2) {
      new MatrixUDT
    } else {
      throw new IllegalArgumentException("cannot convert tensor for non-scalar, vector or matrix tensor")
    }
  }

  def sparkToMleapDataShape(field: StructField,
                            dataset: DataFrame): types.DataShape = field.dataType match {
    case BooleanType | ByteType | ShortType
         | IntegerType | LongType | FloatType
         | DoubleType | StringType | ArrayType(ByteType, false) => types.ScalarShape()
    case ArrayType(_, false) => types.ListShape()
    case vu: VectorUDT =>
      val size = dataset.select(field.name).head.getAs[Vector](0).size
      types.TensorShape(Some(Seq(size)))
    case mu: MatrixUDT =>
      val m = dataset.select(field.name).head.getAs[Matrix](0)
      types.TensorShape(Some(Seq(m.numRows, m.numCols)))
    case _ => throw new IllegalArgumentException("invalid struct field for shape")
  }
}

object TypeConverters extends TypeConverters {
  val VECTOR_BASIC_TYPES: Set[BasicType] = Set(
    BasicType.Boolean,
    BasicType.Byte,
    BasicType.Short,
    BasicType.Int,
    BasicType.Long,
    BasicType.Float,
    BasicType.Double,
    BasicType.String
  )
}
