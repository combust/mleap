package org.apache.spark.sql.mleap

import ml.combust.mleap.core.types
import ml.combust.mleap.core.types.{BasicType, Casting}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import org.apache.spark.ml.linalg.{Matrix, MatrixUDT, Vector, VectorUDT}
import ml.combust.mleap.core.util.VectorConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.Try

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  private def getVectorSize(dataset: DataFrame, field: StructField): Int = {
    val sizeInMeta = Try(field.metadata.getMetadata("ml_attr").getLong("num_attrs").toInt)
    if (sizeInMeta.isSuccess) {
      sizeInMeta.get
    } else {
      dataset.select(field.name).head().getAs[Vector](0).size
    }
  }

  def sparkToMleapValue(dataType: DataType): (Any) => Any = dataType match {
    case _: DecimalType =>
      (v: Any) => v.asInstanceOf[java.math.BigDecimal].doubleValue()
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
    case _ => (v) => v
  }

  def sparkToMleapConverter(dataset: DataFrame,
                            field: StructField): (types.StructField, (Any) => Any) = {
    (sparkFieldToMleapField(dataset, field), sparkToMleapValue(field.dataType))
  }

  def sparkFieldToMleapField(dataset: DataFrame,
                             field: StructField): types.StructField = {
    val dt = field.dataType match {
      case BooleanType => types.ScalarType.Boolean
      case ByteType => types.ScalarType.Byte
      case ShortType => types.ScalarType.Short
      case IntegerType => types.ScalarType.Int
      case LongType => types.ScalarType.Long
      case FloatType => types.ScalarType.Float
      case DoubleType => types.ScalarType.Double
      case _: DecimalType => types.ScalarType.Double
      case StringType => types.ScalarType.String.setNullable(field.nullable)
      case ArrayType(ByteType, _) => types.ListType.Byte
      case ArrayType(BooleanType, _) => types.ListType.Boolean
      case ArrayType(ShortType, _) => types.ListType.Short
      case ArrayType(IntegerType, _) => types.ListType.Int
      case ArrayType(LongType, _) => types.ListType.Long
      case ArrayType(FloatType, _) => types.ListType.Float
      case ArrayType(DoubleType, _) => types.ListType.Double
      case ArrayType(StringType, _) => types.ListType.String
      case ArrayType(ArrayType(ByteType, _), _) => types.ListType.ByteString
      case MapType(keyType, valueType, _) => types.MapType(sparkTypeToMleapBasicType(keyType), sparkTypeToMleapBasicType(valueType))
      case _: VectorUDT =>
        val size = getVectorSize(dataset, field)
        types.TensorType.Double(size)
      case _: MatrixUDT =>
        val m = dataset.select(field.name).head.getAs[Matrix](0)
        types.TensorType.Double(m.numRows, m.numCols)
      case ArrayType(elementType, _) if elementType == new VectorUDT =>
        val a = dataset.select(field.name).head.getAs[mutable.WrappedArray[Vector]](0)
        types.TensorType.Double(a.length, a.head.size)
      case _ => throw new UnsupportedOperationException(s"Cannot convert spark field $field to mleap")
    }

    types.StructField(field.name, dt.setNullable(field.nullable))
  }

  def sparkSchemaToMleapSchema(dataset: DataFrame): types.StructType = {
    val fields = dataset.schema.fields.map(f => sparkFieldToMleapField(dataset, f))
    types.StructType(fields).get
  }

  def sparkTypeToMleapBasicType(sparkType: DataType): types.BasicType = {
    sparkType match{
      case BooleanType => types.BasicType.Boolean
      case ByteType => types.BasicType.Byte
      case ShortType => types.BasicType.Short
      case IntegerType => types.BasicType.Int
      case LongType => types.BasicType.Long
      case FloatType => types.BasicType.Float
      case DoubleType => types.BasicType.Double
      case _: DecimalType => types.BasicType.Double
      case StringType => types.BasicType.String
      case _ => throw new IllegalArgumentException(s"Can not convert spark $sparkType to mleap BasicType")
    }
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
    case _ => throw new UnsupportedOperationException(s"Cannot cast mleap type $base to spark DataType")
  }

  def mleapToSparkValue(dataType: types.DataType): (Any) => Any = dataType match {
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

  def mleapToSparkConverter(field: types.StructField): (StructField, (Any) => Any) = {
    val dt = field.dataType match {
      case types.ScalarType(base, _) => mleapBasicTypeToSparkType(base)
      case types.ListType(base, _) => ArrayType(mleapBasicTypeToSparkType(base), containsNull = false)
      case types.MapType(key, value, _) => MapType(mleapBasicTypeToSparkType(key), mleapBasicTypeToSparkType(value))
      case tt: types.TensorType => mleapTensorToSpark(tt)
    }

    (StructField(field.name, dt, nullable = field.dataType.isNullable), mleapToSparkValue(field.dataType))
  }

  def mleapFieldToSparkField(field: types.StructField): StructField = {
    val dt = field.dataType match {
      case types.ScalarType(base, _) => mleapBasicTypeToSparkType(base)
      case types.ListType(base, _) => ArrayType(mleapBasicTypeToSparkType(base), containsNull = false)
      case types.MapType(key, value, _) => MapType(mleapBasicTypeToSparkType(key), mleapBasicTypeToSparkType(value))
      case tt: types.TensorType => mleapTensorToSpark(tt)
    }

    StructField(field.name, dt, nullable = field.dataType.isNullable)
  }

  def mleapSchemaToSparkSchema(schema: types.StructType): StructType = {
    val fields = schema.fields.map(mleapFieldToSparkField)
    StructType(fields)
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
         | DoubleType | StringType => types.ScalarShape(field.nullable)
    case _: DecimalType => types.ScalarShape(field.nullable)
    case ArrayType(_, false) => types.ListShape(field.nullable)
    case MapType(_, _, _) => types.ListShape(field.nullable)
    case vu: VectorUDT =>
      val size = getVectorSize(dataset, field)
      types.TensorShape(Some(Seq(size)), field.nullable)
    case mu: MatrixUDT =>
      val m = dataset.select(field.name).head.getAs[Matrix](0)
      types.TensorShape(Some(Seq(m.numRows, m.numCols)), field.nullable)
    case _ => throw new IllegalArgumentException(s"invalid struct field for shape: $field")
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
