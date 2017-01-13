package ml.combust.mleap.tensorflow.converter

import java.nio._

import ml.combust.mleap.core.DenseTensor
import ml.combust.mleap.runtime.types._
import org.bytedeco.javacpp.tensorflow
import org.bytedeco.javacpp.tensorflow.TensorShape

/**
  * Created by hollinwilkins on 1/12/17.
  */
object MleapConverter {
  def convert(value: Any, dataType: DataType): tensorflow.Tensor = dataType match {
    case FloatType(isNullable) =>
      val t = new tensorflow.Tensor(tensorflow.DT_FLOAT, new TensorShape(1))
      val b: FloatBuffer = t.createBuffer()
      val v = if(isNullable) value.asInstanceOf[Option[Float]].get else value.asInstanceOf[Float]
      b.put(v)
      t
    case DoubleType(isNullable) =>
      val t = new tensorflow.Tensor(tensorflow.DT_DOUBLE, new TensorShape(1))
      val b: DoubleBuffer = t.createBuffer()
      val v = if(isNullable) value.asInstanceOf[Option[Double]].get else value.asInstanceOf[Double]
      b.put(v)
      t
    case IntegerType(isNullable) =>
      val t = new tensorflow.Tensor(tensorflow.DT_INT32, new TensorShape(1))
      val b: IntBuffer = t.createBuffer()
      val v = if(isNullable) value.asInstanceOf[Option[Int]].get else value.asInstanceOf[Int]
      b.put(v)
      t
    case LongType(isNullable) =>
      val t = new tensorflow.Tensor(tensorflow.DT_INT64, new TensorShape(1))
      val b: LongBuffer = t.createBuffer()
      val v = if(isNullable) value.asInstanceOf[Option[Long]].get else value.asInstanceOf[Long]
      b.put(v)
      t
    case BooleanType(isNullable) =>
      throw new RuntimeException("boolean conversion to tensor not yet supported")
    case StringType(isNullable) =>
      throw new RuntimeException("string conversion to tensor not yet supported")
    case tt: TensorType =>
      tt.base match {
        case FloatType(isNullable) =>
          val t = new tensorflow.Tensor(tensorflow.DT_FLOAT, new TensorShape(1))
          val b: FloatBuffer = t.createBuffer()
          val v = if(isNullable) value.asInstanceOf[Option[DenseTensor[Float]]].get else value.asInstanceOf[DenseTensor[Float]]
          b.put(v.values)
          t
        case DoubleType(isNullable) =>
          val t = new tensorflow.Tensor(tensorflow.DT_DOUBLE, new TensorShape(1))
          val b: DoubleBuffer = t.createBuffer()
          val v = if(isNullable) value.asInstanceOf[Option[DenseTensor[Double]]].get else value.asInstanceOf[DenseTensor[Double]]
          b.put(v.values)
          t
        case IntegerType(isNullable) =>
          val t = new tensorflow.Tensor(tensorflow.DT_INT32, new TensorShape(1))
          val b: IntBuffer = t.createBuffer()
          val v = if(isNullable) value.asInstanceOf[Option[DenseTensor[Int]]].get else value.asInstanceOf[DenseTensor[Int]]
          b.put(v.values)
          t
        case LongType(isNullable) =>
          val t = new tensorflow.Tensor(tensorflow.DT_INT64, new TensorShape(1))
          val b: LongBuffer = t.createBuffer()
          val v = if(isNullable) value.asInstanceOf[Option[DenseTensor[Long]]].get else value.asInstanceOf[DenseTensor[Long]]
          b.put(v.values)
          t
        case BooleanType(isNullable) =>
          throw new RuntimeException("boolean conversion to tensor not yet supported")
        case StringType(isNullable) =>
          throw new RuntimeException("string conversion to tensor not yet supported")
      }
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }
}
