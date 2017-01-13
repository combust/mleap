package ml.combust.mleap.tensorflow.converter

import java.nio._

import ml.combust.mleap.core.DenseTensor
import ml.combust.mleap.runtime.types._
import org.bytedeco.javacpp.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
  def convert(tensor: tensorflow.Tensor, dataType: DataType): Any = dataType match {
    case FloatType(isNullable) =>
      val b: FloatBuffer = tensor.createBuffer()
      val v = b.get(0)
      if(isNullable) Some(v) else v
    case DoubleType(isNullable) =>
      val b: DoubleBuffer = tensor.createBuffer()
      val v = b.get(0)
      if(isNullable) Some(v) else v
    case IntegerType(isNullable) =>
      val b: IntBuffer = tensor.createBuffer()
      val v = b.get(0)
      if(isNullable) Some(v) else v
    case LongType(isNullable) =>
      val b: LongBuffer = tensor.createBuffer()
      val v = b.get(0)
      if(isNullable) Some(v) else v
    case BooleanType(isNullable) =>
      throw new RuntimeException("boolean conversion to tensor not yet supported")
    case StringType(isNullable) =>
      throw new RuntimeException("string conversion to tensor not yet supported")
    case tt: TensorType =>
      tt.base match {
        case FloatType(isNullable) =>
          val b: FloatBuffer = tensor.createBuffer()
          val v = DenseTensor(b.array(), tt.dimensions)
          if(isNullable) Some(v) else v
        case DoubleType(isNullable) =>
          val b: DoubleBuffer = tensor.createBuffer()
          val v = DenseTensor(b.array(), tt.dimensions)
          if(isNullable) Some(v) else v
        case IntegerType(isNullable) =>
          val b: IntBuffer = tensor.createBuffer()
          val v = DenseTensor(b.array(), tt.dimensions)
          if(isNullable) Some(v) else v
        case LongType(isNullable) =>
          val b: LongBuffer = tensor.createBuffer()
          val v = DenseTensor(b.array(), tt.dimensions)
          if(isNullable) Some(v) else v
        case BooleanType(isNullable) =>
          throw new RuntimeException("boolean conversion to tensor not yet supported")
        case StringType(isNullable) =>
          throw new RuntimeException("string conversion to tensor not yet supported")
      }
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }
}
