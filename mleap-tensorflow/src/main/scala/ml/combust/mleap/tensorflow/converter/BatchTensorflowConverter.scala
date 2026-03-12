package ml.combust.mleap.tensorflow.converter

import java.nio._

import ml.combust.mleap.core.types.{BasicType, TensorType}
import ml.combust.mleap.tensor.{DenseTensor}
import org.tensorflow

object BatchTensorflowConverter {

  def convert(tensor: tensorflow.Tensor[_], tt: TensorType): Seq[DenseTensor[_]] = {
    val size = tensor.shape().product.toInt
    val dimensions: Seq[Int] = tt.dimensions.get
    tt.base match {
      case BasicType.Byte =>
        val b = ByteBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        b.array().map(x=> DenseTensor(Array(x),dimensions))
      case BasicType.Int =>
        val b = IntBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        b.array().map(x=> DenseTensor(Array(x),dimensions))
      case BasicType.Long =>
        val b = LongBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        b.array().map(x=> DenseTensor(Array(x),dimensions))
      case BasicType.Float =>
        val b = FloatBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        b.array().map(x=> DenseTensor(Array(x),dimensions))
      case BasicType.Double =>
        val b = DoubleBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        b.array().map(x=> DenseTensor(Array(x),dimensions))
      case _ =>
        throw new RuntimeException(s"unsupported tensorflow type: $tt")
    }
  }
}