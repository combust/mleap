package ml.combust.mleap.tensorflow.converter

import java.nio._

import ml.combust.mleap.core.types.{BasicType, TensorType}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import org.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
  def convert(tensor: tensorflow.Tensor[_], tt: TensorType): DenseTensor[_] = {
    val size = tensor.shape().product.toInt
    val dimensions = tt.dimensions.get

    tt.base match {
      case BasicType.Byte =>
        val b = ByteBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Int =>
        val b = IntBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Long =>
        val b = LongBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Float =>
        val b = FloatBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Double =>
        val b = DoubleBuffer.allocate(Math.max(1, size))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case _ =>
        throw new RuntimeException(s"unsupported tensorflow type: $tt")
    }
  }
}
