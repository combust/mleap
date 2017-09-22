package ml.combust.mleap.tensorflow.converter

import java.nio.{DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer}

import ml.combust.mleap.core.types.{BasicType, TensorType}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import org.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
  def convert(tensor: tensorflow.Tensor, tt: TensorType): DenseTensor[_] = {
    val dimensions = tensor.shape().map(_.toInt)

    tt.base match {
      case BasicType.Int =>
        val b = IntBuffer.allocate(Math.max(1, dimensions.product))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Long =>
        val b = LongBuffer.allocate(Math.max(1, dimensions.product))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Float =>
        val b = FloatBuffer.allocate(Math.max(1, dimensions.product))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case BasicType.Double =>
        val b = DoubleBuffer.allocate(Math.max(1, dimensions.product))
        tensor.writeTo(b)
        DenseTensor(b.array(), dimensions)
      case _ =>
        throw new RuntimeException(s"unsupported tensorflow type: $tt")
    }
  }
}
