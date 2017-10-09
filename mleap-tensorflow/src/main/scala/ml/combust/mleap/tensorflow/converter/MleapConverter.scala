package ml.combust.mleap.tensorflow.converter

import java.nio.{DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer}

import ml.combust.mleap.core.types.{BasicType, TensorType}
import ml.combust.mleap.tensor.Tensor
import org.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object MleapConverter {
  def convert(value: Tensor[_], tt: TensorType): tensorflow.Tensor = {
    val dense = value.toDense
    val dimensions = dense.dimensions.map(_.toLong).toArray

    tt.base match {
      case BasicType.Int =>
        tensorflow.Tensor.create(dimensions,
          IntBuffer.wrap(dense.values.asInstanceOf[Array[Int]]))
      case BasicType.Long =>
        tensorflow.Tensor.create(dimensions,
          LongBuffer.wrap(dense.values.asInstanceOf[Array[Long]]))
      case BasicType.Float =>
        tensorflow.Tensor.create(dimensions,
          FloatBuffer.wrap(dense.values.asInstanceOf[Array[Float]]))
      case BasicType.Double =>
        tensorflow.Tensor.create(dimensions,
          DoubleBuffer.wrap(dense.values.asInstanceOf[Array[Double]]))
      case _ =>
        throw new IllegalArgumentException(s"unsupported tensor type $tt")
    }
  }
}
