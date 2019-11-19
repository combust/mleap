package ml.combust.mleap.tensorflow.converter

import java.nio._

import ml.combust.mleap.core.types.{BasicType, TensorType}
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.tensorflow

object BatchMleapConverter {
  def convert(value: Seq[Tensor[_]], tt: TensorType): tensorflow.Tensor[_] = {

    val dimensions: Array[Long] = (value.size +: value.head.dimensions).map(_.toLong).toArray

    tt.base match {
      case BasicType.ByteString =>
        val x: Array[Array[Byte]] = value
          .flatMap(_.asInstanceOf[Tensor[ByteString]].mapValues(_.bytes).toDense.values)
          .toArray
        tensorflow.Tensor.create(x)
      case BasicType.Byte =>
        val x: Array[Byte] = value
          .flatMap(_.asInstanceOf[Tensor[Byte]].mapValues(_.toByte).toDense.values)
          .toArray
        tensorflow.Tensor.create(x)
      case BasicType.Int =>
        val x: Array[Int] = value
          .flatMap(_.asInstanceOf[Tensor[Int]].mapValues(_.toInt).toDense.values)
          .toArray
        tensorflow.Tensor.create(dimensions,
          IntBuffer.wrap(x))
      case BasicType.Long =>
        val x: Array[Long] = value
          .flatMap(_.asInstanceOf[Tensor[Long]].mapValues(_.toLong).toDense.values)
          .toArray
        tensorflow.Tensor.create(dimensions,
          LongBuffer.wrap(x))
      case BasicType.Float =>
        val x: Array[Float] = value
          .flatMap(_.asInstanceOf[Tensor[Float]].mapValues(_.toFloat).toDense.values)
          .toArray
        tensorflow.Tensor.create(dimensions,
          FloatBuffer.wrap(x))
      case BasicType.Double =>
        val x: Array[Double] = value
          .flatMap(_.asInstanceOf[Tensor[Double]].mapValues(_.toDouble).toDense.values)
          .toArray
        tensorflow.Tensor.create(dimensions,
          DoubleBuffer.wrap(x))
      case _ =>
        throw new IllegalArgumentException(s"unsupported tensor type $tt")
    }
  }
}