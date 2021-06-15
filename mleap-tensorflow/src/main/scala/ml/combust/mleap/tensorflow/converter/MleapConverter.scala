package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.core.types.TensorType
import ml.combust.mleap.tensor.DenseTensor

import java.nio.ByteBuffer
import java.nio.DoubleBuffer
import java.nio.FloatBuffer
import java.nio.IntBuffer
import java.nio.LongBuffer

import org.tensorflow.ndarray.{NdArray, NdArraySequence, NdArrays, Shape}
import org.tensorflow.types.TFloat32
import org.tensorflow.types.TFloat64
import org.tensorflow.types.TInt32
import org.tensorflow.types.TInt64
import org.tensorflow.types.TString
import org.tensorflow.types.TUint8
import org.tensorflow.ndarray.buffer.DataBuffers
import org.tensorflow

import scala.reflect.ClassTag
import ml.combust.mleap.tensor.{ByteString, Tensor}

import java.util.function.BiConsumer

/**
  * Created by hollinwilkins on 1/12/17.
  */

object MleapConverter {
  def convert[T: ClassTag](value: Tensor[T]): tensorflow.Tensor = {
    val dense = value.toDense
    val shape = Shape.of(dense.dimensions.map(_.toLong).toArray: _*)
    value.base.runtimeClass match {
      case Tensor.ByteClass =>
        TUint8.tensorOf(
          shape,
          DataBuffers.of(
              dense.values.asInstanceOf[Array[Byte]], true,false
          )
        )
      case Tensor.IntClass =>
        TInt32.tensorOf(
          shape,
          DataBuffers.of(
              dense.values.asInstanceOf[Array[Int]], true, false
          )
        )

      case Tensor.LongClass =>
        TInt64.tensorOf(
          shape,
          DataBuffers.of(
              dense.values.asInstanceOf[Array[Long]], true, false
          )
        )
      case Tensor.FloatClass =>
        TFloat32.tensorOf(
          shape,
          DataBuffers.of(
              dense.values.asInstanceOf[Array[Float]], true, false
          )
        )
      case Tensor.DoubleClass =>
        TFloat64.tensorOf(
          shape,
          DataBuffers.of(
              dense.values.asInstanceOf[Array[Double]], true, false
          )
        )
      case Tensor.StringClass =>
        val ndString = NdArrays.ofObjects(classOf[String],shape)
        val mlTensor = dense.asInstanceOf[DenseTensor[String]]

        ndString.scalars.asInstanceOf[NdArraySequence[NdArray[String]]].forEachIndexed(
          new BiConsumer[Array[Long], NdArray[String]]
          {
            override def accept(i: Array[Long], e: NdArray[String]): Unit = {
              e.setObject(mlTensor(i.map(_.toInt):_*))
          }
        })
        TString.tensorOf(ndString)

      case Tensor.ByteStringClass =>
        val ndString = NdArrays.ofObjects(classOf[Array[Byte]], shape)
        val mlTensor = dense.asInstanceOf[DenseTensor[ByteString]]

        ndString.scalars.asInstanceOf[NdArraySequence[NdArray[Array[Byte]]]].forEachIndexed(
          new BiConsumer[Array[Long], NdArray[Array[Byte]]]
          {
            override def accept(i: Array[Long], e: NdArray[Array[Byte]]): Unit = {
              e.setObject(mlTensor(i.map(_.toInt):_*).bytes)
          }
        })
        TString.tensorOfBytes(ndString)
      case _ =>
        throw new IllegalArgumentException(s"unsupported tensor type ${value.getClass}[${value.base.runtimeClass}]")
    }
  }
}
