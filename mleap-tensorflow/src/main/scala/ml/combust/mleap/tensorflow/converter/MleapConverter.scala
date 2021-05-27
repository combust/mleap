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
  def convert[T: ClassTag](value: Tensor[T], tt: TensorType): tensorflow.Tensor = {
    val dense = value.toDense
    val dimensions = dense.dimensions.map(_.toLong).toArray

    value.base.runtimeClass match {
      case Tensor.ByteClass =>
        TUint8.tensorOf(
          Shape.of(dimensions: _*),
          DataBuffers.of(
            ByteBuffer.wrap(
              dense.values.asInstanceOf[Array[Byte]]
            )
          )
        )
      case Tensor.IntClass =>
        TInt32.tensorOf(
          Shape.of(dimensions: _*),
          DataBuffers.of(
            IntBuffer.wrap(
              dense.values.asInstanceOf[Array[Int]]
            )
          )
        )

      case Tensor.LongClass =>
        TInt64.tensorOf(
          Shape.of(dimensions: _*),
          DataBuffers.of(
            LongBuffer.wrap(
              dense.values.asInstanceOf[Array[Long]]
            )
          )
        )
      case Tensor.FloatClass =>
        TFloat32.tensorOf(
          Shape.of(dimensions: _*),
          DataBuffers.of(
            FloatBuffer.wrap(
              dense.values.asInstanceOf[Array[Float]]
            )
          )
        )
      case Tensor.DoubleClass =>
        TFloat64.tensorOf(
          Shape.of(dimensions: _*),
          DataBuffers.of(
            DoubleBuffer.wrap(
              dense.values.asInstanceOf[Array[Double]]
            )
          )
        )
      case Tensor.StringClass =>
        val stringMatrix = NdArrays.ofObjects(classOf[String], Shape.of(dimensions: _*))
        val bs_value = dense.asInstanceOf[DenseTensor[String]]

        stringMatrix.scalars.asInstanceOf[NdArraySequence[NdArray[String]]].forEachIndexed(
          new BiConsumer[Array[Long], NdArray[String]]
          {
            override def accept(i: Array[Long], e: NdArray[String]): Unit = {
            stringMatrix.setObject(bs_value.get(i.map(_.toInt):_*).get, i:_*)
          }
        })
        TString.tensorOf(stringMatrix)

      case Tensor.ByteStringClass =>
        val stringMatrix = NdArrays.ofObjects(classOf[Array[Byte]], Shape.of(dimensions: _*))
        val bs_value = dense.asInstanceOf[DenseTensor[ByteString]]

        stringMatrix.scalars.asInstanceOf[NdArraySequence[NdArray[Array[Byte]]]].forEachIndexed(
          new BiConsumer[Array[Long], NdArray[Array[Byte]]]
          {
            override def accept(i: Array[Long], e: NdArray[Array[Byte]]): Unit = {
            stringMatrix.setObject(bs_value.get(i.map(_.toInt):_*).map(s => s.bytes).get, i:_*)
          }
        })
        TString.tensorOfBytes(stringMatrix)
      case _ =>
        throw new IllegalArgumentException(s"unsupported tensor type $tt")
    }
  }
}
