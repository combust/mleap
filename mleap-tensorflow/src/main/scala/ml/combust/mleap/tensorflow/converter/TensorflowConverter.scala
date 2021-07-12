package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.core.types.{BasicType, TensorType}

import java.nio._
import ml.combust.mleap.tensor.{ByteString, DenseTensor}
import org.tensorflow
import org.tensorflow.ndarray.{NdArray, NdArraySequence}
import org.tensorflow.ndarray.buffer.DataBuffers
import org.tensorflow.types._

import scala.collection.mutable.ArrayBuffer
import java.util.function.BiConsumer


/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
    def convert(tensor: tensorflow.Tensor, tensorType: TensorType): DenseTensor[_] = {
      val dimensions = getShape(tensor)
      val size = getSize(tensor)

      tensor match {
        case u8: TUint8 =>
         val buffer = ByteBuffer.allocate(size)
          u8.read(DataBuffers.of(buffer))
          DenseTensor(buffer.array, dimensions)
        case i32: TInt32 =>
         val buffer = IntBuffer.allocate(size)
          i32.read(DataBuffers.of(buffer))
          DenseTensor(buffer.array, dimensions)
        case i64: TInt64=>
         val buffer = LongBuffer.allocate(size)
          i64.read(DataBuffers.of(buffer))
          DenseTensor(buffer.array, dimensions)
        case f32: TFloat32 =>
         val buffer = FloatBuffer.allocate(size)
          f32.read(DataBuffers.of(buffer))
          DenseTensor(buffer.array, dimensions)
        case f64: TFloat64 =>
         val buffer = DoubleBuffer.allocate(size)
          f64.read(DataBuffers.of(buffer))
          DenseTensor(buffer.array, dimensions)
        case str: TString =>
          tensorType.base match {
            case BasicType.String =>
              val array = ArrayBuffer[String]()
              if (dimensions.isEmpty) {
                array += str.getObject()
              } else {
                str.scalars.asInstanceOf[NdArraySequence[NdArray[String]]].forEachIndexed(new BiConsumer[Array[Long], NdArray[String]] {
                  override def accept(i: Array[Long], e: NdArray[String]): Unit = {
                    array += e.getObject()
                  }
                })
              }
              DenseTensor(array.toArray, dimensions)
            case BasicType.ByteString =>
              val array = ArrayBuffer[ByteString]()
              if (dimensions.isEmpty) {
                array += ByteString(str.asBytes().getObject())
              } else {
                str.asBytes().scalars.asInstanceOf[NdArraySequence[NdArray[Array[Byte]]]].forEachIndexed(new BiConsumer[Array[Long], NdArray[Array[Byte]]] {
                  override def accept(i: Array[Long], e: NdArray[Array[Byte]]): Unit = {
                    array += ByteString(e.getObject())
                  }
                })
              }
              DenseTensor(array.toArray, dimensions)
            case _ =>
              throw new RuntimeException(s"unsupported ml TensorType ${tensorType} when Tensorflow tensor is String")
          }
        case _ =>
          throw new RuntimeException(s"unsupported tensorflow type: ${tensor.dataType()}")
      }
    }

  def getSize(tensor: tensorflow.Tensor): Int = {
    tensor.shape.size.asInstanceOf[Int]
  }

  def getShape(tensor: tensorflow.Tensor): Seq[Int] = {
    tensor.shape.asArray.toSeq.map(_.toInt)
  }
}
