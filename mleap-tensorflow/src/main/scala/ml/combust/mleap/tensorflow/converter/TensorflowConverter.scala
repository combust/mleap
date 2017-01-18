package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.DenseTensor
import org.tensorflow

import scala.collection.mutable

/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
  def convert(tensor: tensorflow.Tensor, dataType: DataType): Any = dataType match {
    case FloatType(isNullable) =>
      val v = tensor.floatValue()
      if(isNullable) Some(v) else v
    case DoubleType(isNullable) =>
      val v = tensor.doubleValue()
      if(isNullable) Some(v) else v
    case IntegerType(isNullable) =>
      val v = tensor.intValue()
      if(isNullable) Some(v) else v
    case LongType(isNullable) =>
      val v = tensor.longValue()
      if(isNullable) Some(v) else v
    case BooleanType(isNullable) =>
      val v = tensor.booleanValue()
      if(isNullable) Some(v) else v
    case StringType(isNullable) =>
      val v = new String(tensor.bytesValue(), "UTF-8")
      if(isNullable) Some(v) else v
    case tt: TensorType =>
      val shape = tensor.shape().map(_.toInt).toList
      val nested = createArray(shape)(arrF(tt.base))
      tensor.copyTo(nested)

      tt.base match {
        case FloatType(false) =>
          DenseTensor(flatten[Float](nested, shape)(a => a.asInstanceOf[Array[Float]]).toArray, shape)
        case DoubleType(false) =>
          DenseTensor(flatten[Double](nested, shape)(a => a.asInstanceOf[Array[Double]]).toArray, shape)
        case IntegerType(false) =>
          DenseTensor(flatten[Int](nested, shape)(a => a.asInstanceOf[Array[Int]]).toArray, shape)
        case LongType(false) =>
          DenseTensor(flatten[Long](nested, shape)(a => a.asInstanceOf[Array[Long]]).toArray, shape)
        case BooleanType(false) =>
          DenseTensor(flatten[Boolean](nested, shape)(a => a.asInstanceOf[Array[Boolean]]).toArray, shape)
        case _ =>
          throw new RuntimeException(s"unsupported tensorflow type: $dataType")
      }
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }

  private def arrF(base: BasicType): (Int) => AnyRef = base match {
    case FloatType(false) =>
      (i) => new Array[Float](i)
    case DoubleType(false) =>
      (i) => new Array[Double](i)
    case IntegerType(false) =>
      (i) => new Array[Integer](i)
    case LongType(false) =>
      (i) => new Array[Long](i)
    case BooleanType(false) =>
      (i) => new Array[Boolean](i)
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $base")
  }

  private def createArray(shape: Seq[Int])
                         (f: (Int) => AnyRef): AnyRef = shape match {
    case head :: Nil => f(head)
    case head :: tail => Array.tabulate[AnyRef](head)(_ => createArray(tail)(f))
  }

  private def flatten[T](arr: AnyRef, shape: Seq[Int])
                        (f: (AnyRef) => mutable.IndexedSeq[T]): mutable.IndexedSeq[T] = shape match {
    case head :: Nil => f(arr)
    case head :: tail =>
      val arrArr = mutable.WrappedArray.make[AnyRef](arr.asInstanceOf[Array[AnyRef]])
      arrArr.flatMap {
        a => flatten[T](a, tail)(f)
      }
  }

}
