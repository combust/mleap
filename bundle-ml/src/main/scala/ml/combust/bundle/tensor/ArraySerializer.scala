package ml.combust.bundle.tensor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import ml.combust.mleap.tensor.ByteString

import scala.collection.mutable
import scala.util.{Failure, Success, Try, Using}

/**
  * Created by hollinwilkins on 1/15/17.
  */
trait ArraySerializer[T] {
  def write(arr: Array[T]): Array[Byte]
  def read(arr: Array[Byte]): Array[T]
}

object BooleanArraySerializer extends ArraySerializer[Boolean] {
  override def write(arr: Array[Boolean]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length)
    arr.foreach {
      case true => b.put(1: Byte)
      case false => b.put(0: Byte)
    }

    b.array()
  }

  override def read(arr: Array[Byte]): Array[Boolean] = {
    val b = ByteBuffer.wrap(arr)
    val bArr = mutable.ArrayBuilder.make[Boolean]
    while(b.hasRemaining) {
      bArr += (if(b.get == 1) true else false)
    }
    bArr.result()
  }
}

object ByteArraySerializer extends ArraySerializer[Byte] {
  override def write(arr: Array[Byte]): Array[Byte] = arr
  override def read(arr: Array[Byte]): Array[Byte] = arr
}

object ShortArraySerializer extends ArraySerializer[Short] {
  override def write(arr: Array[Short]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length * 2)
    arr.foreach(s => b.putShort(s))
    b.array()
  }

  override def read(arr: Array[Byte]): Array[Short] = {
    val b = ByteBuffer.wrap(arr)
    val a = mutable.ArrayBuilder.make[Short]

    while(b.hasRemaining) {
      a += b.getShort()
    }

    a.result()
  }
}

object IntArraySerializer extends ArraySerializer[Int] {
  override def write(arr: Array[Int]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length * 4)
    arr.foreach(s => b.putInt(s))
    b.array()
  }

  override def read(arr: Array[Byte]): Array[Int] = {
    val b = ByteBuffer.wrap(arr)
    val a = mutable.ArrayBuilder.make[Int]

    while(b.hasRemaining) {
      a += b.getInt()
    }

    a.result()
  }
}

object LongArraySerializer extends ArraySerializer[Long] {
  override def write(arr: Array[Long]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length * 8)
    arr.foreach(s => b.putLong(s))
    b.array()
  }

  override def read(arr: Array[Byte]): Array[Long] = {
    val b = ByteBuffer.wrap(arr)
    val a = mutable.ArrayBuilder.make[Long]

    while(b.hasRemaining) {
      a += b.getLong()
    }

    a.result()
  }
}

object FloatArraySerializer extends ArraySerializer[Float] {
  override def write(arr: Array[Float]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length * 4)
    arr.foreach(s => b.putFloat(s))
    b.array()
  }

  override def read(arr: Array[Byte]): Array[Float] = {
    val b = ByteBuffer.wrap(arr)
    val a = mutable.ArrayBuilder.make[Float]

    while(b.hasRemaining) {
      a += b.getFloat()
    }

    a.result()
  }
}

object DoubleArraySerializer extends ArraySerializer[Double] {
  override def write(arr: Array[Double]): Array[Byte] = {
    val b = ByteBuffer.allocate(arr.length * 8)
    arr.foreach(s => b.putDouble(s))
    b.array()
  }

  override def read(arr: Array[Byte]): Array[Double] = {
    val b = ByteBuffer.wrap(arr)
    val a = mutable.ArrayBuilder.make[Double]

    while(b.hasRemaining) {
      a += b.getDouble()
    }

    a.result()
  }
}

object StringArraySerializer extends ArraySerializer[String] {
  override def write(arr: Array[String]): Array[Byte] = {
    Using(new ByteArrayOutputStream()) { out =>
      val dout = new DataOutputStream(out)
      arr.foreach {
        str =>
          dout.writeInt(str.length)
          dout.writeBytes(str)
      }

      dout.flush()
      out.toByteArray
    }.toOption.get
  }

  override def read(arr: Array[Byte]): Array[String] = {
    Using(new ByteArrayInputStream(arr)) { in =>
      val din = new DataInputStream(in)
      val arr = mutable.ArrayBuilder.make[String]

      var done = false
      while(!done) {
        Try {
          val size = din.readInt()
          val bytes = new Array[Byte](size)
          din.readFully(bytes)
          new String(bytes)
        } match {
          case Success(str) => arr += str
          case Failure(_) => done = true
        }
      }

      arr.result()
    }.toOption.get
  }
}

object ByteStringArraySerializer extends ArraySerializer[ByteString] {
  override def write(arr: Array[ByteString]): Array[Byte] = {
    Using(new ByteArrayOutputStream()) { out =>
      val dout = new DataOutputStream(out)
      arr.foreach {
        bs =>
          dout.writeInt(bs.bytes.length)
          dout.write(bs.bytes)
      }

      dout.flush()
      out.toByteArray
    }.toOption.get
  }

  override def read(arr: Array[Byte]): Array[ByteString] = {
    Using(new ByteArrayInputStream(arr)) { in =>
      val din = new DataInputStream(in)
      val arr = mutable.ArrayBuilder.make[ByteString]

      var done = false
      while(!done) {
        Try {
          val size = din.readInt()
          val bytes = new Array[Byte](size)
          din.readFully(bytes)
          ByteString(bytes)
        } match {
          case Success(bs) => arr += bs
          case Failure(_) => done = true
        }
      }

      arr.result()
    }.toOption.get
  }
}
