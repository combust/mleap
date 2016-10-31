package ml.combust.mleap.runtime.serialization

import java.io.{DataInputStream, File, FileInputStream, FileOutputStream}

import ml.combust.mleap.runtime.types.StructType
import resource._
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame}

/**
  * Created by hollinwilkins on 8/23/16.
  */
trait FrameSerializer {
  val serializerContext: FrameSerializerContext

  def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte]
  def fromBytes(bytes: Array[Byte]): DefaultLeapFrame

  def write[LF <: LeapFrame[LF]](frame: LF, file: File): Unit = {
    val bytes = toBytes(frame)
    for(out <- managed(new FileOutputStream(file))) {
      out.write(bytes)
    }
  }

  def read(file: File): DefaultLeapFrame = {
    (for(in <- managed(new DataInputStream(new FileInputStream(file)))) yield {
      val bytes = new Array[Byte](file.length().toInt)
      in.readFully(bytes)
      fromBytes(bytes)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bm) => bm
    }
  }

  def rowSerializer(schema: StructType): RowSerializer
}