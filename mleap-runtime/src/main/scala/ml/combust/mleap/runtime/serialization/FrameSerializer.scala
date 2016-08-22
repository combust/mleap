package ml.combust.mleap.runtime.serialization

import java.io.{InputStream, OutputStream}

import ml.combust.mleap.runtime.DefaultLeapFrame
import ml.combust.mleap.runtime.types.StructType

/**
  * Created by hollinwilkins on 8/23/16.
  */
trait FrameSerializer {
  def write(out: OutputStream, frame: DefaultLeapFrame): Unit
  def read(in: InputStream): DefaultLeapFrame

  def withOptions(options: Map[String, String]): FrameSerializer = this
  def withOption(name: String, value: String): FrameSerializer = this
  def withSchema(schema: StructType): FrameSerializer = this
}
