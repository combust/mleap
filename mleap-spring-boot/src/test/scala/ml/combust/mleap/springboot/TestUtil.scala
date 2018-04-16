package ml.combust.mleap.springboot

import com.google.protobuf.ByteString
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.pb.{Mleap, TransformFrameRequest}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.springframework.http.HttpHeaders

object TestUtil {

  lazy val demoUri: String = getClass.getClassLoader.getResource("demo.zip").toURI.toString

  lazy val builder = new LeapFrameBuilder

  def protoHeaders: HttpHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/x-protobuf")

    headers
  }

  lazy val leapFrame: Array[Byte] = {
    val frame = DefaultLeapFrame(StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:c", ScalarType.Double), StructField("demo:d", ScalarType.Double))).get,
      Seq(Row(44.5, 22.1, 98.2)))
    FrameWriter(frame, BuiltinFormats.binary).toBytes().get
  }

  lazy val transformLeapFrameRequest: Mleap.TransformFrameRequest = {
    TransformFrameRequest.toJavaProto(TransformFrameRequest(uri = demoUri, format = BuiltinFormats.binary, timeout = 2000L,
      frame = ByteString.copyFrom(leapFrame)))
  }
}
