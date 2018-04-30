package ml.combust.mleap.springboot

import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.springframework.http.{HttpEntity, HttpHeaders}

object TestUtil {

  lazy val demoUri = getClass.getClassLoader.getResource("demo.zip").toURI.toString

  lazy val builder = new LeapFrameBuilder

  lazy val httpEntityWithProtoHeaders = new HttpEntity[Unit](protoHeaders)

  lazy val httpEntityWithJsonHeaders = new HttpEntity[Unit](jsonHeaders)

  lazy val protoHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/x-protobuf")
    headers
  }

  lazy val jsonHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/json")
    headers
  }

  lazy val leapFrame = FrameWriter(DefaultLeapFrame(
                              StructType(Seq(StructField("demo:a", ScalarType.Double),
                                             StructField("demo:c", ScalarType.Double),
                                             StructField("demo:d", ScalarType.Double))).get,
                              Seq(Row(44.5, 22.1, 98.2))),
                       BuiltinFormats.binary).toBytes().get

  lazy val incompleteLeapFrame = FrameWriter(DefaultLeapFrame(
      StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:d", ScalarType.Double))).get,
    Seq(Row(44.5, 98.2))),
    BuiltinFormats.binary).toBytes().get
}
