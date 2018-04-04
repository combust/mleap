package ml.combust.mleap.grpc.server

import java.io.File
import java.net.URI

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb.MleapGrpc
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.FrameReader

import scala.concurrent.ExecutionContext

object TestUtil {

  implicit val ec = ExecutionContext.global

  lazy val lrUri: String = {
    getClass.getClassLoader.getResource("models/airbnb.model.lr.zip").toURI.toString
  }

  lazy val frame: DefaultLeapFrame = {
    FrameReader().read(new File(getClass.getClassLoader.getResource("leap_frame/frame.airbnb.json").getFile)).get
  }

  def server(system: ActorSystem) : Server = {
    val ssd = MleapGrpc.bindService(new GrpcServer(MleapExecutor(system))(ec, ActorMaterializer()(system)), ec)
    val server = InProcessServerBuilder.forName(uniqueServerName).directExecutor().addService(ssd).build
    server.start()
    server
  }

  def uniqueServerName : String = {
    "in-process server for " + getClass
  }

  def inProcessChannel : ManagedChannel = {
    InProcessChannelBuilder.forName(uniqueServerName).directExecutor.build
  }
}
