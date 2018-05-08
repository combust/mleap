package ml.combust.mleap.grpc.server

import java.io.File
import java.net.URI

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import ml.combust.mleap.executor.{Client, MleapExecutor}
import ml.combust.mleap.grpc.GrpcClient
import ml.combust.mleap.pb.MleapGrpc
import ml.combust.mleap.pb.MleapGrpc.MleapStub
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.FrameReader

import scala.concurrent.ExecutionContext
import scala.util.Try

object TestUtil {

  implicit val ec = ExecutionContext.global

  lazy val lrUri: URI = URI.create(getClass.getClassLoader.getResource("models/airbnb.model.lr.zip").toURI.toString)

  lazy val frame: Try[DefaultLeapFrame] =
    FrameReader().read(new File(getClass.getClassLoader.getResource("leap_frame/frame.airbnb.json").getFile))

  lazy val uniqueServerName : String = "in-process server for " + getClass

  def createServer(system: ActorSystem) : Server = {
    val ssd = MleapGrpc.bindService(new GrpcServer(MleapExecutor(system))(ec, ActorMaterializer.create(system)), ec)
    val server = InProcessServerBuilder.forName(uniqueServerName).directExecutor().addService(ssd).build
    server.start()
    server
  }

  def createClient(channel: ManagedChannel): Client = new GrpcClient(new MleapStub(channel))

  def inProcessChannel : ManagedChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor.build

}
