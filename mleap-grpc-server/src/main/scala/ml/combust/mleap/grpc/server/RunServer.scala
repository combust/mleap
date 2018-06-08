package ml.combust.mleap.grpc.server

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.grpc.{Server, ServerBuilder}
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb.MleapGrpc

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.language.existentials
import scala.util.{Failure, Success, Try}

class RunServer(config: Config) {
  private val logger = Logger(classOf[RunServer])

  private var system: Option[ActorSystem] = None
  private var grpcThreadPool: Option[ExecutorService] = None
  private var grpcServer: Option[Server] = None

  def shutdown(): Unit = {
    for (s <- grpcServer) {
      logger.info("Shutting down gRPC server")
      s.shutdown()
      s.awaitTermination(30, TimeUnit.SECONDS)
      logger.info("Server shut down")
    }

    for (tp <- grpcThreadPool) {
      logger.info("Shutting down thread gRPC pool")
      tp.shutdown()
      tp.awaitTermination(30, TimeUnit.SECONDS)
      logger.info("gRPC thread pool shut down")
    }

    for (s <- system) {
      logger.info("Shutting down actor system")
      s.terminate()
      Try(Await.result(s.whenTerminated, 30.seconds))
      logger.info("Actor system shut down")
    }
  }

  def run(): Unit = {
    sys.addShutdownHook { shutdown() }

    Try {
      logger.info("Starting MLeap gRPC Server")

      val port: Int = config.getInt("port")
      val threads: Option[Int] = if (config.hasPath("threads")) Some(config.getInt("threads")) else None

      implicit val system: ActorSystem = ActorSystem("MleapGrpcServer")
      this.system = Some(system)
      implicit val materializer: Materializer = ActorMaterializer()

      val threadCount = threads.getOrElse {
        Math.min(Math.max(Runtime.getRuntime.availableProcessors() * 4, 32), 64)
      }
      logger.info(s"Creating thread pool for server with size $threadCount")
      grpcThreadPool = Some(Executors.newFixedThreadPool(threadCount))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(grpcThreadPool.get)

      logger.info(s"Creating executor service")
      val executor: MleapExecutor = MleapExecutor(system)
      val grpcService: GrpcServer = new GrpcServer(executor)
      val builder = ServerBuilder.forPort(port)
      builder.intercept(new ErrorInterceptor)
      builder.addService(MleapGrpc.bindService(grpcService, ec))
      grpcServer = Some(builder.build())

      logger.info(s"Starting server on port $port")
      grpcServer.get.start()

      grpcServer.get.awaitTermination()
    } match {
      case Success(_) =>
      case Failure(err) =>
        logger.error("Error encountered starting server", err)
        shutdown()
    }
  }
}
