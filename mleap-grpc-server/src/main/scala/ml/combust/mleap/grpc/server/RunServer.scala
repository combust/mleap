package ml.combust.mleap.grpc.server

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.grpc.ServerBuilder
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb.MleapGrpc

import scala.concurrent.ExecutionContext
import scala.language.existentials

class RunServer(config: Config) {
  private val logger = Logger(classOf[RunServer])

  val port: Int = config.getInt("port")
  val threads: Option[Int] = if (config.hasPath("threads")) Some(config.getInt("threads")) else None

  def run(): Unit = {
    logger.info("Starting MLeap gRPC Server")
    implicit val system: ActorSystem = ActorSystem("MleapGrpcServer")
    implicit val materializer: Materializer = ActorMaterializer()

    val threadCount = threads.getOrElse {
      Math.min(Math.max(Runtime.getRuntime.availableProcessors() * 4, 32), 64)
    }
    logger.info(s"Creating thread pool for server with size $threadCount")
    val grpcThreadPool = Executors.newFixedThreadPool(threadCount)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(grpcThreadPool)

    logger.info(s"Creating executor service")
    val executor: MleapExecutor = MleapExecutor(system)
    val grpcServer: GrpcServer = new GrpcServer(executor)
    val builder = ServerBuilder.forPort(port)
    builder.intercept(new ErrorInterceptor)
    builder.addService(MleapGrpc.bindService(grpcServer, ec))
    val server = builder.build()

    logger.info(s"Starting server on port $port")
    server.start()

    sys.addShutdownHook {
      logger.info("Shutting down...")
      server.shutdown()

      logger.info("Shutting down thread pool...")
      grpcThreadPool.shutdown()
      grpcThreadPool.awaitTermination(30, TimeUnit.SECONDS)
    }

    server.awaitTermination()
  }
}
