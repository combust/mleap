package ml.combust.mleap.grpc.server

import java.util.concurrent.{Executors, TimeUnit}

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.grpc.ServerBuilder
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb.MleapGrpc

import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials
import scala.util.{Failure, Success, Try}

class RunServer(config: Config)
               (implicit system: ActorSystem) {
  private val logger = Logger(classOf[RunServer])

  private var coordinator: Option[CoordinatedShutdown] = None

  def run(): Unit = {
    Try {
      logger.info("Starting MLeap gRPC Server")

      val coordinator = CoordinatedShutdown(system)
      this.coordinator = Some(coordinator)

      implicit val materializer: Materializer = ActorMaterializer()

      val mleapExecutor = MleapExecutor(system)
      val port: Int = config.getInt("port")
      val threads: Option[Int] = if (config.hasPath("threads")) Some(config.getInt("threads")) else None
      val threadCount = threads.getOrElse {
        Math.min(Math.max(Runtime.getRuntime.availableProcessors() * 4, 32), 64)
      }

      logger.info(s"Creating thread pool for server with size $threadCount")
      val grpcThreadPool = Executors.newFixedThreadPool(threadCount)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(grpcThreadPool)

      coordinator.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "threadPoolShutdownNow") {
        () =>
          Future {
            logger.info("Shutting down gRPC thread pool")
            grpcThreadPool.shutdown()
            grpcThreadPool.awaitTermination(5, TimeUnit.SECONDS)

            Done
          }
      }

      logger.info(s"Creating executor service")
      val grpcService: GrpcServer = new GrpcServer(mleapExecutor)
      val builder = ServerBuilder.forPort(port)
      builder.intercept(new ErrorInterceptor)
      builder.addService(MleapGrpc.bindService(grpcService, ec))
      val grpcServer = builder.build()

      logger.info(s"Starting server on port $port")
      grpcServer.start()

      coordinator.addTask(CoordinatedShutdown.PhaseServiceUnbind, "grpcServiceShutdown") {
        () =>
          Future {
            logger.info("Shutting down gRPC")
            grpcServer.shutdown()
            grpcServer.awaitTermination(10, TimeUnit.SECONDS)
            Done
          }(ExecutionContext.global)
      }

      coordinator.addTask(CoordinatedShutdown.PhaseServiceStop, "grpcServiceShutdownNow") {
        () =>
          Future {
            if (!grpcServer.isShutdown) {
              logger.info("Shutting down gRPC NOW!")

              grpcServer.shutdownNow()
              grpcServer.awaitTermination(5, TimeUnit.SECONDS)
            }

            Done
          }(ExecutionContext.global)
      }
    } match {
      case Success(_) =>
      case Failure(err) =>
        logger.error("Error encountered starting server", err)
        for (c <- this.coordinator) {
          c.run(CoordinatedShutdown.UnknownReason)
        }
        throw err
    }
  }
}
