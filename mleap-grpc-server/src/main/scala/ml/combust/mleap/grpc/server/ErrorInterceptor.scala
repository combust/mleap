package ml.combust.mleap.grpc.server

import io.grpc._
import ml.combust.mleap.executor.error.{AlreadyExistsException, NotFoundException, TimeoutException}

class ErrorInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall[ReqT, RespT](call) {
      override def close(status: Status, trailers: Metadata): Unit = {
        if (status.isOk) { call.close(status, trailers) }
        else {
          status.getCause match {
            case err: NotFoundException =>
              val status = Status.NOT_FOUND.withCause(err).withDescription(err.getMessage)
              call.close(status, trailers)
            case err: AlreadyExistsException =>
              val status = Status.ALREADY_EXISTS.withCause(err).withDescription(err.getMessage)
              call.close(status, trailers)
            case err: TimeoutException =>
              val status = Status.DEADLINE_EXCEEDED.withCause(err).withDescription(err.getMessage)
              call.close(status, trailers)
            case err: IllegalArgumentException =>
              val status = Status.FAILED_PRECONDITION.withCause(err).withDescription(err.getMessage)
              call.close(status, trailers)
            case _ => call.close(status, trailers)
          }
        }
      }
    }

    next.startCall(wrappedCall, headers)
  }
}
