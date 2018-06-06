package ml.combust.mleap.grpc.server

import io.grpc._

class ErrorInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall[ReqT, RespT](call) {
      override def close(status: Status, trailers: Metadata): Unit = {
        if (status.isOk) { call.close(status, trailers) }
        else {
          status.getCause match {
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
