package ml.combust.mleap.executor.error

class TimeoutException(message: String,
                       cause: Throwable) extends ExecutorException(message, cause) {
  def this(err: Throwable) = this(err.getMessage, err)
}
