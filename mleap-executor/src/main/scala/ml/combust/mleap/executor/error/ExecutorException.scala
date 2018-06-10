package ml.combust.mleap.executor.error

class ExecutorException(message: String,
                        cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(err: Throwable) = this(err.getMessage, err)
}
