package ml.combust.mleap.executor.error

class NotFoundException(message: String,
                        cause: Throwable) extends ExecutorException(message, cause) {
  def this(message: String) = this(message, null)
  def this(err: Throwable) = this(err.getMessage, err)
}
