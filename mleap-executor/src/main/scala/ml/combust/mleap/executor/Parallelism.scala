package ml.combust.mleap.executor

object Parallelism {
  import scala.language.implicitConversions

  implicit def parallelismToInt(parallelism: Parallelism): Int = parallelism.get
  implicit def intoToParallelism(value: Int): Parallelism = Parallelism(value)
}

case class Parallelism(value: Int) {
  def get: Int = value
}
