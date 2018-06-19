package ml.combust.mleap.springboot

import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.protobuf.ProtocolStringList
import ml.combust.mleap.executor
import ml.combust.mleap.pb._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.Try
import ml.combust.mleap.runtime.types.BundleTypeConverters._

object TypeConverters {
  import scala.language.implicitConversions

  implicit def getTimeout(ms: Int): FiniteDuration = FiniteDuration(ms, TimeUnit.MILLISECONDS)

  implicit def pbToExecutorLoadModelRequest(request: LoadModelRequest): executor.LoadModelRequest =
    executor.LoadModelRequest(modelName = request.modelName,
                              uri = URI.create(request.uri),
                              config = request.config.get,
                              force = request.force)

  implicit def javaPbToExecutorLoadModelRequest(request: Mleap.LoadModelRequest): executor.LoadModelRequest =
    executor.LoadModelRequest(modelName = request.getModelName,
                              uri = URI.create(request.getUri),
                              config = request.getConfig,
                              force = request.getForce)

  implicit def pbToExecutorModelConfig(config: ModelConfig): executor.ModelConfig =
    executor.ModelConfig(memoryTimeout = config.memoryTimeout.millis, diskTimeout = config.diskTimeout.millis)

  implicit def javaPbToExecutorModelConfig(config: Mleap.ModelConfig): executor.ModelConfig =
    executor.ModelConfig(memoryTimeout = config.getMemoryTimeout.millis, diskTimeout = config.getDiskTimeout.millis)

  implicit def executorToPbModelConfig(config: executor.ModelConfig): ModelConfig =
    ModelConfig(memoryTimeout = config.memoryTimeout.toMillis, diskTimeout = config.diskTimeout.toMillis)

  implicit def executorToPbModel(model: executor.Model): Model =
    Model(name = model.name, uri = model.uri.toString, config = Some(model.config))

  implicit def pbToExecutorModel(model: Model): executor.Model =
    executor.Model(name = model.name, uri = URI.create(model.uri), config = model.config.get)

  implicit def executorToPbBundleMeta(meta: executor.BundleMeta): BundleMeta =
    BundleMeta(bundle = Some(meta.info.asBundle), inputSchema = Some(meta.inputSchema), outputSchema = Some(meta.outputSchema))

  implicit def pbToExecutorTransformOptions(options: TransformOptions): executor.TransformOptions =
    executor.TransformOptions(select = options.select, selectMode = options.selectMode)

  implicit def javaPbToExecutorTransformOptions(options: Mleap.TransformOptions): executor.TransformOptions =
    executor.TransformOptions(select = options.getSelectList, selectMode = options.getSelectMode)

  implicit def javaPbToExecutorSelectMode(sm: Mleap.SelectMode): executor.SelectMode =
    if (sm == Mleap.SelectMode.SELECT_MODE_RELAXED)
      executor.SelectMode.Relaxed
    else if (sm == Mleap.SelectMode.SELECT_MODE_STRICT)
      executor.SelectMode.Strict
    else executor.SelectMode.Strict


  implicit def javaPbToExecutorSelect(select: ProtocolStringList): Option[Seq[String]] =
    if (select.isEmpty) None else Some(select.toArray().map(_.asInstanceOf[String]).toSeq)

  implicit def pbToExecutorSelectMode(sm: SelectMode): executor.SelectMode =
    if (sm.isSelectModeRelaxed)
      executor.SelectMode.Relaxed
    else if (sm.isSelectModeStrict)
      executor.SelectMode.Strict
    else executor.SelectMode.Strict

  implicit def pbToExecutorSelect(select: Seq[String]): Option[Seq[String]] = if (select.isEmpty) None else Some(select)

  implicit class RichFuture[T](f: Future[T]) {
    def mapAll[U](pf: PartialFunction[Try[T], U])(implicit executor: ExecutionContext): Future[U] = {
      val p = Promise[U]()
      f.onComplete(r => p.complete(Try(pf(r))))(executor)
      p.future
    }
  }
}
