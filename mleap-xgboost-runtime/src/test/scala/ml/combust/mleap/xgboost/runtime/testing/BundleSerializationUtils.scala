package ml.combust.mleap.xgboost.runtime.testing

import java.io.File
import java.nio.file.{Files, Path}

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.runtime.{MleapContext, frame}
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.bundle.ops.{XGBoostClassificationOp, XGBoostPredictorClassificationOp}
import resource.managed


trait BundleSerializationUtils {

  def serializeModelToMleapBundle(transformer: Transformer): File = {
    import ml.combust.mleap.runtime.MleapSupport._

    val tempDirPath = {
      val temp: Path = Files.createTempDirectory("xgboost-runtime-parity")
      temp.toFile.deleteOnExit()
      temp.toAbsolutePath
    }

    val file = new File(s"${tempDirPath}/${this.getClass.getName}.zip")

    for(bf <- managed(BundleFile(file))) {
      transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
    }
    file
  }

  def loadMleapTransformerFromBundle(bundleFile: File)
                                    (implicit context: MleapContext): frame.Transformer = {

    import ml.combust.mleap.runtime.MleapSupport._

    (for(bf <- managed(BundleFile(bundleFile))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def loadXGBoostPredictorFromBundle(bundleFile: File)
                                    (implicit context: MleapContext): frame.Transformer = {

    // Register a different Op to change the deserialization class between tests.
    // Use to deserialize with Predictor rather than xgboost4j
    context.bundleRegistry.register(new XGBoostPredictorClassificationOp())
    val transformer = loadMleapTransformerFromBundle(bundleFile)
    context.bundleRegistry.register(new XGBoostClassificationOp())  // revert to the original Op
    transformer
  }
}
