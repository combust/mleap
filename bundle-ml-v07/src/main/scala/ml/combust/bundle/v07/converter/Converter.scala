package ml.combust.bundle.v07.converter

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle

import scala.util.Try

/**
  * Created by hollinwilkins on 1/27/18.
  */
class Converter(registry: ConverterRegistry) {
  private val bundleConverter = new BundleConverter
  private val nodeConverter = new NodeConverter

  def convert(in: BundleFile, out: BundleFile): Try[Unit] = {
    val inFile = ConverterFile(in.fs, in.path)
    val outFile = ConverterFile(in.fs, in.path)
    var context = ConverterContext(registry, inFile, outFile)

    bundleConverter.convertFile(inFile.file(Bundle.bundleJson),
      outFile.file(Bundle.bundleJson))

    context = context.updatePath("root")
    nodeConverter.convert(context)
  }
}
