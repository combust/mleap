package ml.combust.bundle.v07.converter

/**
  * Created by hollinwilkins on 1/27/18.
  */
case class ConverterContext(registry: ConverterRegistry,
                            in: ConverterFile,
                            out: ConverterFile) {
  def updatePath(name: String): ConverterContext = {
    ConverterContext(registry, in.updatePath(name), out.updatePath(name))
  }
}
