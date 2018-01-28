package ml.combust.bundle.v07.converter

/**
  * Created by hollinwilkins on 1/27/18.
  */
object ConverterRegistry {
  val baseModelConverter: BaseModelConverter = new BaseModelConverter
  val baseNodeConverter: BaseNodeConverter = new BaseNodeConverter
}

case class ConverterRegistry(models: Map[String, ModelConverter]) {
  def modelConverter(name: String): ModelConverter = {
    models.getOrElse(name, ConverterRegistry.baseModelConverter)
  }

  def withModelConverter(name: String, modelConverter: ModelConverter): ConverterRegistry = {
    copy(models = models + (name -> modelConverter))
  }
}
