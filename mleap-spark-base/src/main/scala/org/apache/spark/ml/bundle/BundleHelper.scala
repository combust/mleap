package org.apache.spark.ml.bundle

/**
  * Created by hollinwilkins on 5/25/17.
  */
object BundleHelper {
  def sampleDataframeMessage(klazz: Class[_]): String = {
    s"""
      |*****************************************************************************************************
      |Must provide a sample data frame for the ${klazz.getCanonicalName} transformer.
      |
      |See more information here:
      |https://combust.github.io/mleap-docs//troubleshooting.html#must-provide-a-sample-dataset-for-the-x-transformer
      |
      |// Use your Spark ML Pipeline to transform the Spark DataFrame
      |val transformedDataset = sparkTransformer.transform(sparkDataset)
      |
      |// Create a custom SparkBundleContext and provide the transformed DataFrame
      |implicit val sbc = SparkBundleContext().withDataset(transformedDataset)
      |
      |// Serialize the pipeline as you would normally
      |Using(BundleFile(file))) { bf =>
      |  sparkTransformer.writeBundle.save(bf).get
      |}.get
      |*****************************************************************************************************
    """.stripMargin
  }
}
