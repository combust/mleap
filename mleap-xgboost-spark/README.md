# MLeap XGBoost Spark

This is the XGBoost Spark integration for MLeap. It provides Bundle Ops for serializing/deserialize XGBoostClassificationModel and XGBoostRegressionModel to a Bundle.ML file.

Make sure to set `ml.combust.mleap.version` to the desired MLeap version.

```
<dependency>
  <groupId>ml.combust.mleap</groupId>
  <artifactId>mleap-xgboost-spark_${scala.binary.version}</artifactId>
  <version>${ml.combust.mleap.version}</version>
</dependency>
```

Once you have added `mleap-xgboost-spark` as a dependency to your project, you should be able to train an `XGBoostClassificationModel` or `XGBoostRegressionModel` using the `XGBoostEstimator`. You will then be able to export the classifier or regression to MLeap along with the rest of your Spark pipeline.

[Exporting Spark pipelines with MLeap](http://mleap-docs.combust.ml/spark/)

## Thank You

Thank you to [Swoop](https://www.swoop.com/) for supporting this
integration.

