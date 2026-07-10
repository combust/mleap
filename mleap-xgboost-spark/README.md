# MLeap XGBoost Spark

This is the XGBoost Spark integration for MLeap. It provides Bundle Ops for serializing and deserializing `XGBoostClassificationModel` and `XGBoostRegressionModel` to a Bundle.ML file.

Add the dependency to your project, setting `ml.combust.mleap.version` and `scala.binary.version` to the values you need.

```
<dependency>
  <groupId>ml.combust.mleap</groupId>
  <artifactId>mleap-xgboost-spark_${scala.binary.version}</artifactId>
  <version>${ml.combust.mleap.version}</version>
</dependency>
```

Once you have added `mleap-xgboost-spark` as a dependency, train an `XGBoostClassificationModel` or `XGBoostRegressionModel` using the `XGBoostClassifier` or `XGBoostRegressor` estimator. You can then export the fitted model to MLeap along with the rest of your Spark pipeline.

[Exporting Spark pipelines with MLeap](https://combust.github.io/mleap-docs/spark/)

## Thank You

Thank you to [Swoop](https://www.swoop.com/) for supporting this
integration.

