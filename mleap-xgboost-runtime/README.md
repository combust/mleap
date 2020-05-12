# MLeap XGBoost Runtime

We provide two implementation of XGboost for use at runtime:
- XGboost4j (from `ml.dmlc.xgboost4j`): this is the official C++ implementation.
- XGBoost-Predictor (from `biz.k11i.xgboost-predictor`): this is a much faster implementation written directly in Java.

By default, MLeap Bundles are de-serialized into XGboost4j Booster objects.
In order to use the Predictor implementation, you may:

1. Create a `resources/reference.conf` file in your project, like this:
    ```
    ml.combust.mleap.xgboost.ops = [
      "ml.combust.mleap.xgboost.runtime.bundle.ops.XGBoostPredictorClassificationOp",
      "ml.combust.mleap.xgboost.runtime.bundle.ops.XGBoostRegressionOp"
    ]
    ```
2. add this to your project's pom file:
    ```
    <!-- Append our reference.conf into MLeap's reference.conf so our Ops are registered -->
      <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
      <resource>reference.conf</resource>
      </transformer>
    ```
