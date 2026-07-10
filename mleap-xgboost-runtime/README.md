# MLeap XGBoost Runtime

We provide two implementations of XGBoost for use at runtime:
- XGBoost4j (from `ml.dmlc.xgboost4j`): this is the official implementation backed by the native XGBoost library.
- XGBoost-Predictor (from `ai.h2o.xgboost-predictor`): this is a much faster implementation written directly in Java.

By default, MLeap bundles are deserialized into XGBoost4j Booster objects.
To use the Predictor implementation instead:

1. Create a `resources/reference.conf` file in your project, like this:
    ```
    ml.combust.mleap.xgboost.ops = [
      "ml.combust.mleap.xgboost.runtime.bundle.ops.XGBoostPredictorClassificationOp",
      "ml.combust.mleap.xgboost.runtime.bundle.ops.XGBoostPredictorRegressionOp"
    ]
    ```
2. Add this to your project's pom file so the shade plugin appends our `reference.conf` into MLeap's, registering our Ops:
    ```
    <!-- Append our reference.conf into MLeap's reference.conf so our Ops are registered -->
      <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
      <resource>reference.conf</resource>
      </transformer>
    ```
