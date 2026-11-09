# MLeap XGBoost Runtime

We provide two implementations of XGBoost for use at runtime:
- XGBoost4j (from `ml.dmlc.xgboost4j`): the official implementation backed by the native XGBoost library.
- XGBoost-Predictor (from `com.yelp:xgboost-predictor`): a much faster pure-JVM implementation with no JNI on the prediction hot path.

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

## Sparse inputs

Both implementations read a `SparseVector` or sparse `Tensor` the same way. An index that is not stored is missing, and a stored entry is a value. Dense inputs keep every zero as a value.

This matches a model trained on sparse rows by XGBoost 1.7.6, or by an xgboost4j-spark that passes the stored indices and values to the `DMatrix`. Such a model learns a missing feature as the default direction of each split.

A model trained on the dense row learned those gaps as `0.0`. Upstream xgboost4j-spark 3.4.0 does this: it expands a sparse vector with `toArray`. To score such a model, pass a dense input, or a sparse one with the zeros stored.

If the model was trained with `missing=0.0`, a stored `0.0` is missing too.

MLeap 0.25.3 read every gap as `0.0`. On that release, models trained on sparse rows scored differently from their training run, and rows that were mostly gaps all got the same score. 0.26.0 reads gaps as missing.
