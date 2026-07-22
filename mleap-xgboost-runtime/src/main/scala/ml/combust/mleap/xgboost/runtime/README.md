# MLeap XGBoost runtime — pure-JVM predictor

This package is MLeap's runtime for scoring XGBoost models. As of the XGBoost
3.3.0 migration it serves predictions through a **pure-JVM predictor** (no JNI on
the hot path), not the `xgboost4j` `Booster`.

## What lives here

The prediction engine (reader + tree traversal) is the external
[`com.yelp:xgboost-predictor`](https://github.com/Yelp/xgboost-predictor)
artifact (`com.yelp.xgboost.*`: `Predictor`, the tree traversals, `FVec`,
`ObjFunction`, `PredictorFactory`). This module is the MLeap glue around it:

- `bundle/ops/` (Scala) — the MLeap ops registered in `reference.conf`
  (`XGBoostPredictor{Classification,Regression}Op`), plus the older Booster ops.
- `struct/FVecFactory.scala`, `XgbConverters.scala` — MLeap `Vector`/`Tensor` to
  `FVec` conversion (double→float cast, `missing`/zero-as-NA handling).
- `XGBoostPredictor{Classification,Regression}Model.scala` — the MLeap models
  that wrap a `Predictor`.

## Why pure-JVM

Inference avoids the JNI boundary and the per-call `DMatrix` allocation that the
`xgboost4j` `Booster` requires. The reader is dual-format (leading byte `{` →
UBJSON; otherwise legacy pre-1.0 binary), so both existing deployed bundles and
new 3.3.0 bundles load through the same engine with no bundle migration.

## Objectives and boosters

`gbtree` only. Supported objectives include `binary:logistic`, `reg:logistic`,
`reg:squarederror`, `reg:linear`, `reg:squaredlogerror`, `reg:gamma`,
`reg:tweedie`, `count:poisson`, `multi:softmax`, `multi:softprob`, `rank:*`.
`dart` and vector-leaf / multi-target trees are rejected with a clear error.
Categorical splits are supported.

## Benchmark

The single-row prediction benchmark (JNI Booster vs pure-JVM predictor) now
lives in the `com.yelp:xgboost-predictor` repo. See its README for numbers and
how to run it.
