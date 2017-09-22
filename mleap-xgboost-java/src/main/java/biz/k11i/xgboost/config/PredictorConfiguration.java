package biz.k11i.xgboost.config;

import biz.k11i.xgboost.learner.ObjFunction;

public class PredictorConfiguration {
    public static class Builder {
        private PredictorConfiguration predictorConfiguration;

        Builder() {
            predictorConfiguration = new PredictorConfiguration();
        }

        public Builder objFunction(ObjFunction objFunction) {
            predictorConfiguration.objFunction = objFunction;
            return this;
        }

        public PredictorConfiguration build() {
            PredictorConfiguration result = predictorConfiguration;
            predictorConfiguration = null;
            return result;
        }
    }

    public static final PredictorConfiguration DEFAULT = new PredictorConfiguration();

    private ObjFunction objFunction;

    public ObjFunction getObjFunction() {
        return objFunction;
    }

    public static Builder builder() {
        return new Builder();
    }
}
