package biz.k11i.xgboost.spark;

import biz.k11i.xgboost.util.ModelReader;

import java.io.IOException;
import java.io.Serializable;

public class SparkModelParam implements Serializable {
    public static final String MODEL_TYPE_CLS = "_cls_";
    public static final String MODEL_TYPE_REG = "_reg_";

    final String modelType;
    final String featureCol;

    final String labelCol;
    final String predictionCol;

    // classification model only
    final String rawPredictionCol;
    final double[] thresholds;

    public SparkModelParam(String modelType, String featureCol, ModelReader reader) throws IOException {
        this.modelType = modelType;
        this.featureCol = featureCol;
        this.labelCol = reader.readUTF();
        this.predictionCol = reader.readUTF();

        if (MODEL_TYPE_CLS.equals(modelType)) {
            this.rawPredictionCol = reader.readUTF();
            int thresholdLength = reader.readIntBE();
            this.thresholds = thresholdLength > 0 ? reader.readDoubleArrayBE(thresholdLength) : null;

        } else if (MODEL_TYPE_REG.equals(modelType)) {
            this.rawPredictionCol = null;
            this.thresholds = null;

        } else {
            throw new UnsupportedOperationException("Unknown modelType: " + modelType);
        }
    }

    public String getModelType() {
        return modelType;
    }

    public String getFeatureCol() {
        return featureCol;
    }

    public String getLabelCol() {
        return labelCol;
    }

    public String getPredictionCol() {
        return predictionCol;
    }

    public String getRawPredictionCol() {
        return rawPredictionCol;
    }

    public double[] getThresholds() {
        return thresholds;
    }
}
