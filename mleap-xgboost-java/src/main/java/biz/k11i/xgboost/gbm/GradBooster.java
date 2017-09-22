package biz.k11i.xgboost.gbm;

import biz.k11i.xgboost.util.FVec;
import biz.k11i.xgboost.util.ModelReader;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface of gradient boosting model.
 */
public interface GradBooster extends Serializable {
    class Factory {
        /**
         * Creates a gradient booster from given name.
         *
         * @param name name of gradient booster
         * @return created gradient booster
         */
        public static GradBooster createGradBooster(String name) {
            if ("gbtree".equals(name)) {
                return new GBTree();
            } else if ("gblinear".equals(name)) {
                return new GBLinear();
            }

            throw new IllegalArgumentException(name + " is not supported model.");
        }
    }

    void setNumClass(int num_class);

    /**
     * Loads model from stream.
     *
     * @param reader       input stream
     * @param with_pbuffer whether the incoming data contains pbuffer
     * @throws IOException If an I/O error occurs
     */
    void loadModel(ModelReader reader, boolean with_pbuffer) throws IOException;

    /**
     * Generates predictions for given feature vector.
     *
     * @param feat        feature vector
     * @param ntree_limit limit the number of trees used in prediction
     * @return prediction result
     */
    double[] predict(FVec feat, int ntree_limit);

    /**
     * Generates a prediction for given feature vector.
     * <p>
     * This method only works when the model outputs single value.
     * </p>
     *
     * @param feat        feature vector
     * @param ntree_limit limit the number of trees used in prediction
     * @return prediction result
     */
    double predictSingle(FVec feat, int ntree_limit);

    /**
     * Predicts the leaf index of each tree. This is only valid in gbtree predictor.
     *
     * @param feat        feature vector
     * @param ntree_limit limit the number of trees used in prediction
     * @return predicted leaf indexes
     */
    int[] predictLeaf(FVec feat, int ntree_limit);
}

abstract class GBBase implements GradBooster {
    protected int num_class;

    public void setNumClass(int num_class) {
        this.num_class = num_class;
    }
}