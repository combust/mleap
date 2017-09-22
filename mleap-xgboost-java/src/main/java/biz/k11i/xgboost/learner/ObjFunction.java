package biz.k11i.xgboost.learner;

import biz.k11i.xgboost.config.PredictorConfiguration;
import net.jafama.FastMath;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Objective function implementations.
 */
public class ObjFunction implements Serializable {
    private static final Map<String, ObjFunction> FUNCTIONS = new HashMap<>();

    static {
        register("rank:pairwise", new ObjFunction());
        register("binary:logistic", new RegLossObjLogistic());
        register("binary:logitraw", new ObjFunction());
        register("multi:softmax", new SoftmaxMultiClassObjClassify());
        register("multi:softprob", new SoftmaxMultiClassObjProb());
        register("reg:linear", new ObjFunction());
    }

    /**
     * Gets {@link ObjFunction} from given name.
     *
     * @param name name of objective function
     * @return objective function
     */
    public static ObjFunction fromName(String name) {
        ObjFunction result = FUNCTIONS.get(name);
        if (result == null) {
            throw new IllegalArgumentException(name + " is not supported objective function.");
        }
        return result;
    }

    /**
     * Register an {@link ObjFunction} for a given name.
     *
     * @param name name of objective function
     * @param objFunction objective function
     * @deprecated This method will be made private. Please use {@link PredictorConfiguration.Builder#objFunction(ObjFunction)} instead.
     */
    public static void register(String name, ObjFunction objFunction) {
        FUNCTIONS.put(name, objFunction);
    }

    /**
     * Uses Jafama's {@link FastMath#exp(double)} instead of {@link Math#exp(double)}.
     *
     * @param useJafama {@code true} if you want to use Jafama's {@link FastMath#exp(double)},
     *                  or {@code false} if you don't want to use it but JDK's {@link Math#exp(double)}.
     */
    public static void useFastMathExp(boolean useJafama) {
        if (useJafama) {
            register("binary:logistic", new RegLossObjLogistic_Jafama());
            register("multi:softprob", new SoftmaxMultiClassObjProb_Jafama());

        } else {
            register("binary:logistic", new RegLossObjLogistic());
            register("multi:softprob", new SoftmaxMultiClassObjProb());
        }
    }

    /**
     * Transforms prediction values.
     *
     * @param preds prediction
     * @return transformed values
     */
    public double[] predTransform(double[] preds) {
        // do nothing
        return preds;
    }

    /**
     * Transforms a prediction value.
     *
     * @param pred prediction
     * @return transformed value
     */
    public double predTransform(double pred) {
        // do nothing
        return pred;
    }

    /**
     * Logistic regression.
     */
    static class RegLossObjLogistic extends ObjFunction {
        @Override
        public double[] predTransform(double[] preds) {
            for (int i = 0; i < preds.length; i++) {
                preds[i] = sigmoid(preds[i]);
            }
            return preds;
        }

        @Override
        public double predTransform(double pred) {
            return sigmoid(pred);
        }

        double sigmoid(double x) {
            return (1 / (1 + Math.exp(-x)));
        }
    }

    /**
     * Logistic regression.
     * <p>
     * Jafama's {@link FastMath#exp(double)} version.
     * </p>
     */
    static class RegLossObjLogistic_Jafama extends RegLossObjLogistic {
        double sigmoid(double x) {
            return (1 / (1 + FastMath.exp(-x)));
        }
    }

    /**
     * Multiclass classification.
     */
    static class SoftmaxMultiClassObjClassify extends ObjFunction {
        @Override
        public double[] predTransform(double[] preds) {
            int maxIndex = 0;
            double max = preds[0];
            for (int i = 1; i < preds.length; i++) {
                if (max < preds[i]) {
                    maxIndex = i;
                    max = preds[i];
                }
            }

            return new double[]{maxIndex};
        }

        @Override
        public double predTransform(double pred) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Multiclass classification (predicted probability).
     */
    static class SoftmaxMultiClassObjProb extends ObjFunction {
        @Override
        public double[] predTransform(double[] preds) {
            double max = preds[0];
            for (int i = 1; i < preds.length; i++) {
                max = Math.max(preds[i], max);
            }

            double sum = 0;
            for (int i = 0; i < preds.length; i++) {
                preds[i] = exp(preds[i] - max);
                sum += preds[i];
            }

            for (int i = 0; i < preds.length; i++) {
                preds[i] /= (float) sum;
            }

            return preds;
        }

        @Override
        public double predTransform(double pred) {
            throw new UnsupportedOperationException();
        }

        double exp(double x) {
            return Math.exp(x);
        }
    }

    /**
     * Multiclass classification (predicted probability).
     * <p>
     * Jafama's {@link FastMath#exp(double)} version.
     * </p>
     */
    static class SoftmaxMultiClassObjProb_Jafama extends SoftmaxMultiClassObjProb {
        @Override
        double exp(double x) {
            return FastMath.exp(x);
        }
    }
}
