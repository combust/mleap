package biz.k11i.xgboost.gbm;

import biz.k11i.xgboost.util.FVec;
import biz.k11i.xgboost.util.ModelReader;

import java.io.IOException;
import java.io.Serializable;

/**
 * Linear booster implementation
 */
public class GBLinear extends GBBase {

    private ModelParam mparam;
    private float[] weights;

    GBLinear() {
        // do nothing
    }

    @Override
    public void loadModel(ModelReader reader, boolean ignored_with_pbuffer) throws IOException {
        mparam = new ModelParam(reader);
        reader.readInt(); // read padding
        weights = reader.readFloatArray((mparam.num_feature + 1) * mparam.num_output_group);
    }

    @Override
    public double[] predict(FVec feat, int ntree_limit) {
        double[] preds = new double[mparam.num_output_group];
        for (int gid = 0; gid < mparam.num_output_group; ++gid) {
            preds[gid] = pred(feat, gid);
        }
        return preds;
    }

    @Override
    public double predictSingle(FVec feat, int ntree_limit) {
        if (mparam.num_output_group != 1) {
            throw new IllegalStateException(
                    "Can't invoke predictSingle() because this model outputs multiple values: "
                            + mparam.num_output_group);
        }
        return pred(feat, 0);
    }

    double pred(FVec feat, int gid) {
        double psum = bias(gid);
        double featValue;
        for (int fid = 0; fid < mparam.num_feature; ++fid) {
            featValue = feat.fvalue(fid);
            if (!Double.isNaN(featValue)) {
                psum += featValue * weight(fid, gid);
            }
        }
        return psum;
    }

    @Override
    public int[] predictLeaf(FVec feat, int ntree_limit) {
        throw new UnsupportedOperationException("gblinear does not support predict leaf index");
    }

    float weight(int fid, int gid) {
        return weights[(fid * mparam.num_output_group) + gid];
    }

    float bias(int gid) {
        return weights[(mparam.num_feature * mparam.num_output_group) + gid];
    }

    static class ModelParam implements Serializable {
        /*! \brief number of features */
        final int num_feature;
        /*!
         * \brief how many output group a single instance can produce
         *  this affects the behavior of number of output we have:
         *    suppose we have n instance and k group, output will be k*n
         */
        final int num_output_group;
        /*! \brief reserved parameters */
        final int[] reserved;

        ModelParam(ModelReader reader) throws IOException {
            num_feature = reader.readInt();
            num_output_group = reader.readInt();
            reserved = reader.readIntArray(32);
            reader.readInt(); // read padding
        }
    }

}
