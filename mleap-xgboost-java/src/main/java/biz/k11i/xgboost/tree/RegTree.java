package biz.k11i.xgboost.tree;

import biz.k11i.xgboost.util.FVec;
import biz.k11i.xgboost.util.ModelReader;

import java.io.IOException;
import java.io.Serializable;

/**
 * Regression tree.
 */
public class RegTree implements Serializable {
    private Param param;
    private Node[] nodes;
    private RTreeNodeStat[] stats;

    /**
     * Loads model from stream.
     *
     * @param reader input stream
     * @throws IOException If an I/O error occurs
     */
    public void loadModel(ModelReader reader) throws IOException {
        param = new Param(reader);

        nodes = new Node[param.num_nodes];
        for (int i = 0; i < param.num_nodes; i++) {
            nodes[i] = new Node(reader);
        }

        stats = new RTreeNodeStat[param.num_nodes];
        for (int i = 0; i < param.num_nodes; i++) {
            stats[i] = new RTreeNodeStat(reader);
        }
    }

    /**
     * Retrieves nodes from root to leaf and returns leaf index.
     *
     * @param feat    feature vector
     * @param root_id starting root index
     * @return leaf index
     */
    public int getLeafIndex(FVec feat, int root_id) {
        int pid = root_id;

        Node n;
        while (!(n = nodes[pid])._isLeaf) {
            pid = n.next(feat);
        }

        return pid;
    }

    /**
     * Retrieves nodes from root to leaf and returns leaf value.
     *
     * @param feat    feature vector
     * @param root_id starting root index
     * @return leaf value
     */
    public double getLeafValue(FVec feat, int root_id) {
        Node n = nodes[root_id];
        while (!n._isLeaf) {
            n = nodes[n.next(feat)];
        }

        return n.leaf_value;
    }

    /**
     * Parameters.
     */
    static class Param implements Serializable {
        /*! \brief number of start root */
        final int num_roots;
        /*! \brief total number of nodes */
        final int num_nodes;
        /*!\brief number of deleted nodes */
        final int num_deleted;
        /*! \brief maximum depth, this is a statistics of the tree */
        final int max_depth;
        /*! \brief  number of features used for tree construction */
        final int num_feature;
        /*!
         * \brief leaf vector size, used for vector tree
         * used to store more than one dimensional information in tree
         */
        final int size_leaf_vector;
        /*! \brief reserved part */
        final int[] reserved;

        Param(ModelReader reader) throws IOException {
            num_roots = reader.readInt();
            num_nodes = reader.readInt();
            num_deleted = reader.readInt();
            max_depth = reader.readInt();
            num_feature = reader.readInt();

            size_leaf_vector = reader.readInt();
            reserved = reader.readIntArray(31);
        }
    }

    static class Node implements Serializable {
        // pointer to parent, highest bit is used to
        // indicate whether it's a left child or not
        final int parent_;
        // pointer to left, right
        final int cleft_, cright_;
        // split feature index, left split or right split depends on the highest bit
        final /* unsigned */ int sindex_;
        // extra info (leaf_value or split_cond)
        final double leaf_value;
        final double split_cond;

        private final int _defaultNext;
        private final int _splitIndex;
        final boolean _isLeaf;

        // set parent
        Node(ModelReader reader) throws IOException {
            parent_ = reader.readInt();
            cleft_ = reader.readInt();
            cright_ = reader.readInt();
            sindex_ = reader.readInt();

            if (is_leaf()) {
                leaf_value = reader.readFloat();
                split_cond = Float.NaN;
            } else {
                split_cond = reader.readFloat();
                leaf_value = Float.NaN;
            }

            _defaultNext = cdefault();
            _splitIndex = split_index();
            _isLeaf = is_leaf();
        }

        boolean is_leaf() {
            return cleft_ == -1;
        }

        int split_index() {
            return (int) (sindex_ & ((1l << 31) - 1l));
        }

        int cdefault() {
            return default_left() ? cleft_ : cright_;
        }

        boolean default_left() {
            return (sindex_ >>> 31) != 0;
        }

        int next(FVec feat) {
            double fvalue = feat.fvalue(_splitIndex);
            if (fvalue != fvalue) {  // is NaN?
                return _defaultNext;
            }
            return (fvalue < split_cond) ? cleft_ : cright_;
        }
    }

    /**
     * Statistics each node in tree.
     */
    static class RTreeNodeStat implements Serializable {
        /*! \brief loss chg caused by current split */
        final float loss_chg;
        /*! \brief sum of hessian values, used to measure coverage of data */
        final float sum_hess;
        /*! \brief weight of current node */
        final float base_weight;
        /*! \brief number of child that is leaf node known up to now */
        final int leaf_child_cnt;

        RTreeNodeStat(ModelReader reader) throws IOException {
            loss_chg = reader.readFloat();
            sum_hess = reader.readFloat();
            base_weight = reader.readFloat();
            leaf_child_cnt = reader.readInt();
        }
    }
}
