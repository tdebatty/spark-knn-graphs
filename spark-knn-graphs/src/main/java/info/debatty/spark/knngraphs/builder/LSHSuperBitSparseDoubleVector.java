package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.utils.SparseDoubleVector;

/**
 *
 * @author Thibault Debatty
 */
public class LSHSuperBitSparseDoubleVector extends LSHSuperBit<SparseDoubleVector> {

    public LSHSuperBitSparseDoubleVector() {
        super();
        this.similarity = new SimilarityInterface<SparseDoubleVector>() {

            public double similarity(SparseDoubleVector value1, SparseDoubleVector value2) {
                return value1.cosineSimilarity(value2);
            }
        };
    }
    
    @Override
    int[] hash(SparseDoubleVector node_value) {
        return lsh.hash(node_value);
    }
    
}
