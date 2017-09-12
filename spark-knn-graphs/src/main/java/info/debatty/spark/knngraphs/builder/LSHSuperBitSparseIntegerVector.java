/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.utils.SparseIntegerVector;

/**
 *
 * @author Thibault Debatty
 */
public class LSHSuperBitSparseIntegerVector extends LSHSuperBit<SparseIntegerVector> {

    public LSHSuperBitSparseIntegerVector() {
        super();
        this.similarity = new SimilarityInterface<SparseIntegerVector>() {

            public double similarity(SparseIntegerVector value1, SparseIntegerVector value2) {
                return value1.cosineSimilarity(value2);
            }
        };
    }
    
    @Override
    int[] hash(SparseIntegerVector node_value) {
        return lsh.hash(node_value);
    }
    
}
