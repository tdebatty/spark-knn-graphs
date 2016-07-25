/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.lsh.SuperBit;

/**
 * A k-nn graph builder that bins input items using LSH SuperBit algorithm.
 * Inside the buckets, brute-force is used to build the sub-graphs.
 * It is meant for cosine similarity, but another similarity measure can be
 * defined.
 * Uses double[] as input type.
 * @author Thibault Debaty
 */
public class LSHSuperBitDoubleArray extends LSHSuperBit<double[]>{

    public LSHSuperBitDoubleArray() {
        super();
        this.similarity = new SimilarityInterface<double[]>() {

            public double similarity(double[] value1, double[] value2) {
                return SuperBit.cosineSimilarity(value1, value2);
            }
        };
    }
    
    @Override
    int[] hash(double[] node_value) {
        return lsh.hash(node_value);
    }
    
}
