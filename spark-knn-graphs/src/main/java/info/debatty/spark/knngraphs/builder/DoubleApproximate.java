/*
 * The MIT License
 *
 * Copyright 2015 tibo.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.utils.SparseDoubleVector;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author tibo
 */
public class DoubleApproximate extends AbstractPartitioningBuilder<SparseDoubleVector> {
    
    info.debatty.java.lsh.LSHSuperBit lsh;
    protected int dim;
    
    
    public DoubleApproximate() {
        super();
        this.similarity = new SimilarityInterface<SparseDoubleVector>() {

            public double similarity(SparseDoubleVector value1, SparseDoubleVector value2) {
                return value1.cosineSimilarity(value2);
            }
        };
    }
    
    public void setDim(int dim) {
        this.dim = dim;
    }

    @Override
    protected JavaPairRDD<Integer, Node<SparseDoubleVector>> _binNodes(JavaRDD<Node<SparseDoubleVector>> nodes) throws Exception {
        final long count = nodes.count();
        lsh = new info.debatty.java.lsh.LSHSuperBit(stages, buckets, this.dim);
        
        
        return nodes.flatMapToPair(
                new PairFlatMapFunction<Node<SparseDoubleVector>, Integer, Node<SparseDoubleVector>>() {
            
            public Iterable<Tuple2<Integer, Node<SparseDoubleVector>>> call(Node<SparseDoubleVector> n) throws Exception {
                ArrayList<Tuple2<Integer, Node<SparseDoubleVector>>> r = new ArrayList<Tuple2<Integer, Node<SparseDoubleVector>>>();
                int[] hash = lsh.hash(n.value);
                
                // Downsample vectors using DIMSUM
                n.value.sampleDIMSUM(0.5, (int) count, dim);
                
                
                for (int v : hash) {
                    r.add(new Tuple2<Integer, Node<SparseDoubleVector>>(v, n));
                }
                
                return r;
            }
        });
    }
    
}
