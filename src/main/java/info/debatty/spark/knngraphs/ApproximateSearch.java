/*
 * The MIT License
 *
 * Copyright 2015 Thibault Debatty.
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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class ApproximateSearch<T> implements Serializable {

    private JavaPairRDD<Node<T>, NeighborList> graph;
    private final SimilarityInterface<T> similarity;
    private final BalancedKMedoidsPartitioner partitioner;

    /**
     *
     * @param graph
     * @param partitioning_iterations
     * @param partitioning_medoids
     * @param similarity
     */
    public ApproximateSearch(
            JavaPairRDD<Node<T>, NeighborList> graph, 
            int partitioning_iterations, 
            int partitioning_medoids, 
            SimilarityInterface<T> similarity) {

        this.similarity = similarity;
        
        // Partition the graph        
        this.partitioner = new BalancedKMedoidsPartitioner();
        partitioner.iterations = partitioning_iterations;
        partitioner.partitions = partitioning_medoids;
        partitioner.similarity = similarity;
        partitioner.imbalance = 1.1;
        
        this.graph = partitioner.partition(graph);
        this.graph.cache();
    }
    
    public NodePartitioner getPartitioner() {
        return partitioner.internal_partitioner;
    }
    
    public List<Node<T>> getMedoids() {
        return partitioner.medoids;
    }


    /**
     *
     * @param query
     * @param k
     * @param speedup
     * @return
     */
    public NeighborList search(
            final Node<T> query, 
            final int k, 
            final double speedup) {
        
        
        JavaRDD<NeighborList> candidates_neighborlists_graph = 
                graph.mapPartitions(
                        new FlatMapFunction<
                                Iterator<Tuple2<Node<T>, NeighborList>>, 
                                NeighborList>() {

            public Iterable<NeighborList> call(
                    Iterator<Tuple2<Node<T>, NeighborList>> tuples) 
                    throws Exception {

                // Read all tuples to rebuild the subgraph
                Graph<T> local_graph = new Graph<T>();
                while (tuples.hasNext()) {
                    Tuple2<Node<T>, NeighborList> next = tuples.next();
                    local_graph.put(next._1, next._2);
                }
                
                local_graph.setSimilarity(similarity);

                // Search the local graph
                NeighborList nl = local_graph.search(
                        query.value, 
                        k,
                        speedup);
                
                ArrayList<NeighborList> result = new ArrayList<NeighborList>(1);
                result.add(nl);
                return result;
            }
        });

        NeighborList final_neighborlist = new NeighborList(k);
        for (NeighborList nl : candidates_neighborlists_graph.collect()) {
            final_neighborlist.addAll(nl);
        }
        return final_neighborlist;
    }

    public JavaPairRDD<Node<T>, NeighborList> getGraph() {
        return this.graph;
    }

    public void setGraph(JavaPairRDD<Node<T>, NeighborList> graph) {
        this.graph = graph;
    }
}


