/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
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

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.BalancedKMedoidsPartitioner;
import info.debatty.spark.knngraphs.NodePartitioner;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class Online<T> {
    private final ApproximateSearch<T> searcher;
    private final int k;
    private final JavaSparkContext sc;
    private final SimilarityInterface<T> similarity;
    
    
    public Online(
            int k,
            SimilarityInterface<T> similarity,
            JavaSparkContext sc,
            JavaPairRDD<Node<T>, NeighborList> initial) {
        
        this.similarity = similarity;
        this.k = k;
        this.sc = sc;
        searcher = new ApproximateSearch<T>(
                initial,
                5, 
                4,
                similarity);
    }
    
    
    public void addNode(Node<T> node) {
        NeighborList neighborlist = searcher.search(
                node,
                k,
                2000);
        
        // Parallelize the pair node => neighborlist
        LinkedList<Tuple2<Node<T>, NeighborList>> list = 
                new LinkedList<Tuple2<Node<T>, NeighborList>>();
        list.add(new Tuple2<Node<T>, NeighborList>(node, neighborlist));
        JavaPairRDD<Node<T>, NeighborList> new_graph_piece = sc.parallelizePairs(list);
        
        // Partition the pair
        JavaPairRDD<Node<T>, NeighborList> partitioned_piece = partition(
                new_graph_piece,
                searcher.getMedoids(),
                getCounts(),
                searcher.getPartitioner());
        
        // The new graph is the union of Java RDD's
        JavaPairRDD union = searcher.getGraph().union(partitioned_piece);
        
        
        // From now on, use the new graph...
        searcher.setGraph(union);
    }
    
    Long[] getCounts() {
        List<Long> counts = searcher.getGraph().mapPartitions(new PartitionCountFunction(), true).collect();
        
        return counts.toArray(new Long[counts.size()]);
    }

    JavaPairRDD<Node<T>, NeighborList> getGraph() {
        return searcher.getGraph();
    }
    
        
    /**
     * Perform a single iteration of partitioning, without recomputing new medoids
     * @param piece
     * @param medoids
     * @param counts
     * @param internal_partitioner
     * @return 
     */
    public JavaPairRDD<Node<T>, NeighborList> partition(
            JavaPairRDD<Node<T>, NeighborList> piece,
            List<Node<T>> medoids,
            Long[] counts,
            NodePartitioner internal_partitioner) {
        
        // Assign each node to a partition id
        JavaPairRDD<Node<T>, NeighborList> partitioned_graph = 
                piece.mapPartitionsToPair(
                        new AssignFunction<T>(medoids, counts, similarity),
                        true);

        // Partition
        partitioned_graph = partitioned_graph.partitionBy(internal_partitioner);
        
        return partitioned_graph;
    }

    private static class PartitionCountFunction<U> 
        implements FlatMapFunction<Iterator<Tuple2<Node<U>, NeighborList>>, Long> {
            
        public Iterable<Long> call(Iterator<Tuple2<Node<U>, NeighborList>> iterator) throws Exception {
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            ArrayList<Long> result = new ArrayList<Long>(1);
            result.add(count);
            return result;
        }
    }
}
    


class AssignFunction<U>
    implements PairFlatMapFunction<Iterator<Tuple2<Node<U>, NeighborList>>, Node<U>, NeighborList> {
    private final List<Node<U>> medoids;
    private final Long[] counts;
    public final SimilarityInterface<U> similarity;

    public AssignFunction(List<Node<U>> medoids, Long[] counts, SimilarityInterface<U> similarity) {
        this.medoids = medoids;
        this.counts = counts;
        this.similarity = similarity;
    }

    public Iterable<Tuple2<Node<U>, NeighborList>> 
        call(Iterator<Tuple2<Node<U>, NeighborList>> iterator) 
                throws Exception {

        // Total number of elements
        long n = sum(counts) + 1;
        double imbalance = 1.1;
        int partitions = medoids.size();
        int partition_constraint = (int) (imbalance * n / partitions);

        // fetch all tuples in this partition 
        // to compute the partition_constraint
        ArrayList<Tuple2<Node<U>, NeighborList>> tuples = 
                new ArrayList<Tuple2<Node<U>, NeighborList>>();

        while (iterator.hasNext()) {
            Tuple2<Node<U>, NeighborList> tuple = iterator.next();
            tuples.add(tuple);

            double[] similarities = new double[partitions];
            double[] values = new double[partitions];

            // 1. similarities
            for (int center_id = 0; center_id < partitions; center_id++) {
                similarities[center_id] = similarity.similarity(
                        medoids.get(center_id).value,
                        tuple._1.value);
            }

            // 2. value to maximize = similarity * (1 - cluster_size / capacity_constraint)
            for (int center_id = 0; center_id < partitions; center_id++) {
                values[center_id] = similarities[center_id] *
                        (1 - counts[center_id] / partition_constraint);
            }

            // 3. choose partition that minimizes compute value
            int partition = argmax(values);
            counts[partition]++;
            tuple._1.setAttribute(BalancedKMedoidsPartitioner.PARTITION_KEY, partition);
        }

        return tuples;
    }
        
    private static long sum(Long[] values) {
            long agg = 0;
            for (long value : values) {
                agg += value;
            }
            return agg;
        }
        
    private static int argmax(double[] values) {
        double max_value = -1.0 * Double.MAX_VALUE;
        ArrayList<Integer> ties = new ArrayList<Integer>();
        
        for (int i = 0; i < values.length; i++) {
            if (values[i] > max_value) {
                max_value = values[i];
                ties = new ArrayList<Integer>();
                ties.add(i);
                
            } else if(values[i] == max_value) {
                // add a tie
                ties.add(i);
            }
        }
        
        if (ties.size() == 1) {
            return ties.get(0);
        }
        
        Random rand = new Random();
        return ties.get(rand.nextInt(ties.size()));
    }
}