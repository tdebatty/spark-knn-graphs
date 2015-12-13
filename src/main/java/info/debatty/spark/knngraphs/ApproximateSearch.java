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

import info.debatty.java.graphs.Dijkstra;
import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.Partitioner;
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

    private final JavaPairRDD graph;
    private final SimilarityInterface<T> similarity;
    private final int partitioning_medoids;

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

        
        // Partition the graph
        VoronoiPartitioner partitioner = new VoronoiPartitioner();
        partitioner.iterations = partitioning_iterations;
        partitioner.medoids_count = partitioning_medoids;
        partitioner.similarity = similarity;
        this.graph = partitioner.partition(graph);
        this.graph.cache();

        this.similarity = similarity;
        this.partitioning_medoids = partitioning_medoids;
    }
    
    /**
     *
     * @param query
     * @param k
     * @param max_similarities
     * @return
     */
    public NeighborList search(
            final Node<T> query, 
            final int k, 
            final int max_similarities) {
        
        
        return search(
                query, 
                k,
                max_similarities,
                100, 
                1.01);
    }

    /**
     *
     * @param query
     * @param k
     * @param max_similarities
     * @param gnss_depth
     * @param gnss_expansion
     * @return
     */
    public NeighborList search(
            final Node<T> query, 
            final int k, 
            final int max_similarities, 
            final int gnss_depth,
            final double gnss_expansion) {
        
        final int max_similarities_per_partition = 
                max_similarities / partitioning_medoids;
        
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

                ArrayList<NeighborList> result = new ArrayList<NeighborList>(1);

                NeighborList nl = local_graph.search(
                        query, 
                        k, 
                        similarity, 
                        max_similarities_per_partition,
                        gnss_depth,
                        gnss_expansion);
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

}

class VoronoiPartitioner<T> implements Serializable {

    public SimilarityInterface similarity;
    public int iterations = 5;
    public int medoids_count;

    public JavaPairRDD<Node<T>, NeighborList> partition(JavaPairRDD<Node<T>, NeighborList> graph) {

        // Pick some random initial medoids
        double fraction = 10.0 * medoids_count / graph.count();
        Iterator<Tuple2<Node<T>, NeighborList>> sample_iterator = graph.sample(false, fraction).collect().iterator();
        List<Node<T>> medoids = new ArrayList<Node<T>>();
        for (int i = 0; i < medoids_count; i++) {
            medoids.add(sample_iterator.next()._1);
        }

        InternalVoronoiPartitioner internal_partitioner =  new InternalVoronoiPartitioner();
        internal_partitioner.setSimilarity(similarity);

        for (int iteration = 0; iteration < iterations; iteration++) {

            internal_partitioner.setMedoids(medoids);
            graph = graph.partitionBy(internal_partitioner);
            graph.cache();

            JavaRDD<Node<T>> new_medoids;
            new_medoids = graph.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Node<T>, NeighborList>>, Node<T>>() {

                public Iterable<Node<T>> call(Iterator<Tuple2<Node<T>, NeighborList>> t) throws Exception {
                    // Build the partition
                    Graph partition = new Graph();
                    while (t.hasNext()) {
                        Tuple2<Node<T>, NeighborList> tuple = t.next();
                        partition.put(tuple._1(), tuple._2());
                    }

                    if (partition.size() == 0) {
                        return new ArrayList<Node<T>>();
                    }

                    // This partition might contain multiple subgraphs => find largest subgraph
                    ArrayList<Graph<T>> stronglyConnectedComponents = partition.stronglyConnectedComponents();
                    int largest_subgraph_size = 0;
                    Graph<T> largest_subgraph = stronglyConnectedComponents.get(0);
                    for (Graph<T> subgraph : stronglyConnectedComponents) {
                        if (subgraph.size() > largest_subgraph_size) {
                            largest_subgraph = subgraph;
                            largest_subgraph_size = subgraph.size();
                        }
                    }

                    int largest_distance = Integer.MAX_VALUE;
                    Node medoid = (Node) largest_subgraph.keySet().iterator().next();
                    for (Node n : largest_subgraph.keySet()) {
                        //Node n = (Node) o;
                        Dijkstra dijkstra = new Dijkstra(largest_subgraph, n);

                        int node_largest_distance = dijkstra.getLargestDistance();

                        if (node_largest_distance == 0) {
                            continue;
                        }

                        if (node_largest_distance < largest_distance) {
                            largest_distance = node_largest_distance;
                            medoid = n;
                        }
                    }
                    ArrayList<Node<T>> list = new ArrayList<Node<T>>(1);
                    list.add(medoid);
                    
                    return list;
                }
            });
            medoids = new_medoids.collect();
        }

        return graph;
    }
}

class InternalVoronoiPartitioner<T> extends Partitioner {

    protected List<Node<T>> medoids;
    protected SimilarityInterface similarity;

    public void setMedoids(List<Node<T>> medoids) {
        this.medoids = medoids;
    }

    public void setSimilarity(SimilarityInterface similarity) {
        this.similarity = similarity;
    }

    @Override
    public int numPartitions() {
        return medoids.size();
    }

    @Override
    public int getPartition(Object object) {
        Node node = (Node) object;

        double highest_similarity = 0;
        int most_similar = 0;

        for (int i = 0; i < medoids.size(); i++) {
            Node medoid = medoids.get(i);
            
            if (medoid.equals(node)) {
                return i;
            }
            
            double sim = similarity.similarity(node.value, medoid.value);
            if (sim > highest_similarity) {
                highest_similarity = sim;
                most_similar = i;
            }
        }

        return most_similar;
    }

}
