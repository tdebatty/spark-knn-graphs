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

    public ApproximateSearch(JavaPairRDD<Node<T>, NeighborList> graph, int partitioning_iterations, int partitioning_medoids, SimilarityInterface<T> similarity) {

        // Partition the graph
        VoronoiPartitioner partitioner = new VoronoiPartitioner();
        partitioner.iterations = partitioning_iterations;
        partitioner.medoids_count = partitioning_medoids;
        partitioner.similarity = similarity;
        this.graph = partitioner.partition(graph);
        this.graph.cache();

        this.similarity = similarity;
    }

    public NeighborList search(final Node<T> query, final int k, final int gnss_restarts, final int gnss_depth, int[] computed_similarities) {
        JavaRDD<SearchResult> candidates_neighborlists_graph = graph.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Node<T>, NeighborList>>, SearchResult>() {

            public Iterable<SearchResult> call(Iterator<Tuple2<Node<T>, NeighborList>> tuples) throws Exception {

                // Read all tuples to rebuild the subgraph
                Graph<T> local_graph = new Graph<T>();
                while (tuples.hasNext()) {
                    Tuple2<Node<T>, NeighborList> next = tuples.next();
                    local_graph.put(next._1, next._2);
                }

                ArrayList<SearchResult> result = new ArrayList<SearchResult>(1);

                int[] computed_similarities = new int[1];
                NeighborList nl = local_graph.search(query, k, gnss_restarts, gnss_depth, similarity, computed_similarities);
                result.add(new SearchResult(nl, computed_similarities[0]));
                return result;
            }
        });

        NeighborList final_neighborlist = new NeighborList(k);
        for (SearchResult sr : candidates_neighborlists_graph.collect()) {
            final_neighborlist.addAll(sr.neighborlist);
            computed_similarities[0] += sr.computed_similarities;
        }

        return final_neighborlist;
    }

}

class SearchResult implements Serializable {

    public NeighborList neighborlist;
    public int computed_similarities;

    public SearchResult(NeighborList neighborList, int computed_similarities) {
        this.neighborlist = neighborList;
        this.computed_similarities = computed_similarities;
    }
}

class VoronoiPartitioner<T> implements Serializable {

    public SimilarityInterface similarity;
    public int iterations = 5;
    public int medoids_count;

    public JavaPairRDD<Node<T>, NeighborList> partition(JavaPairRDD<Node<T>, NeighborList> graph) {

        System.out.println("Picking some random initial medoids...");

        double fraction = 10.0 * medoids_count / graph.count();
        Iterator<Tuple2<Node<T>, NeighborList>> sample_iterator = graph.sample(false, fraction).collect().iterator();
        List<Node<T>> medoids = new ArrayList<Node<T>>();
        for (int i = 0; i < medoids_count; i++) {
            medoids.add(sample_iterator.next()._1);
        }

        System.out.println("Configuring internal Voronoi partitioner...");
        InternalVoronoiPartitioner part = new InternalVoronoiPartitioner();
        part.setSimilarity(similarity);

        for (int iteration = 0; iteration < iterations; iteration++) {
            System.out.printf("Iteration %d\n", iteration);

            part.setMedoids(medoids);
            graph = graph.partitionBy(part);
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

                    // This partition might contain multiple subgraphs
                    ArrayList<Graph<T>> stronglyConnectedComponents = partition.stronglyConnectedComponents();
                    //System.out.printf(
                    //        "This partition contains %d subgraphs\n",
                    //        stronglyConnectedComponents.size());

                    // Find the largest subgraph
                    int largest_subgraph_size = 0;
                    Graph<T> largest_subgraph = stronglyConnectedComponents.get(0);
                    for (Graph<T> subgraph : stronglyConnectedComponents) {
                        if (subgraph.size() > largest_subgraph_size) {
                            largest_subgraph = subgraph;
                            largest_subgraph_size = subgraph.size();
                        }
                    }

                    //System.out.printf(
                    //        "Largest subgraph in partition contains %d nodes\n",
                    //        largest_subgraph.size());
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

                    //System.out.printf(
                    //        "New medoid is %s (radius %d)\n",
                    //        medoid,
                    //        largest_distance);
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
    public int getPartition(Object o) {
        Node n = (Node) o;

        double highest_similarity = 0;
        int most_similar = 0;

        for (int i = 0; i < medoids.size(); i++) {
            Node medoid = medoids.get(i);
            double sim = similarity.similarity(n.value, medoid.value);
            if (sim > highest_similarity) {
                highest_similarity = sim;
                most_similar = i;
            }
        }

        return most_similar;
    }

}
