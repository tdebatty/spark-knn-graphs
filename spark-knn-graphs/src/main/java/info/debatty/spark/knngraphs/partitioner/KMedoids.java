/*
 * The MIT License
 *
 * Copyright 2017 Thibault Debatty.
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
package info.debatty.spark.knngraphs.partitioner;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.kmedoids.Budget;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import info.debatty.spark.kmedoids.Clusterer;
import info.debatty.spark.kmedoids.Similarity;
import info.debatty.spark.kmedoids.Solution;
import info.debatty.spark.kmedoids.budget.TrialsBudget;
import info.debatty.spark.kmedoids.neighborgenerator.WindowNeighborGenerator;
import info.debatty.spark.knngraphs.DistributedGraph;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class KMedoids<T> implements Partitioner<T> {

    private static final double DEFAULT_IMBALANCE = Double.POSITIVE_INFINITY;
    private static final int DEFAULT_TRIALS = 1000;

    private final SimilarityInterface<T> similarity;
    private final int partitions;
    private final double imbalance;
    private final Budget budget;

    /**
     *
     * @param similarity
     * @param partitions
     */
    public KMedoids(
            final SimilarityInterface<T> similarity, final int partitions) {
        this(similarity, partitions, DEFAULT_IMBALANCE);
    }

    /**
     * Use default budget (1000 trials).
     * @param similarity
     * @param partitions
     * @param imbalance
     */
    public KMedoids(
            final SimilarityInterface<T> similarity,
            final int partitions,
            final double imbalance) {

        this.similarity = similarity;
        this.partitions = partitions;
        this.imbalance = imbalance;
        this.budget = new TrialsBudget(DEFAULT_TRIALS);
    }

    /**
     *
     * @param similarity
     * @param partitions
     * @param imbalance
     * @param budget
     */
    public KMedoids(
            final SimilarityInterface<T> similarity,
            final int partitions,
            final double imbalance,
            final Budget budget) {

        this.similarity = similarity;
        this.partitions = partitions;
        this.imbalance = imbalance;
        this.budget = budget;
    }

    /**
     *
     * @param graph
     * @return
     */
    public final KMedoidsPartitioning<T> partition(
            final JavaPairRDD<Node<T>, NeighborList> graph) {

        KMedoidsPartitioning<T> solution = new KMedoidsPartitioning<T>();

        Clusterer<Node<T>> clusterer = new Clusterer<Node<T>>();
        clusterer.setK(partitions);
        clusterer.setSimilarity(new ClusteringSimilarityAdapter<T>(similarity));
        clusterer.setNeighborGenerator(new WindowNeighborGenerator<Node<T>>());
        clusterer.setImbalance(imbalance);
        clusterer.setBudget(budget);
        Solution<Node<T>> medoids = clusterer.cluster(graph.keys());

        // Assign each node to the most similar medoid
        // Taking imbalance into account
        solution.medoids = medoids.getMedoids();
        solution.graph =
                graph.mapPartitionsToPair(new AssignToMedoidFunction<T>(
                        medoids.getMedoids(),
                        similarity,
                        imbalance));
        solution.graph = DistributedGraph.moveNodes(solution.graph, partitions);
        solution.graph.cache();
        solution.graph.count();
        solution.end_time = System.currentTimeMillis();
        return solution;
    }

}

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
class AssignToMedoidFunction<T>
        implements PairFlatMapFunction<
            Iterator<Tuple2<Node<T>, NeighborList>>,
            Node<T>,
            NeighborList> {

    private final ArrayList<Node<T>> medoids;
    private final SimilarityInterface<T> similarity;
    private final double imbalance;

    AssignToMedoidFunction(
            final ArrayList<Node<T>> medoids,
            final SimilarityInterface<T> similarity,
            final double imbalance) {

        this.medoids = medoids;
        this.similarity = similarity;
        this.imbalance = imbalance;
    }

    public Iterator<Tuple2<Node<T>, NeighborList>> call(
            final Iterator<Tuple2<Node<T>, NeighborList>> iterator) {

        int k = medoids.size();

        // Collect all tuples
        LinkedList<Tuple2<Node<T>, NeighborList>> tuples =
                new LinkedList<Tuple2<Node<T>, NeighborList>>();
        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }

        int n_local = tuples.size();
        double capacity = imbalance * n_local / k;
        int[] cluster_sizes = new int[k];

        for (Tuple2<Node<T>, NeighborList> tuple : tuples) {
            double[] sims = new double[k];
            double[] values = new double[k];

            for (int i = 0; i < k; i++) {
                sims[i] = similarity.similarity(
                        tuple._1.value, medoids.get(i).value);
                values[i] =
                        sims[i] * (1.0 - (double) cluster_sizes[i] / capacity);
            }

            int cluster_index = argmax(values);
            cluster_sizes[cluster_index]++;
            tuple._1.setAttribute(NodePartitioner.PARTITION_KEY, cluster_index);
        }

        return tuples.iterator();
    }

    /**
     * Return the index of the highest value in the array.
     * @param values
     */
    private static int argmax(final double[] values) {
        double max = -Double.MAX_VALUE;
        int max_index = -1;
        for (int i = 0; i < values.length; i++) {
            if (values[i] > max) {
                max = values[i];
                max_index = i;
            }
        }

        return max_index;
    }
}

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
class ClusteringSimilarityAdapter<T> implements Similarity<Node<T>> {

    private final SimilarityInterface<T> similarity;

    ClusteringSimilarityAdapter(final SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    public double similarity(final Node<T> obj1, final Node<T> obj2) {
        return similarity.similarity(obj1.value, obj2.value);
    }
}


