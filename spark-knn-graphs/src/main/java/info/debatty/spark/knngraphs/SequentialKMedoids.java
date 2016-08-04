package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.SimilarityInterface;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author Thibault Debatty
 * @param <T> The type of data that will be clustered
 */
public class SequentialKMedoids<T> {
    private static final int DEFAULT_K = 10;
    private static final double DEFAULT_IMBALANCE = 1.1;
    private static final int DEFAULT_MAX_ITERATIONS = 20;

    /**
     * Pick k initial medoids from the dataset.
     * @param <U>
     * @param dataset
     * @param k
     * @return
     */
    public static <U> LinkedList<U> pickInitialMedoids(
            final List<U> dataset,
            final int k) {
        LinkedList<U> initial_medoids = new LinkedList<U>();
        Random rand = new Random();
        while (initial_medoids.size() < k) {

            // Pick a random medoid
            U new_medoid = dataset.get(rand.nextInt(dataset.size() - 1));

            // Check if we already picked this one
            if (!initial_medoids.contains(new_medoid)) {
                initial_medoids.add(new_medoid);
            }
        }
        return initial_medoids;
    }

    private SimilarityInterface<T> similarity;
    private int k = DEFAULT_K;
    private double imbalance = DEFAULT_IMBALANCE;
    private int max_iterations = DEFAULT_MAX_ITERATIONS;
    private List<T> data;

    private LinkedList<T> medoids;
    private int similarities;
    private SumAggregator total_similarity;
    private double previous_total_pairwize_similarity;
    private int iteration;
    private ArrayList<T>[] clusters;

    /**
     * Get the clusters computed by the algorithm.
     * @return
     */
    public final ArrayList<T>[] getClusters() {
        return clusters;
    }

    /**
     * Return the number of executed iterations.
     * @return
     */
    public final int getIterations() {
        return iteration;
    }

    /**
     * Return the total intra-cluster similarity.
     * @return
     */
    public final double getTotalSimilarity() {
        return total_similarity.value();
    }

    /**
     * Get the total number of computed similarities.
     * @return
     */
    public final int getSimilarities() {
        return similarities;
    }

    /**
     * Return the computed medoids.
     * @return
     */
    public final LinkedList<T> getMedoids() {
        return medoids;
    }

    /**
     * Set initial medoids.
     * (if you want to compute them with another algorithm)
     * @param medoids
     */
    public final void setMedoids(final LinkedList<T> medoids) {
        this.medoids = medoids;
    }

    /**
     * Run the clustering algorithm and compute medoids.
     */
    public final void run() {
        similarities = 0;
        int n = data.size();
        int capacity = (int) (n * imbalance / k);

        if (medoids == null) {
            medoids = pickInitialMedoids(data, k);
        }

        for (iteration = 1; iteration <= max_iterations; iteration++) {
            total_similarity = new SumAggregator();

            // Create empty clusters
            clusters = new ArrayList[k];
            for (int i = 0; i < k; i++) {
                clusters[i] = new ArrayList<T>();
            }

            // Assign
            for (int i = 0; i < n; i++) {
                T item = data.get(i);

                double[] sims = new double[k];
                double[] values = new double[k];
                for (int j = 0; j < k; j++) {
                    similarities++;
                    sims[j] = similarity.similarity(item, medoids.get(j));
                    values[j] = sims[j];

                    // Capacity == 0.0 indicate we should compute classical
                    // (unconstrained) k-medoids
                    if (capacity == 0.0) {
                        continue;
                    }

                    values[j] *= (1.0 - (double) clusters[j].size() / capacity);
                }

                int cluster_index = argmax(values);
                clusters[cluster_index].add(item);
                total_similarity.add(sims[cluster_index]);
            }

            // Continue if the new medoids improved total similarity
            if (total_similarity.value()
                    <= previous_total_pairwize_similarity) {
                break;
            }
            this.previous_total_pairwize_similarity =
                    total_similarity.value();

            // Compute new medoids
            medoids = new LinkedList<T>();
            for (int i = 0; i < k; i++) {
                medoids.add(findMedoid(clusters[i]));
            }
        }
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

    /**
     * Set the algorithm to use to compute similarity between items.
     * @param similarity
     */
    public final void setSimilarity(
            final SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    /**
     * Set the number of clusters.
     * @param k
     */
    public final void setK(final int k) {
        this.k = k;
    }

    /**
     * Set the allowed imbalance between clusters.
     * Should be >= 1.0, or 0 for classical (unbalanced) k-medoids.
     * Default: 1.1
     * @param imbalance
     */
    public final void setImbalance(final double imbalance) {
        if (imbalance == 0.0
                || imbalance >= 1.0) {
            this.imbalance = imbalance;

        } else {
            throw new InvalidParameterException(
                    "Imbalance should be 0.0 (classical k-medoids) or >= 1.0");
        }

    }

    /**
     * Set the maximum number of iterations.
     * @param iterations
     */
    public final void setMaxIterations(final int iterations) {
        this.max_iterations = iterations;
    }

    /**
     * Set the data to cluster.
     * @param data
     */
    public final void setData(final List<T> data) {
        this.data = data;
    }

    /**
     * Find the point that maximizes the sum of similarity to all other points
     * in the cluster.
     * @param arrayList
     * @return
     */
    private T findMedoid(final ArrayList<T> cluster) {
        T new_medoid = null;
        double new_medoid_total_similarity = -1.0;

        for (T point : cluster) {
            // Compute the total similarity of this point
            SumAggregator point_total_similarity = new SumAggregator();
            for (T other_point : cluster) {
                point_total_similarity.add(
                        similarity.similarity(point, other_point));
                similarities++;
            }

            // This is currently the most central point
            if (point_total_similarity.value() > new_medoid_total_similarity) {
                new_medoid = point;
                new_medoid_total_similarity = point_total_similarity.value();
            }
        }
        return new_medoid;
    }
}
