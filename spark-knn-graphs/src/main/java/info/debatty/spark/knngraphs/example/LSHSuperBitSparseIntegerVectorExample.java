package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.NeighborList;

import info.debatty.java.utils.SparseIntegerVector;
import info.debatty.spark.knngraphs.Node;
import info.debatty.spark.knngraphs.builder.LSHSuperBitSparseIntegerVector;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * An example of how to use LSHSuperBit algorithm to build a k-nn graph from
 * a dataset of SparseIntegerVector.
 *
 * @author Thibault Debatty
 */
public class LSHSuperBitSparseIntegerVectorExample implements Runnable {

    /**
     * @param args the command line arguments
     */
    public static void main(final String[] args) {
        LSHSuperBitSparseIntegerVectorExample instance =
                new LSHSuperBitSparseIntegerVectorExample();

        instance.run();

    }

    @Override
    public final void run() {

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create some nodes consisting of SparseIntegerVector
        int d = 1000; // dimensions
        int n = 1000; // items
        Random r = new Random();
        List<SparseIntegerVector> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int[] vector = new int[d];
            for (int j = 0; j < d / 2; j++) {
                vector[r.nextInt(d)] = r.nextInt(100);
            }

            data.add(new SparseIntegerVector(vector));
        }
        JavaRDD<SparseIntegerVector> nodes = sc.parallelize(data);


        // Configure LSHSuperBit graph builder
        LSHSuperBitSparseIntegerVector gbuilder =
                new LSHSuperBitSparseIntegerVector();
        gbuilder.setK(10);
        gbuilder.setStages(2);
        gbuilder.setBuckets(10);
        // LSH hashing requires the dimensionality
        gbuilder.setDim(d);

        // By default, LSHSuperBit graph builder uses cosine similarity
        // but another similarity measure can be defined if needed...

        // Build the graph...
        try {
            JavaPairRDD<Node<SparseIntegerVector>, NeighborList> graph =
                    gbuilder.computeGraph(nodes);
            System.out.println(graph.first());

        } catch (Exception ex) {
            System.err.println("Something went wrong...");

        } finally {
            sc.close();
        }
    }
}
