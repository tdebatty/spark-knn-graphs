package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.LSHSuperBitDoubleArray;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Thibault Debatty
 */
public class LSHSuperBitExample {

    public static void main(String[] args) throws Exception {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        // Create some nodes consisting of double[]
        int d = 100; // dimensions
        int n = 1000; // items
        Random r = new Random();
        List<Node<double[]>> data = new ArrayList<Node<double[]>>();
        for (int i = 0; i < n; i++) {
            double[] vector = new double[d];
            for (int j = 0; j < d; j++) {
                vector[j] = r.nextDouble() * 100;
            }
            
            data.add(new Node(String.valueOf(i), vector));
        }
        JavaRDD<Node<double[]>> nodes = sc.parallelize(data);
        
        
        // Configure LSHSuperBit graph builder
        LSHSuperBitDoubleArray gbuilder = new LSHSuperBitDoubleArray();
        gbuilder.setK(10);
        gbuilder.setStages(2);
        gbuilder.setBuckets(10);
        // LSH hashing requires the dimensionality
        gbuilder.setDim(d);
        
        
        // Build the graph...
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                gbuilder.computeGraph(nodes);
        
        System.out.println(graph.first());
        
    }
}
