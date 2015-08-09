# spark-knn-graphs
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/info.debatty/spark-knn-graphs/badge.svg)](https://maven-badges.herokuapp.com/maven-central/info.debatty/spark-knn-graphs)

Spark algorithms for building k-nn graphs

## Installation
```
<dependency>
    <groupId>info.debatty</groupId>
    <artifactId>spark-knn-graphs</artifactId>
    <version>RELEASE</version>
</dependency>
```

Or on spark-packages: http://spark-packages.org/package/tdebatty/spark-knn-graphs

## NN-Descent

```java
import info.debatty.spark.knngraphs.builder.NNDescent;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class NNDescentExample {

    public static void main(String[] args) {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Create some nodes
        // the value of the nodes will simply be an integer:
        List<Node<Integer>> data = new ArrayList<Node<Integer>>();
        for (int i = 0; i < 1000; i++) {
            data.add(new Node(String.valueOf(i), i));
        }
        JavaRDD<Node<Integer>> nodes = sc.parallelize(data);
        
        // Instanciate and configure NNDescent for Integer node values
        NNDescent nndes = new NNDescent<Integer>();
        nndes.setK(10);
        nndes.setMaxIterations(10);
        nndes.setSimilarity(new SimilarityInterface<Integer>() {

                    // Define the similarity that will be used
                    // in this case: 1 / (1 + delta)
                    public double similarity(Integer value1, Integer value2) {
                        
                        // The value of nodes is an integer...
                        return 1.0 / (1.0 + Math.abs((Integer) value1 - (Integer) value2));
                    }
        });
        
        // Compute the graph...
        JavaPairRDD<Node, NeighborList> graph = nndes.computeGraph(nodes);
        
        // BTW: until now graph is only an execution plan and nothing has been
        // executed by the spark cluster...
        
        // This will actually compute the graph...
        double total_similarity = graph.aggregate(
                0.0,
                new  Function2<Double,Tuple2<Node,NeighborList>,Double>() {

                    public Double call(Double val, Tuple2<Node, NeighborList> tuple) throws Exception {
                        for (Neighbor n : tuple._2()) {
                            val += n.similarity;
                        }
                        
                        return val;
                    }
                },
                new Function2<Double, Double, Double>() {

                    public Double call(Double val0, Double val1) throws Exception {
                        return val0 + val1;
                    }
                    
                });
        
        System.out.println("Total sim: " + total_similarity);
        System.out.println(graph.first());
        
    }    
}
```

## LSH SuperBit

```java
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

    public static void main(String[] args) {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setMaster("local");
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
        JavaPairRDD<Node<double[]>, NeighborList> graph = gbuilder.computeGraph(nodes);
        System.out.println(graph.first());
        
    }
}
```

```java
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.utils.SparseIntegerVector;
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
 * a dataset of SparseIntegerVector
 * 
 * @author Thibault Debatty
 */
public class LSHSuperBitSparseIntegerVectorExample {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        // Create some nodes consisting of SparseIntegerVector
        int d = 1000; // dimensions
        int n = 1000; // items
        Random r = new Random();
        List<Node<SparseIntegerVector>> data = new ArrayList<Node<SparseIntegerVector>>();
        for (int i = 0; i < n; i++) {
            int[] vector = new int[d];
            for (int j = 0; j < d/2; j++) {
                vector[r.nextInt(d)] = r.nextInt(100);
            }
            
            data.add(new Node(String.valueOf(i), new SparseIntegerVector(vector)));
        }
        JavaRDD<Node<SparseIntegerVector>> nodes = sc.parallelize(data);
        
        
        // Configure LSHSuperBit graph builder
        LSHSuperBitSparseIntegerVector gbuilder = new LSHSuperBitSparseIntegerVector();
        gbuilder.setK(10);
        gbuilder.setStages(2);
        gbuilder.setBuckets(10);
        // LSH hashing requires the dimensionality
        gbuilder.setDim(d);
        
        
        // By default, LSHSuperBit graph builder uses cosine similarity
        // but another similarity measure can be defined if needed...
        
        // Build the graph...
        JavaPairRDD<Node<SparseIntegerVector>, NeighborList> graph = gbuilder.computeGraph(nodes);
        System.out.println(graph.first());
    }
}
```
