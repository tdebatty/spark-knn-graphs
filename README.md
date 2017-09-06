# spark-knn-graphs
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/info.debatty/spark-knn-graphs/badge.svg)](https://maven-badges.herokuapp.com/maven-central/info.debatty/spark-knn-graphs) [![Build Status](https://travis-ci.org/tdebatty/spark-knn-graphs.svg?branch=master)](https://travis-ci.org/tdebatty/spark-knn-graphs) [![Javadocs](http://www.javadoc.io/badge/info.debatty/spark-knn-graphs.svg)](http://www.javadoc.io/doc/info.debatty/spark-knn-graphs)

Spark algorithms for building and processing k-nn graphs.

Currently implemented k-nn graph building algorithms:
* Brute force
* NN-Descent (which supports any similarity)
* LSH SuperBit (for cosine similarity)
* NNCTPH (for text datasets)
* Fast online graph building

Implemented k-nn graph processing algorithms:
* Distributed exhaustive nearest neighbor search
* Distributed graph based nearest neighbor search


All algorithms support custom classes as value. See [an example with custom class as value](https://github.com/tdebatty/spark-knn-graphs/blob/master/spark-knn-graphs/src/main/java/info/debatty/spark/knngraphs/example/NNDescentCustomValue.java).

## Installation and requirements

spark-knn-graphs requires **Spark 1.4.0** or above. It is currently tested with Spark versions **1.4.1**, **1.5.2**, **1.6.0** and **1.6.2**.

Installation using Maven:
```
<dependency>
    <groupId>info.debatty</groupId>
    <artifactId>spark-knn-graphs</artifactId>
    <version>RELEASE</version>
</dependency>
```

Or check [Spark Packages](http://spark-packages.org/package/tdebatty/spark-knn-graphs)

## Examples
Here are only a few short examples. Check [the examples folder](https://github.com/tdebatty/spark-knn-graphs/tree/master/spark-knn-graphs/src/main/java/info/debatty/spark/knngraphs/example) for more examples and complete code.

### NN-Descent
```java
public class NNDescentExample {

    public static void main(String[] args) {

        // Configure spark instance
        SparkConf conf = new SparkConf();
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
public class LSHSuperBitExample {

    public static void main(String[] args) {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
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
