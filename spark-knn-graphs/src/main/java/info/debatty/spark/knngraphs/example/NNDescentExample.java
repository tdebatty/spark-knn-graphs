package info.debatty.spark.knngraphs.example;

import info.debatty.spark.knngraphs.builder.NNDescent;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class NNDescentExample implements Runnable, Serializable {

    public static void main(String[] args) throws Exception {
        NNDescentExample instance = new NNDescentExample();
        instance.run();
    }

    @Override
    public final void run() {
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("NNDescentExample");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create some nodes
        // the value of the nodes will simply be an integer:
        List<Node<Integer>> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            data.add(new Node(String.valueOf(i), i));
        }
        JavaRDD<Node<Integer>> nodes = sc.parallelize(data);

        // Instanciate and configure NNDescent for Integer node values
        NNDescent<Integer> nndes = new NNDescent<>();
        nndes.setK(10);
        nndes.setMaxIterations(10);
        nndes.setSimilarity(new SimilarityInterface<Integer>() {

            // Define the similarity that will be used
            // in this case: 1 / (1 + delta)
            @Override
            public double similarity(
                    final Integer value1, final Integer value2) {

                // The value of nodes is an integer...
                return 1.0 / (1.0 + Math.abs(value1 - value2));
            }
        });

        // Compute the graph...
        JavaPairRDD<Node<Integer>, NeighborList> graph;
        try {
            graph = nndes.computeGraph(nodes);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            sc.close();
            return;
        }

        // BTW: until now graph is only an execution plan and nothing has been
        // executed by the spark cluster...
        // This will actually build the graph to compute the total similarity
        double total_similarity = graph.aggregate(
                0.0,
                new  Function2<
                        Double, Tuple2<Node<Integer>, NeighborList>, Double>() {
                    @Override
                    public Double call(
                            final Double val,
                            final Tuple2<Node<Integer>, NeighborList> tuple) {

                        double acc = val;
                        for (Neighbor n : tuple._2()) {
                            acc += n.similarity;
                        }
                        return acc;
                    }
                },
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(
                            final Double val0,
                            final Double val1) {
                        return val0 + val1;
                    }

                });

        System.out.println("Total sim: " + total_similarity);
        System.out.println(graph.first());
        sc.close();
    }
}
