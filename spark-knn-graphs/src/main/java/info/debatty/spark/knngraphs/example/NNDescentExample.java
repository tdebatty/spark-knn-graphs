package info.debatty.spark.knngraphs.example;

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

    public static void main(String[] args) throws Exception {
        
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
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
                return 1.0 / (1.0 + Math.abs(value1 - value2));
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

                    public Double call(
                            Double val, 
                            Tuple2<Node, NeighborList> tuple) throws Exception {
                        for (Neighbor n : tuple._2()) {
                            val += n.similarity;
                        }

                        return val;
                    }
                },
                new Function2<Double, Double, Double>() {

                    public Double call(
                            Double val0, 
                            Double val1) throws Exception {
                        return val0 + val1;
                    }

                });

        System.out.println("Total sim: " + total_similarity);
        System.out.println(graph.first());
        
    }    
}
