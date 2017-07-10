package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author Thibault Debatty
 * @param <T> the type of element the actual graph builder works with...
 */
public abstract class DistributedGraphBuilder<T> implements Serializable {

    protected int k = 10;
    protected SimilarityInterface<T> similarity;

    /**
     * Set k (the number of edges per node).
     * Default value is 10
     * @param k
     */
    public void setK(int k) {
        if (k <= 0) {
            throw new InvalidParameterException("k must be positive!");
        }

        this.k = k;
    }

    /**
     * Define how similarity will be computed between node values.
     * NNDescent can use any similarity (even non metric).
     * @param similarity
     */
    public void setSimilarity(SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    /**
     * Compute and return the graph.
     * Children classes must implement _computeGraph(nodes) method
     *
     * @param nodes
     * @return the graph
     * @throws java.lang.Exception
     */
    public JavaPairRDD<Node<T>, NeighborList>
        computeGraph(JavaRDD<Node<T>> nodes) throws Exception {
        if (similarity == null) {
            throw new InvalidParameterException("Similarity is not defined!");
        }

        return doComputeGraph(nodes);
    }

    /**
     *
     * @param nodes
     * @return
     * @throws java.lang.Exception
     */
    protected abstract JavaPairRDD<Node<T>, NeighborList>
        doComputeGraph(JavaRDD<Node<T>> nodes) throws Exception;

    public static ArrayList<String> readFile(String path) throws IOException {

        File file = new File(path);
	BufferedReader br = new BufferedReader(new FileReader(file));

        ArrayList<String> r = new ArrayList<String>();
	String line;
	while ((line = br.readLine()) != null) {
		r.add(line);
	}

	br.close();
        return r;
    }
}
