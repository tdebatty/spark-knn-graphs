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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.NeighborList;

import info.debatty.spark.SparkCase;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * A base class for spark test cases. Contains no test...
 * @author Thibault Debatty
 */
public class KNNGraphCase extends SparkCase {

    /**
     * Read the SPAM dataset from resources and parallelize in Spark.
     * @return
     * @throws IOException if we cannot read the file
     */
    public final JavaRDD<String> readSpam() throws IOException {
        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Parallelize the dataset in Spark
        JavaRDD<String> data = getSpark().parallelize(strings);
        return data;
    }

    /**
     * Read the exact SPAM graph from resources.
     * @return
     */
    public final JavaPairRDD<String, NeighborList> readSpamGraph() {
        String file =  getClass().getClassLoader().
                getResource("graph-spam").getPath();

        JavaRDD<Tuple2<String, NeighborList>> tuples =
                getSpark().objectFile(file, 8);
        JavaPairRDD<String, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        return graph;
    }

    /**
     * Read the exact synthetic graph from resources.
     * @return
     */
    public final JavaPairRDD<double[], NeighborList>
        readSyntheticGraph() {

        String file =  getClass().getClassLoader().
                getResource("graph-synthetic-10K").getPath();

        JavaRDD<Tuple2<double[], NeighborList>> tuples =
                getSpark().objectFile(file, 8);
        JavaPairRDD<double[], NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        return graph;
    }
}
