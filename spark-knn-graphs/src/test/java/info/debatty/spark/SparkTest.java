/*
 * The MIT License
 *
 * Copyright 2015 Thibault Debatty.
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

package info.debatty.spark;

import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 *
 * @author Thibault Debatty
 */
public class SparkTest extends TestCase implements Serializable {

    /**
     * Test of computeGraph method, of class NNDescent.
     * @throws java.io.IOException
     */
    public final void testSpark() throws IOException, Exception {
        System.out.println("Spark test");
        System.out.println("==========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<String> data = sc.parallelize(strings);

        JavaRDD<String> mapped =
                data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(final String arg0) throws Exception {
                return new LinkedList<String>().iterator();
            }

        });

        assertEquals(0, mapped.count());

        sc.close();
    }

}
