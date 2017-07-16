/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
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

package info.devatty.spark.knngraphs.eval;

import info.debatty.java.datasets.textfile.Dataset;

/**
 *
 * @author Thibault Debatty
 */
public class MultiSpam {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Dataset dataset = new Dataset(args[0]);

        MultiTest<String> multi_test = new MultiTest<String>();
        multi_test.k = 10;
        multi_test.n = 40000;
        multi_test.n_add = 1000;
        multi_test.dataset_iterator = dataset.iterator();
        multi_test.setSimilarity(new JWSimilarity());

        // Vary expansion
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},// search speedup
                        new double[]{3.5, 4.0, 4.5, 5.0},                  // search expansion
                        "test-spam-expansion.csv"));

        /*
        multi_test.batches.add(
                new Batch(
                        new int[]{2, 4},         // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},                  // search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-partitions-2.csv"));

        /*
        // Test with sequential k-medoids
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{10},                       // max-iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},// search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-sequential-kmedoids.csv"));

        /*
        // Vary expansion
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},// search speedup
                        new double[]{1.0, 1.1, 1.5, 2.0, 2.5, 3.0},                  // search expansion
                        "test-spam-expansion.csv"));

        // Vary random jumps
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0, 2, 2, 4, 8},                       // random jumps
                        new double[]{5.0},// search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-jumps.csv"));


        // Vary search speedup
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0, 10.0, 15.0, 20.0},// search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-speedup.csv"));


        // Vary update depth
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{5},                       // iterations
                        new int[]{1, 2, 3, 4, 5, 6},        // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},                 // search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-depth.csv"));

        // Vary partitioning iterations
        multi_test.batches.add(
                new Batch(
                        new int[]{8},                       // partitions
                        new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},// iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},                 // search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-iterations.csv"));

        // Vary partitions
        multi_test.batches.add(
                new Batch(
                        new int[]{1, 8, 16, 24, 32},         // partitions
                        new int[]{5},                       // iterations
                        new int[]{2},                       // update depth
                        new int[]{0},                       // random jumps
                        new double[]{5.0},                  // search speedup
                        new double[]{1.1},                  // search expansion
                        "test-spam-partitions.csv"));
        */
        multi_test.run();
    }

}
