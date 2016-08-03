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

/**
 *
 * @author Thibault Debatty
 */
public class Batch {

    public int[] partitioning_medoids;
    public int[] partitioning_iterations;
    public int[] update_depths;
    public int[] random_jumps;
    public double[] search_speedups;
    public double[] search_expansions;
    public String result_file;

    public Batch(int[] partitioning_medoids,
            int[] partitioning_iterations,
            int[] update_depths,
            int[] random_jumps,
            double[] search_speedups,
            double[] search_expansions,
            String result_file) {
        this.partitioning_medoids = partitioning_medoids;
        this.partitioning_iterations = partitioning_iterations;
        this.update_depths = update_depths;
        this.random_jumps = random_jumps;
        this.search_speedups = search_speedups;
        this.search_expansions = search_expansions;
        this.result_file = result_file;

    }
}
