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

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import info.debatty.spark.knngraphs.builder.Online;
import info.debatty.spark.knngraphs.builder.StatisticsAccumulator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public abstract class AbstractTest<T> {

    private static final int NODES_BEFORE_FEEDBACK = 10;

    private int k;
    private int partitioning_iterations;
    private int partitioning_medoids;
    private int n;
    private int n_add;
    private String dataset_file;
    private String result_file;
    private double search_speedup;
    private int search_random_jumps;
    private double search_expansion;
    private int update_depth;
    private Iterator<T> dataset_iterator;
    private int n_evaluation;
    private double medoids_update_ratio;

    private SimilarityInterface<T> similarity;

    private PrintWriter result_file_writer;

    /**
     *
     * @param similarity
     */
    public final void setSimilarity(final SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    /**
     *
     * @throws ParseException if we cannot parse command line
     * @throws Exception if we cannot build the initial graph
     */
    public final void run()
            throws ParseException, Exception {

        printSettings();

        if (!result_file.equals("-")) {
            log("Create result file");
            File f = new File(result_file);
            if (f.exists()) {
                result_file_writer = new PrintWriter(
                    new BufferedWriter(new FileWriter(result_file, true)));

            } else {
                result_file_writer = new PrintWriter(
                    new BufferedWriter(new FileWriter(result_file, true)));
                writeHeader();
            }
        }

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        log("Configure spark instance");
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        log("Spark version: " + sc.version());

        log("Read dataset...");
        List<Node<T>> dataset = new ArrayList<Node<T>>(n + n_add);
        for (int i = 0; i < n + n_add; i++) {
            dataset.add(new Node<T>(
                    String.valueOf(i),
                    dataset_iterator.next()));
        }

        log("Split the dataset between training and validation...");
        Random rand = new Random();
        ArrayList<Node<T>> validation_dataset
                = new ArrayList<Node<T>>(n_add);
        for (int i = 0; i < n_add; i++) {
            validation_dataset.add(
                    dataset.remove(rand.nextInt(dataset.size())));
        }

        log("Parallelize the training dataset and force execution...");
        JavaRDD<Node<T>> nodes = sc.parallelize(dataset);
        nodes = nodes.cache();
        nodes.count();

        log("Compute initial graph...");
        long start_time = System.currentTimeMillis();
        DistributedGraphBuilder<T> builder = new Brute<T>();
        builder.setK(k);
        builder.setSimilarity(similarity);
        JavaPairRDD<Node<T>, NeighborList> graph
                = builder.computeGraph(nodes);
        graph = graph.cache();
        graph.count();
        long time_build_graph = System.currentTimeMillis() - start_time;
        log("DONE!");
        System.out.printf(
                "%-30s %d ms\n", "Time to build graph:",
                time_build_graph);

        log("Initialize online graph (partition)...");
        start_time = System.currentTimeMillis();
        Online<T> online_graph = new Online<T>(
                k,
                similarity,
                sc,
                graph,
                partitioning_medoids,
                partitioning_iterations);

        online_graph.setUpdateDepth(update_depth);
        online_graph.setSearchSpeedup(search_speedup);
        online_graph.setSearchRandomJumps(search_random_jumps);
        online_graph.setSearchExpansion(search_expansion);
        online_graph.setMedoidUpdateRatio(medoids_update_ratio);

        long time_partition_graph = System.currentTimeMillis() - start_time;
        log("DONE!");
        System.out.printf(
                "%-30s %d ms\n", "Time to partition graph:",
                time_partition_graph);

        log("Add nodes...");
        int i = 0;
        long similarities = 0;
        long restarts = 0;
        // search restarts due to cross partition edges
        long xpartition_restarts = 0;
        start_time = System.currentTimeMillis();
        for (final Node<T> query : validation_dataset) {
            i++;
            Accumulator<StatisticsContainer> stats_accumulator = sc.accumulator(
                    new StatisticsContainer(),
                    new StatisticsAccumulator());

            online_graph.fastAdd(query, stats_accumulator);

            StatisticsContainer global_stats = stats_accumulator.value();
            similarities += global_stats.getSimilarities();
            restarts += global_stats.getSearchRestarts();
            xpartition_restarts
                    += global_stats.getSearchCrossPartitionRestarts();

            dataset.add(query);

            if (i % NODES_BEFORE_FEEDBACK == 0) {
                log("" + i);
            }

            if (i % n_evaluation == 0) {
                Graph<T> local_approximate_graph
                        = list2graph(online_graph.getGraph().collect());

                log("Compute verification graph");
                Graph<T> local_exact_graph
                        = list2graph(
                                builder.computeGraph(
                                        sc.parallelize(dataset)).collect());
                log("done...");

                int correct = 0;
                for (Node<T> node : local_exact_graph.getNodes()) {
                    correct += local_exact_graph.get(node).countCommons(
                            local_approximate_graph.get(node));
                }

                System.out.printf(
                        "%-30s %d (%f)\n", "Correct edges in online graph: ",
                        correct, 1.0 * correct / (k * (n + i)));

                long time_add = System.currentTimeMillis() - start_time;
                writeResult(
                        i,
                        correct,
                        similarities,
                        time_add,
                        restarts,
                        xpartition_restarts);
                similarities = 0;
                restarts = 0;
                xpartition_restarts = 0;
                System.currentTimeMillis();
            }
        }
        log("SUCCESS!");
    }

    /**
     *
     * @param iterator
     */
    public final void setDataSource(final Iterator<T> iterator) {
        this.dataset_iterator = iterator;
    }

    /**
     *
     * @return
     */
    public final String getDatasetFile() {
        return dataset_file;
    }

    /**
     *
     * @param args
     * @throws ParseException if the command line arguments cannot be parsed
     */
    public final void parseArgs(final String[] args) throws ParseException {
        log("Parse command line arguments...");
        Options options = new Options();
        options.addOption("i", true, "Input (dataset) file");
        options.addOption("o", true, "Output file for writing results of test");
        options.addOption("k", true, "K for building the k-nn graph (10)");
        options.addOption("pi", true, "Partition iterations (10)");
        options.addOption("pm", true, "Number of partitions (medoids) (32)");
        options.addOption("ud", true, "Update depth (2)");
        options.addOption("ss", true, "Fast search: Speedup (10)");
        options.addOption("srj", true, "Fast search : random jumps (2)");
        options.addOption("se", true, "Fast search : expansion (1.2)");
        options.addOption("n", true, "Nodes in initial graph (1000)");
        options.addOption("na", true, "Number of nodes to add (1000)");
        options.addOption("ne", true, "Nodes to add before evaluation (100)");
        options.addOption("mur", true, "Medoids update ratio (0)");
        options.addOption("h", false, "Show help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("h")
                || !cmdline.hasOption("i")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("...", options);
            System.exit(0);
        }

        // graph update algorithm parameters
        k = Integer.valueOf(cmdline.getOptionValue("k", "10"));
        partitioning_iterations
                = Integer.valueOf(cmdline.getOptionValue("pi", "10"));
        partitioning_medoids
                = Integer.valueOf(cmdline.getOptionValue("pm", "32"));
        update_depth = Integer.valueOf(cmdline.getOptionValue("ud", "2"));
        medoids_update_ratio
                = Double.valueOf(cmdline.getOptionValue("mur", "0.0"));

        // Search parameters
        search_speedup = Integer.valueOf(cmdline.getOptionValue("ss", "10"));
        search_random_jumps
                = Integer.valueOf(cmdline.getOptionValue("srj", "2"));
        search_expansion
                = Double.valueOf(cmdline.getOptionValue("se", "1.2"));

        // Test parameters
        dataset_file = cmdline.getOptionValue("i");
        result_file = cmdline.getOptionValue("o", "-");
        n = Integer.valueOf(cmdline.getOptionValue("n", "1000"));
        n_add = Integer.valueOf(cmdline.getOptionValue("na", "1000"));
        n_evaluation = Integer.valueOf(cmdline.getOptionValue("ne", "100"));

    }

    protected final void printSettings() {

        System.out.printf("%-30s %s\n", "Dataset:", dataset_file);
        System.out.printf("%-30s %s\n", "Initial graph size:", n);
        System.out.printf("%-30s %d\n", "k:", k);
        System.out.printf("%-30s %d\n", "Partitioning iterations:",
                partitioning_iterations);
        System.out.printf("%-30s %d\n", "Partitioning medoids:",
                partitioning_medoids);
        System.out.printf("%-30s %d\n", "Update depth:",
                update_depth);
        System.out.printf("%-30s %d\n", "Nodes to add:",
                n_add);
        System.out.printf("%-30s %f\n", "Search speedup:", search_speedup);
        System.out.printf("%-30s %d\n", "Search random jumps:",
                search_random_jumps);
        System.out.printf("%-30s %f\n", "Search expansion:", search_expansion);
        System.out.printf("%-30s %f\n", "Medoid update ratio:",
                medoids_update_ratio);
        System.out.printf("%-30s %s\n", "Result file:", result_file);
    }

    protected final void log(final String s) {
        java.util.Date date = new java.util.Date();
        SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
        System.out.printf("%s %s\n", format.format(date), s);
    }

    private Graph<T> list2graph(
            final List<Tuple2<Node<T>, NeighborList>> list) {

        Graph<T> graph = new Graph<T>();
        for (Tuple2<Node<T>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }

    private void writeResult(
            final int n_added,
            final int correct,
            final long similarities,
            final long time_add,
            final long restarts,
            final long xpartition_restarts) throws IOException {

        // Write results to file
        if (result_file_writer == null) {
            return;
        }

        double em = 1.0 * n * k * n_added / (n_added + n) + n_added * k;
        double eu = (1.0 * n + n_added) * k - em;
        double quality = (1.0 * correct - eu) / em;
        System.out.println("Quality factor (Q): " + quality);

        double correct_ratio = 1.0 * correct / (k * (n_added + n));

        long n_end = n + n_added;
        long n_start = n_end - n_evaluation;
        long similarities_naive
                = n_end * (n_end - 1) / 2 - n_start * (n_start - 1) / 2;

        double real_speedup = 1.0 * similarities_naive / similarities;
        double quality_equivalent_speedup = quality * real_speedup;

        result_file_writer.printf("%d\t", n);
        result_file_writer.printf("%d\t", k);
        result_file_writer.printf("%d\t", n_evaluation);
        result_file_writer.printf("%d\t", n_added);
        result_file_writer.printf("%d\t", partitioning_iterations);
        result_file_writer.printf("%d\t", partitioning_medoids);
        result_file_writer.printf("%d\t", update_depth);
        result_file_writer.printf("%f\t", medoids_update_ratio);
        result_file_writer.printf("%f\t", search_speedup);
        result_file_writer.printf("%d\t", search_random_jumps);
        result_file_writer.printf("%f\t", search_expansion);
        result_file_writer.printf("%d\t", correct);
        result_file_writer.printf("%f\t", correct_ratio);
        result_file_writer.printf("%f\t", quality);
        result_file_writer.printf("%d\t", similarities);
        result_file_writer.printf("%f\t", real_speedup);
        result_file_writer.printf("%f\t", quality_equivalent_speedup);
        result_file_writer.printf("%d\t", time_add);
        result_file_writer.printf("%d\t", restarts);
        result_file_writer.printf("%d\n", xpartition_restarts);
        result_file_writer.flush();
    }

    private void writeHeader() {
        result_file_writer.printf("# n\t");
        result_file_writer.printf("k\t");
        result_file_writer.printf("n_evaluation\t");
        result_file_writer.printf("n_added\t");
        result_file_writer.printf("partitioning_iterations\t");
        result_file_writer.printf("partitioning_medoids\t");
        result_file_writer.printf("update_depth\t");
        result_file_writer.printf("medoids_update_ratio\t");
        result_file_writer.printf("search_speedup\t");
        result_file_writer.printf("search_random_jumps\t");
        result_file_writer.printf("search_expansion\t");
        result_file_writer.printf("correct\t");
        result_file_writer.printf("correct_ratio\t");
        result_file_writer.printf("quality\t");
        result_file_writer.printf("similarities\t");
        result_file_writer.printf("real_speedup\t");
        result_file_writer.printf("quality_equivalent_speedup\t");
        result_file_writer.printf("time_add\t");
        result_file_writer.printf("restarts\t");
        result_file_writer.printf("xpartition_restarts\n");
        result_file_writer.flush();
    }

}
