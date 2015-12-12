# Spark k-nn graph examples

## Running the examples

These examples are meant to show how to use the spark-knn-graph library in your own project. If you wish to actually run the examples:

1. clone the GIT repository
```
git clone https://github.com/tdebatty/spark-knn-graphs.git
```

2. Build the JAR
```
$ mvn package
```

3. Run the examples using spark-submit
```
$ export LIBS=$(echo target/libs/*.jar | tr ' ' ',')
$ spark-submit \
  --class info.debatty.spark.knngraphs.example.Search \
  --jars $LIBS \
  target/spark-knn-graphs-0.7-SNAPSHOT.jar \
  726-unique-spams
```

## Examples

###Distributed Brute Force graph building

###LSHSuperBitExample

###LSHSuperBitNNDescentTextExample

###LSHSuperBitSparseIntegerVectorExample

###LSHSuperBitTextExample

###NNCTPHExample

###NNDescentCustomValue


###Search