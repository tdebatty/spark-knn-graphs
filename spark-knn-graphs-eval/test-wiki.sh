#!/bin/bash

CLASS="info.devatty.spark.knngraphs.eval.Wiki"

### Local tests
JAR="target/spark-knn-graphs-eval-0.1-SNAPSHOT.jar"
SPARK="/opt/spark-1.6.2-bin-hadoop2.6/bin/spark-submit"
OPTS=""
DATASET="/home/tibo/Datasets/wikipedia-fr-html/fr/articles/"

### Eurecom cluster
#JAR="spark-knn-graphs-eval-0.1-SNAPSHOT.jar"
#SPARK="/home/ubuntu/usr/spark-1.5.1-dist/bin/spark-submit"
#OPTS="--driver-memory 4g --num-executors 8 --executor-cores 4 --executor-memory 4g --master yarn-client --conf spark.eventLog.dir=/tmp/online/spark-events"
#DATASET="-"

$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-wiki.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 5 -srj 1 -se 1.1 -n 1000 -na 100 -ne 100 -mur 0 2&>> test-wiki.log


# Send e-mail when done
#../sendmail.py "Test with Synthetic dataset is done"

