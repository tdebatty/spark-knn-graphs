#!/bin/bash

CLASS="info.devatty.spark.knngraphs.eval.Spam"

### Local tests
JAR="target/spark-knn-graphs-eval-0.1-SNAPSHOT.jar"
SPARK="spark-submit"
OPTS=""
DATASET="/home/tibo/Datasets/SPAM/spam-subject-200K.txt"

### Eurecom cluster
#JAR="k-medoids-0.1-SNAPSHOT.jar"
#SPARK="/home/ubuntu/usr/spark-1.5.1-dist/bin/spark-submit"
#OPTS="--driver-memory 4g --num-executors 8 --executor-cores 4 --executor-memory 4g --master yarn-client --conf spark.eventLog.dir=/tmp/online/spark-events"
#DATASET="../datasets/spam-subject-200K.txt"

$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 8 -ud 2 -ss 4 -srj 2 -se 1.2 -n 200 -na 200 -ne 200 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 20 -pi 5 -pm 8 -ud 2 -ss 4 -srj 2 -se 1.2 -n 200 -na 200 -ne 200 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 40 -pi 5 -pm 8 -ud 2 -ss 4 -srj 2 -se 1.2 -n 200 -na 200 -ne 200 -mur 0 2&>> test-spam.log
