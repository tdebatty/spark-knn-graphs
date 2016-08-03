#!/bin/bash

CLASS="info.devatty.spark.knngraphs.eval.MultiSpam"

### Local test
JAR="target/spark-knn-graphs-eval-0.1-SNAPSHOT.jar"
SPARK="/opt/spark-1.6.2-bin-hadoop2.6/bin/spark-submit"
OPTS=""
DATASET="/home/tibo/Datasets/SPAM/spam-subject-200K.txt"

### Eurecom cluster
#JAR="k-medoids-0.1-SNAPSHOT.jar"
#SPARK="/home/ubuntu/usr/spark-1.5.1-dist/bin/spark-submit"
#OPTS="--driver-memory 4g --num-executors 16 --executor-cores 4 --executor-memory 4g --master yarn-client --conf spark.eventLog.dir=/tmp/online/spark-events"
#DATASET="../datasets/spam-subject-200K.txt"

$SPARK $OPTS --class $CLASS $JAR $DATASET




# Send e-mail when done
#../sendmail.py "Test with spam dataset is done"

