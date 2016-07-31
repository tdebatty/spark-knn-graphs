#!/bin/bash

CLASS="info.devatty.spark.knngraphs.eval.Synthetic"

### Local tests
JAR="target/spark-knn-graphs-eval-0.1-SNAPSHOT.jar"
SPARK="/opt/spark-1.6.2-bin-hadoop2.6/bin/spark-submit"
OPTS=""
DATASET="-"

### Eurecom cluster
#JAR="k-medoids-0.1-SNAPSHOT.jar"
#SPARK="/home/ubuntu/usr/spark-1.5.1-dist/bin/spark-submit"
#OPTS="--driver-memory 4g --num-executors 8 --executor-cores 4 --executor-memory 4g --master yarn-client --conf spark.eventLog.dir=/tmp/online/spark-events"
#DATASET="../datasets/spam-subject-200K.txt"

$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 1 -se 1.2 -n 20000 -na 10 -ne 10 -mur 0 2&>> test-synthetic.log

# Vary partitioning iterations
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 0 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 1 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 2 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 3 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log

# Vary random jumps
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 0 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 1 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 2 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 3 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 4 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log
#$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-synthetic.csv" -k 10 -pi 4 -pm 4 -ud 2 -ss 10 -srj 5 -se 1.2 -n 2000 -na 2000 -ne 2000 -mur 0 2&>> test-synthetic.log


# Send e-mail when done
#../sendmail.py "Test with spam dataset is done"

