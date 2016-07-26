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

# Vary k
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 4 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 20 -pi 5 -pm 4 -ud 2 -ss 4 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 40 -pi 5 -pm 4 -ud 2 -ss 4 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log

# Vary random jumps
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 4 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 4 -srj 2 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 4 -srj 3 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log

# Vary search speedup
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 4 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 8 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log
$SPARK $OPTS --class $CLASS $JAR -i $DATASET -o "test-spam.csv" -k 10 -pi 5 -pm 4 -ud 2 -ss 16 -srj 1 -se 1.2 -n 1000 -na 1000 -ne 1000 -mur 0 2&>> test-spam.log

# Send e-mail when done
#../sendmail.py "Test with spam dataset is done"

