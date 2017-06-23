## Start your batch spark script:

spark-submit --master spark://34.224.161.59:7077 --executor-memory 6G spark-batch.pysc

## Start your kafka-consumer script:

sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 streaming.py 

## Copy from/to remote machine

scp sm:/home/ubuntu/spark-batch-new-ps-em.py /home/samariya57/ATV/ATV/src/batch/
