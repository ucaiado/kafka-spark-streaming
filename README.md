SF crime statistics with Spark Streaming
===========

This project is part of the [Data Streaming Nanodegree](https://www.udacity.com/course/data-streaming-nanodegree--nd029) program, from Udacity. I will implement a streaming application using Apache Spark Structured Streaming to ingest data and a Kafka server to produce data related to San Francisco crime incidents, extracted from Kaggle.


### Install

The project requires __Python 3.6__ and the following:

- [Spark 2.4.3](https://spark.apache.org/downloads.html.)
- Scala 2.11.x
- Java 1.8.x
- [Kafka build with Scala 2.11.x](https://kafka.apache.org/downloads)

After the installation, set up your python environment to run the code in this repository, create a new environment with Anaconda, and install the dependencies.

```shell
$ conda create --name kafka-spark python=3.6
$ source activate kafka-spark
$ pip install -r requirements.txt
```


### Run
In a terminal or command window, navigate to the top-level project directory (that contains this README) and run the following commands. You’ll need to open up several terminal tabs to execute each command:

```shell
$ cd scripts/
$ . start.sh
$ . start-zookeeper.sh
$ . start-kafka-server.sh
$ . start-broker.sh
```

Finally, after starting up all the services, you can ingest data using a Kafka consumer or using Spark. Again, navigate to the top-level project directory:

```shell
$ python consumer_server.py
$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
```


### Project Requirements

#### Screenshots

All screenshots are available in the [screenshots/](screenshots/) folder.

#### Questions

##### 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Depending on the startup parameters used, we can fine-tune both latency and throughput to our requirements. For example, `processedRowsPerSecond` makes the application ingests more data per second, leading to higher throughput.

##### 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
I tested different values to the parameters bellow, and 200 to the first two presented the best performance:
- `spark.streaming.kafka.maxRatePerPartition`
- `spark.streaming.kafka.processedRowsPerSecond`
- `spark.streaming.backpressure.enabled`
- `spark.default.parallelism`
- `spark.sql.shuffle.partitions`


### License
The contents of this repository are covered under the [MIT License](LICENSE).
