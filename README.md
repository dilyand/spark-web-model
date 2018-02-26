# Snowplow Spark Web Model

## Quickstart

First, in order to always have a stable Spark environment, create a Docker image. It will take some time, but should be done only once.

```shell
$ docker build -t "web-model:0.0.1" .
```

Next launch just created image as a container and `spark-shell` to start you experiments:

```shell
$ docker run --rm -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -h spark -v $(pwd):/root/web-model --name=spark web-model:0.0.1
$ spark-shell --jars /root/web-model/target/scala-2.11/spark-web-model-0.1.0-rc1.jar
```

In `spark-shell` you can read your data and start testing modeling functions:

```shell
scala> import org.apache.spark.sql.{DataFrame, SparkSession}
scala> import com.snowplowanalytics.snowplow.webmodel.WebModel
scala> val spark = SparkSession.builder().getOrCreate()
scala> val atomicData = WebModel.getAtomicData(sc, spark, "/root/web-model/data/snplow6-2017-11-13.gz")
```
