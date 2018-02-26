# Snowplow Spark Web Model

## Quickstart

```shell
$ docker build -t "web-model:0.0.1" .
$ docker run --rm -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -h spark -v $(pwd):/root/web-model --name=spark web-model:0.0.1
$ spark-shell --jars /root/web-model/target/scala-2.11/spark-web-model-0.1.0-rc1.jar
scala> import org.apache.spark.sql.{DataFrame, SparkSession}
scala> val spark = SparkSession.builder().getOrCreate()
scala> import com.snowplowanalytics.snowplow.webmodel.WebModel
scala> val enrichedData = sc.textFile("/root/web-model/data/snplow6-2017-11-13.gz")
```
