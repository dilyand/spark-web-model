import sbt._

object Dependencies {

  object V {
    val hadoop           = "2.7.3"
    val spark            = "2.1.0"
    val analyticsSdk     = "0.2.0"
  }

  val hadoop           = "org.apache.hadoop"     % "hadoop-aws"                    % V.hadoop         % "provided"
  val spark            = "org.apache.spark"      %% "spark-core"                   % V.spark          % "provided"
  val sparkSql         = "org.apache.spark"      %% "spark-sql"                    % V.spark          % "provided"
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
}
