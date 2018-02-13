lazy val root = project.in(file("."))
  .settings(Seq(
    name := "spark-web-model",
    mainClass := Some("com.snowplowanalytics.snowplow.webmodel.Main"),
    organization := "com.snowplowanalytics",
    version := "0.1.0-rc1",
    scalaVersion := "2.11.12"
  ))
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.hadoop,
      Dependencies.spark,
      Dependencies.sparkSql,
      Dependencies.analyticsSdk,
      Dependencies.scopt
    )
  )

