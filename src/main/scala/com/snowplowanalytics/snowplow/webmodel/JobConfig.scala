package com.snowplowanalytics.snowplow.webmodel

/**
  * Complete configuration parsed from CLI or passed directly
  * @param input S3 (or local) path to folder with enriched data
  */
case class JobConfig(input: String)

object JobConfig {
  private[webmodel] case class RawJobConfig(input: String)

  /** Temporary always-invalid CLI configuration to build a valid one */
  private [webmodel] val rawJobConfig = RawJobConfig("")

  private val parser = new scopt.OptionParser[RawJobConfig](generated.ProjectMetadata.name) {
    opt[String]('i', "input")
      .action((x, c) => c.copy(input = x))
      .text("S3 path to enriched data")
  }

  /** Parse arguments passed via CLI to either error message or valid configuration */
  def parseCli(argv: Array[String]): Either[String, JobConfig] =
    parser.parse(argv, rawJobConfig) match {
      case Some(valid) => validate(valid)
      case None => Left("Invalid options")    // Exact message will be printed out by scopt
    }

  def validate(rawJobConfig: RawJobConfig): Either[String, JobConfig] = {
    if (rawJobConfig.input.startsWith("s3://")) Right(JobConfig(rawJobConfig.input))
    else Left("Input path must start with s3:// prefix")
  }
}
