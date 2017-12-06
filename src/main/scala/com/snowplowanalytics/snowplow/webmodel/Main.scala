package com.snowplowanalytics.snowplow.webmodel

object Main {
  /** Job entry-point */
  def main(args: Array[String]) {
    JobConfig.parseCli(args) match {
      case Right(config) => WebModel.init(config)
      case Left(error) => System.err.println(error)
    }
  }
}