package com.snowplowanalytics.snowplow.webmodel

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.parse

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer

object WebModel {

  /** Initialize Spark Job environment from parsed configuration and launch modeling */
  def init(jobConfig: JobConfig): Unit = {
    val conf = new SparkConf()
      .setAppName("sparkDataModeling")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()
    val atomicData = getAtomicData(sc, spark, jobConfig.input)

    model(spark, atomicData).show(3)
  }

  /*
  def initDf(jobConfig: JobConfig): Unit = {
    val conf = new SparkConf()
      .setAppName("sparkDataModeling")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val atomicData = getAtomicData(sc, spark, jobConfig.input)

    val scratchWebPageContextData = scratchWebPageContextDf(atomicData)
    val scratchEventsData = scratchEventsDf(atomicData, scratchWebPageContextData)


  }
  */



  def getAtomicData(sc: SparkContext, spark: SparkSession, path: String): DataFrame = {
    val inputRDD = sc.textFile(path)
    val eventsRDD = inputRDD.map(transformToJson).persist
    spark.read.json(eventsRDD)
    // add filtering on page_name and platform here
  }

  def transformToJson(line: String): String =
    EventTransformer.transform(line) match {
      case Right(s) => s
      case Left(_) => throw new RuntimeException(s"Unexpected JSON input: $line")
    }


  def createAtomicEvents(spark: SparkSession, enrichedData: DataFrame): DataFrame = {
    enrichedData.createOrReplaceTempView("atomic_events")
    enrichedData
  }

  /** Select all desired fields from atomic.events and deduplicate cases where there is more than one page view
    * per page view ID */
  def scratchWebEventsDf(atomicEvents: DataFrame): DataFrame = {
    val userId = new ColumnName("user_id")
    val domainUserId = new ColumnName("domain_userid")
    val networkUserId = new ColumnName("network_userid")
    val domainSessionId = new ColumnName("domain_sessionid")
    val domainSessionIdx = new ColumnName("domain_sessionidx")
    val pageViewId = new ColumnName("contexts_com_snowplowanalytics_snowplow_web_page_1")
      .getItem(0)               // Get first available context
      .getField("id")           // Get `id` property
      .alias("page_view_id")    // Alias
    val pageTitle = new ColumnName("page_title")
    val pageUrlScheme = new ColumnName("page_urlscheme")
    val pageUrlHost = new ColumnName("page_urlhost")
    val pageUrlPort = new ColumnName("page_urlport")
    val pageUrlPath = new ColumnName("page_urlpath")
    val pageUrlQuery = new ColumnName("page_urlquery")
    val pageUrlFragment = new ColumnName("page_urlfragment")
    val refrUrlScheme = new ColumnName("refr_urlscheme")
    val refrUrlHost = new ColumnName("refr_urlhost")
    val refrUrlPort = new ColumnName("refr_urlport")
    val refrUrlPath = new ColumnName("refr_urlpath")
    val refrUrlQuery = new ColumnName("refr_urlquery")
    val refrUrlFragment = new ColumnName("refr_urlfragment")
    val refrMedium = new ColumnName("refr_medium")
    val refrSource = new ColumnName("refr_source")
    val refrTerm = new ColumnName("refr_term")
    val mktMedium = new ColumnName("mkt_medium")
    val mktSource = new ColumnName("mkt_source")
    val mktTerm = new ColumnName("mkt_term")
    val mktContent = new ColumnName("mkt_content")
    val mktCampaign = new ColumnName("mkt_campaign")
    val mktClickId = new ColumnName("mkt_clickid")
    val mktNetwork = new ColumnName("mkt_network")
    val geoCountry = new ColumnName("geo_country")
    val geoRegion = new ColumnName("geo_region")
    val geoRegionName = new ColumnName("geo_region_name")
    val geoCity = new ColumnName("geo_city")
    val geoZipcode = new ColumnName("geo_zipcode")
    val geoLatitude = new ColumnName("geo_latitude")
    val geoLongitude = new ColumnName("geo_longitude")
    val geoTimezone = new ColumnName("geo_timezone")
    val userIpAddress = new ColumnName("user_ipaddress")
    val ipIsp = new ColumnName("ip_isp")
    val ipOrganization = new ColumnName("ip_organization")
    val ipDomain = new ColumnName("ip_domain")
    val ipNetSpeed = new ColumnName("ip_netspeed")
    val appId = new ColumnName("app_id")
    val useragent = new ColumnName("useragent")
    val brName = new ColumnName("br_name")
    val brFamily = new ColumnName("br_family")
    val brRenderEngine = new ColumnName("br_renderengine")
    val brLang = new ColumnName("br_lang")
    val dvceType = new ColumnName("dvce_type")
    val dvceIsMobile = new ColumnName("dvce_ismobile")
    val useragentFamily = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("useragentFamily")
      .alias("useragent_family")
    val useragentMajor = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("useragentMajor")
      .alias("useragent_major")
    val useragentMinor = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("useragentMinor")
      .alias("useragent_minor")
    val useragentPatch = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("useragentPatch")
      .alias("useragent_patch")
    val useragentVersion = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("useragentVersion")
      .alias("useragent_version")
    val osFamily = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("osFamily")
      .alias("os_family")
    val osMajor = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("osMajor")
      .alias("os_major")
    val osMinor = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("osMinor")
      .alias("os_minor")
    val osPatch = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("osPatch")
      .alias("os_patch")
    val osVersion = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("osVersion")
      .alias("os_version")
    val deviceFamily = new ColumnName("contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
      .getItem(0)
      .getField("deviceFamily")
      .alias("device_family")
    val osManufacturer = new ColumnName("os_manufacturer")
    val osTimezone = new ColumnName("os_timezone")
    val nameTracker = new ColumnName("name_tracker")
    val dvceCreatedTstamp = new ColumnName("dvce_created_tstamp")
    val eventId = new ColumnName("event_id")

    atomicEvents
      .filter(new ColumnName("platform") === "web")
      .filter(new ColumnName("event_name") === "page_view")
      .select(userId, domainUserId, networkUserId, domainSessionId, domainSessionIdx, pageTitle, pageUrlScheme,
        pageUrlHost, pageUrlPort, pageUrlPath, pageUrlQuery, pageUrlFragment, refrUrlScheme, refrUrlHost, refrUrlPort,
        refrUrlPath, refrUrlQuery, refrUrlFragment, refrMedium, refrSource, refrTerm, mktMedium, mktSource, mktTerm,
        mktContent, mktCampaign, mktClickId, mktNetwork, geoCountry, geoRegion, geoRegionName, geoCity, geoZipcode,
        geoLatitude, geoLongitude, geoTimezone, userIpAddress, ipIsp, ipOrganization, ipDomain, ipNetSpeed, appId,
        useragent, brName, brFamily, brRenderEngine, brLang, dvceType, dvceIsMobile, useragentFamily, useragentMajor,
        useragentMinor, useragentPatch, useragentVersion, osFamily, osMajor, osMinor, osPatch, osVersion,
        deviceFamily, osManufacturer, osTimezone, nameTracker, dvceCreatedTstamp, eventId,
        row_number().over(Window.partitionBy(pageViewId).orderBy(dvceCreatedTstamp.asc)).alias("n"))
      .filter(new ColumnName("n") === 1)
      .drop("n")
  }

  /** Calculate time engaged */
  def scratchWebEventsTimeDf(atomicEvents: DataFrame): DataFrame = {
    val pageViewId = new ColumnName("contexts_com_snowplowanalytics_snowplow_web_page_1")
      .getItem(0)               // Get first available context
      .getField("id")           // Get `id` property
      .alias("page_view_id")    // Alias
    val derivedTstamp = new ColumnName("derived_tstamp")
    val eventName = new ColumnName("event_name")

    atomicEvents
      .filter(eventName === "page_view" || eventName === "page_ping")
      .select(pageViewId,
        min(derivedTstamp).alias("min_tstamp"),
        max(derivedTstamp).alias("max_tstamp"),
        sum(when(eventName === "page_view", 1).otherwise(0)).alias("pv_count"),
        sum(when(eventName === "page_ping", 1).otherwise(0)).alias("pp_count"),
        countDistinct(floor(unix_timestamp(derivedTstamp) / 10)) * 10 - 10).alias("time_engaged_in_s")
    // HOW TO GROUP BY 1?
  }

  /** Get unique pairs of `event_id` and `page_view_id` */
  def scratchWebPageContextDf(atomicEvents: DataFrame): DataFrame = {
    val eventId = new ColumnName("event_id")
    val pageViewId = new ColumnName("contexts_com_snowplowanalytics_snowplow_web_page_1")
      .getItem(0)               // Get first available context
      .getField("id")           // Get `id` property
      .as("page_view_id")       // Alias

    atomicEvents
      .select(eventId, pageViewId)
      .groupBy(eventId, new ColumnName("page_view_id"))
      .count().filter(new ColumnName("count") === 1)    // Exclude all rows with more than one page view id
      .drop("count")
  }

  def scratchWebPageContext(spark: SparkSession): Unit = {
    val dfWebPageContext = spark.sql(
      """
      WITH prep AS
      (
        SELECT
          event_id AS root_id,
          contexts_com_snowplowanalytics_snowplow_web_page_1[0] AS page_view_id
        FROM
          atomic_events
        GROUP BY 1, 2
      )

      SELECT
        *
      FROM
        prep
      WHERE
        root_id NOT IN (SELECT root_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1) -- exclude all root ID with more than one page view ID
      """
    )
    dfWebPageContext.createOrReplaceTempView("scratch_web_page_context")
  }

  def scratchEvents(spark: SparkSession): Unit = {
    val dfEvents = spark.sql(
      """
      -- select the relevant dimensions from atomic_events

      WITH prep AS
      (
        SELECT
          ev.user_id,
          ev.domain_userid,
          ev.network_userid,
          ev.domain_sessionid,
          ev.domain_sessionidx,
          wp.page_view_id,
          ev.page_title,
          ev.page_urlscheme,
          ev.page_urlhost,
          ev.page_urlport,
          ev.page_urlpath,
          ev.page_urlquery,
          ev.page_urlfragment,
          ev.refr_urlscheme,
          ev.refr_urlhost,
          ev.refr_urlport,
          ev.refr_urlpath,
          ev.refr_urlquery,
          ev.refr_urlfragment,
          ev.refr_medium,
          ev.refr_source,
          ev.refr_term,
          ev.mkt_medium,
          ev.mkt_source,
          ev.mkt_term,
          ev.mkt_content,
          ev.mkt_campaign,
          ev.mkt_clickid,
          ev.mkt_network,
          ev.geo_country,
          ev.geo_region,
          ev.geo_region_name,
          ev.geo_city,
          ev.geo_zipcode,
          ev.geo_latitude,
          ev.geo_longitude,
          ev.geo_timezone,
          ev.user_ipaddress,
          ev.ip_isp,
          ev.ip_organization,
          ev.ip_domain,
          ev.ip_netspeed,
          ev.app_id,
          ev.useragent,
          ev.br_name,
          ev.br_family,
          ev.br_version,
          ev.br_type,
          ev.br_renderengine,
          ev.br_lang,
          ev.dvce_type,
          ev.dvce_ismobile,
          ev.os_name,
          ev.os_family,
          ev.os_manufacturer,
          ev.os_timezone,
          ev.name_tracker, -- included to filter on
          ev.dvce_created_tstamp -- included to sort on
        FROM
          atomic_events AS ev
          INNER JOIN scratch_web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
            ON ev.event_id = wp.root_id
        WHERE
          ev.platform = 'web'
          AND ev.event_name = 'page_view' -- filtering on page view events removes the need for a FIRST_VALUE function
        )

        -- more than one page view event per page view ID? select the first one

        SELECT
          *
        FROM
          (
            SELECT
              *,
              ROW_NUMBER () OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp) AS n
            FROM
              prep
          )
        WHERE
          n = 1
      """
    )
    dfEvents.createOrReplaceTempView("scratch_events")
  }

  def scratchWebEventsTime(spark: SparkSession): Unit = {
    val dfEventsTime = spark.sql(
      """
        |SELECT
        |  wp.page_view_id,
        |  MIN(ev.derived_tstamp) AS min_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)
        |  MAX(ev.derived_tstamp) AS max_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)
        |  SUM(CASE WHEN ev.event_name = 'page_view' THEN 1 ELSE 0 END) AS pv_count, -- for debugging
        |  SUM(CASE WHEN ev.event_name = 'page_ping' THEN 1 ELSE 0 END) AS pp_count, -- for debugging
        |  10 * COUNT(DISTINCT(FLOOR(UNIX_TIMESTAMP(ev.derived_tstamp)/10))) - 10 AS time_engaged_in_s -- assumes 10 seconds between subsequent page pings
        |
        |FROM
        |  atomic_events AS ev
        |  INNER JOIN scratch_web_page_context AS wp
        |    ON ev.event_id = wp.root_id
        |
        |WHERE
        |  ev.event_name IN ('page_view', 'page_ping')
        |
        |GROUP BY 1
      """.stripMargin
    )
    dfEventsTime.createOrReplaceTempView("scratch_web_events_time")
  }

  /** Primary data-modeling function */
  def model(spark: SparkSession, enrichedData: DataFrame): DataFrame = {
    createAtomicEvents(spark, enrichedData)

    // 00-web-page-context
    scratchWebPageContext(spark)

    // 01-events
    scratchEvents(spark)

    // 02-events-time
    scratchWebEventsTime(spark)

    spark.sql("select * from scratch_web_events_time")

    /*
    spark.sql("SELECT * FROM web_page_context").show(5)

    spark.sql("SELECT contexts_com_snowplowanalytics_snowplow_web_page_1 FROM events").show(3)

    val dfVisitors = spark.sql("SELECT domain_userid, MAX(domain_sessionidx) AS sessions FROM events GROUP BY domain_userid")
    dfVisitors.registerTempTable("visitors")

    spark.sql("SELECT a.domain_userid, b.sessions, COUNT(*) AS count FROM events AS a LEFT JOIN visitors AS b ON a.domain_userid = b.domain_userid GROUP BY a.domain_userid, b.sessions").show(5)
    */
  }
}
