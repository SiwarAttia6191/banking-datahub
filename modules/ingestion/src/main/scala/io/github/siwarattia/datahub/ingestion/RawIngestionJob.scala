package io.github.siwarattia.datahub.ingestion

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

/** Land the source csv feeds into the raw zone, exactly as delivered.
  *
  * Raw-layer rules:
  *  - every column is a string: the landing zone records what was delivered and makes no
  *    typing decisions - validation and casting belong to the curated layer, where they
  *    are tested
  *  - explicit schemas everywhere, never schema inference (inference reads the data to
  *    guess types, drifts silently when a source changes, and breaks on empty files)
  *  - one partition per ingestion_date; a rerun overwrites exactly that partition
  *    (dynamic overwrite is configured on the session) and touches nothing else
  *
  * --date all processes the whole inbox backlog in one run (the daily feeds' own date
  * column becomes the partition; snapshots are stamped with today). The backlog mode
  * exists because downstream baselines need the full history, not one day of it.
  */
object RawIngestionJob {

  private val log = LoggerFactory.getLogger(getClass)

  private def rawSchema(columns: String*): StructType =
    StructType(columns.map(c => StructField(c, StringType, nullable = true)))

  private val clientsSchema =
    rawSchema("client_id", "name", "client_type", "country", "risk_level", "kyc_status")

  private val accountsSchema =
    rawSchema("account_id", "client_id", "iban", "currency", "status")

  private val restrictedCountriesSchema =
    rawSchema("country_code", "list_source", "effective_date")

  private val fxRatesSchema =
    rawSchema("rate_date", "base_ccy", "quote_ccy", "rate")

  private val transactionsSchema =
    rawSchema(
      "txn_id", "account_id", "direction", "amount", "currency", "channel",
      "counterparty_name", "counterparty_bic", "counterparty_country",
      "value_date", "txn_timestamp"
    )

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: RawIngestionJob --date yyyy-MM-dd | --date all"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"raw-ingestion-$date", opts.get("master"))

    try {
      ingest(spark, config, date)
    } finally {
      spark.stop()
    }
  }

  private def ingest(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    val backlog = date == "all"

    def dailyFeed(domain: String): String =
      if (backlog) S3Locations.inbox(config, domain)
      else S3Locations.inboxDaily(config, domain, date)

    val feeds: Seq[(String, StructType, String, Option[String])] = Seq(
      ("clients", clientsSchema, S3Locations.inbox(config, "clients"), None),
      ("accounts", accountsSchema, S3Locations.inbox(config, "accounts"), None),
      ("restricted_countries", restrictedCountriesSchema,
        S3Locations.inbox(config, "restricted_countries"), None),
      ("fx_rates", fxRatesSchema, dailyFeed("fx_rates"), Some("rate_date")),
      ("transactions", transactionsSchema, dailyFeed("transactions"), Some("value_date"))
    )

    feeds.foreach { case (domain, schema, path, partitionFrom) =>
      val rows = land(spark, config, domain, schema, path, date, partitionFrom)
      log.info(s"raw ingestion [$date] $domain: $rows rows landed")
    }
    log.info(s"raw ingestion complete for $date")
  }

  private def land(spark: SparkSession, config: PipelineConfig, domain: String,
                   schema: StructType, path: String, date: String,
                   partitionFrom: Option[String]): Long = {
    val reader = spark.read.option("header", "true").schema(schema)
    val df: DataFrame = partitionFrom match {
      // backlog mode: recursiveFileLookup disables path-based partition discovery, and
      // the feed's own date column becomes the ingestion partition
      case Some(dateColumn) =>
        reader.option("recursiveFileLookup", "true").csv(path)
          .withColumn("ingestion_date", col(dateColumn))
      case None =>
        reader.csv(path)
          .withColumn("ingestion_date",
            lit(if (date == "all") java.time.LocalDate.now.toString else date))
    }

    val rows = df.count()

    // the partition column is added on write: spark writes raw/{domain}/ingestion_date=D,
    // and dynamic overwrite means a rerun replaces only the partitions present in the df
    df.write.mode("overwrite").partitionBy("ingestion_date")
      .parquet(S3Locations.rawDomain(config, domain))

    rows
  }
}
