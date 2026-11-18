package io.github.siwarattia.datahub.transform

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/** The serving layer: publish the curated aggregates and alerts for one business date
  * into PostgreSQL - "the DataHub serves the business", as the last step of the eod
  * chain, after the dq gate passed.
  *
  * Tables (schema `serving`):
  *  - daily_volumes: one row per account-direction-day with the behavioral features
  *  - aml_alerts: the detection alerts with triage scores and severity
  *
  * Publication is an append: the same date can be republished after a rerun - the
  * serving tables are consumption copies, the lake stays the source of truth. Idempotent
  * at the consumption level via the business keys (account/day for volumes, alert_id
  * for alerts).
  */
object ServingJob {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: ServingJob --date yyyy-MM-dd"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"serving-$date", opts.get("master"))

    try run(spark, config, date)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    // base-path reads + date filter: partition-dir reads would drop the partition column
    val features = spark.read.parquet(S3Locations.curatedDomain(config, "account_day_features"))
      .filter(col("business_date") === lit(date).cast("date"))
    val alerts = spark.read.parquet(S3Locations.curatedDomain(config, "alerts"))
      .filter(col("business_date") === lit(date).cast("date"))

    val volumes = features
      .select(
        col("business_date"), col("account_id"), col("direction"),
        col("txn_count"), col("day_sum_eur"), col("sum_7d_eur"), col("count_7d"),
        col("zscore_day_log"), col("boxplot_outlier"))
      .withColumn("published_at", current_timestamp())

    val alertOut = alerts
      .select(
        col("alert_id"), col("rule_id"), col("business_date"), col("account_id"),
        col("txn_id"), col("amount_eur"), col("detail"), col("score"), col("severity"))
      .withColumn("published_at", current_timestamp())

    writeJdbc(volumes, config, "serving.daily_volumes")
    writeJdbc(alertOut, config, "serving.aml_alerts")

    log.info(s"[$date] serving published: ${volumes.count()} daily_volumes, ${alertOut.count()} aml_alerts")
  }

  /** JDBC publish: spark creates the tables on first write; appends afterwards. */
  private def writeJdbc(df: DataFrame, config: PipelineConfig, table: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", config.serving.url)
      .option("dbtable", table)
      .option("user", config.serving.user)
      .option("password", config.serving.password)
      .option("driver", "org.postgresql.Driver")
      .mode("append")
      .save()
    log.info(s"published to $table: ${df.count()} rows")
  }
}
