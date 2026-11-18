package io.github.siwarattia.datahub.baseline

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/** The legacy baseline: a Spark SQL port of migration/legacy/hive/daily_load.hql.
  *
  * Same logic as the legacy script, line for line:
  *  - stage the day's delivered feed
  *  - deduplicate on the business key (txn_id), keep the latest version
  *  - publish the day's partition with the business validity filters
  *
  * The output goes to a legacy-style bucket; the reconciliation job (same module)
  * proves this output and the curated layer agree - the parallel-validation pattern
  * from production, in code. Written in Spark SQL on purpose: the migration port
  * stays as close to the HiveQL as possible, so the comparison is honest.
  */
object BaselineJob {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: BaselineJob --date yyyy-MM-dd"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"baseline-$date", opts.get("master"))

    try run(spark, config, date)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    val raw = spark.read.parquet(S3Locations.rawPartition(config, "transactions", date))
    // the view is the day's feed only (date-scoped by the raw partition path),
    // so the ported sql drops the legacy script's date filter
    raw.createOrReplaceTempView("raw_transactions")

    // the daily_load.hql logic, ported: staging was typed in the legacy script
    // (amount double at staging), so the cast happens before the filters here too
    val baseline = spark.sql(
      s"""
      |select txn_id, account_id, direction, amount, currency, channel,
      |       counterparty_name, counterparty_bic, counterparty_country,
      |       value_date, txn_timestamp
      |from (
      |  select t.*,
      |         row_number() over (partition by txn_id order by txn_timestamp desc) as rn
      |  from raw_transactions t
      |) ranked
      |where rn = 1
      |  and cast(amount as double) > 0
      |  and currency != 'XX'
      |  and account_id != 'ACCT000000'
      """.stripMargin)

    val out = baseline
      .withColumn("business_date", col("value_date"))

    out.write.mode("overwrite").partitionBy("business_date")
      .parquet(S3Locations.legacyDomain(config, "cur_transactions") + "/business_date=" + date)

    log.info(s"[$date] legacy baseline written: ${out.count()} rows")
  }
}
