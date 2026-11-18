package io.github.siwarattia.datahub.baseline

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/** Parallel-run reconciliation: the legacy baseline output vs the curated layer,
  * per business date - the parallel-validation pattern from production, in code.
  *
  * Per partition:
  *  - row counts and distinct business keys on both sides
  *  - key-level full-outer-join diff: matched / only-legacy / only-curated
  *  - amount comparison among matched rows (the core financial figure)
  *  - a strict go/no-go verdict: any gap fails - a domain cut over only when its
  *    reconciliation report is clean
  *
  * The json report lands next to the data (reports/reconciliation/) so the
  * compliance-side decision makers - here, the milestone reader - see the evidence,
  * not just the claim.
  */
object ReconciliationJob {

  private val log = LoggerFactory.getLogger(getClass)

  final case class ReconciliationReport(
      business_date: String,
      verdict: String,
      legacy_count: Long,
      curated_count: Long,
      legacy_distinct: Long,
      matched: Long,
      only_legacy: Long,
      only_curated: Long,
      amount_mismatches: Long)

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: ReconciliationJob --date yyyy-MM-dd"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"reconciliation-$date", opts.get("master"))

    try run(spark, config, date)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    val legacy = spark.read.parquet(S3Locations.legacyDomain(config, "cur_transactions") +
      "/business_date=" + date)
    val curated = spark.read.parquet(S3Locations.curatedPartition(config, "transactions", date))

    val legacyKeys = legacy.select(col("txn_id"), col("amount").cast("double").as("amount_legacy"))
    val curatedKeys = curated.select(col("txn_id"), col("amount").as("amount_curated"))

    val legacyCount = legacyKeys.count()
    val curatedCount = curatedKeys.count()
    val legacyDistinct = legacyKeys.select("txn_id").distinct().count()

    val diff = legacyKeys.join(curatedKeys, Seq("txn_id"), "full_outer")
    val matched = diff.filter(col("amount_legacy").isNotNull && col("amount_curated").isNotNull).count()
    val onlyLegacy = diff.filter(col("amount_curated").isNull).count()
    val onlyCurated = diff.filter(col("amount_legacy").isNull).count()
    val mismatched = diff
      .filter(col("amount_legacy").isNotNull && col("amount_curated").isNotNull &&
        (abs(col("amount_legacy") - col("amount_curated")) > lit(1e-6)))
      .count()

    val verdict = if (legacyCount == curatedCount && onlyLegacy == 0 &&
        onlyCurated == 0 && mismatched == 0) "PASS" else "FAIL"

    val report = ReconciliationReport(date, verdict, legacyCount, curatedCount,
      legacyDistinct, matched, onlyLegacy, onlyCurated, mismatched)

    import spark.implicits._
    spark.createDataset(Seq(report))
      .coalesce(1)
      .write.mode("overwrite")
      .json(S3Locations.reconciliationReports(config))

    log.info(s"[$date] reconciliation verdict: $verdict " +
      s"(legacy=$legacyCount curated=$curatedCount matched=$matched " +
      s"only_legacy=$onlyLegacy only_curated=$onlyCurated mismatches=$mismatched)")
  }
}
