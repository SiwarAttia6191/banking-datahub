package io.github.siwarattia.datahub.quality

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/** Per-run data-quality report for one business date.
  *
  * The checks mirror what the generator injects and what the curated layer promises:
  *  - reconciliation: raw count == curated + rejects (nothing lost, nothing invented)
  *  - dedup completeness: no duplicate business keys survive
  *  - reject rate within the configured threshold
  *  - referential integrity: every curated transaction has a curated account
  *  - fx conversion completeness and iban validity (quarantine severity)
  *
  * The verdict lands in a json report next to the data; the orchestrator (phase 4)
  * reads it as the go/no-go gate.
  */
object DataQualityJob {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: DataQualityJob --date yyyy-MM-dd"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"data-quality-$date", opts.get("master"))

    try run(spark, config, date)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    val rawTxns = spark.read.parquet(S3Locations.rawPartition(config, "transactions", date))
    val curatedTxns = spark.read.parquet(S3Locations.curatedPartition(config, "transactions", date))
    // per-partition: the reconciliation compares one business date against itself -
    // reading the rejects base path would mix all dates into the count
    val rejects = spark.read.parquet(S3Locations.rejectsPartition(config, "transactions", date))
    val accounts = spark.read.parquet(S3Locations.curatedDomain(config, "accounts"))

    val maxRejectRate = config.rules.maxRejectRate

    def result(check: DQCheck)(outcome: (Boolean, String, String)): CheckResult =
      CheckResult(check, outcome._1, outcome._2, outcome._3)

    val results = Seq(
      result(DQCheck("raw_vs_curated_reconciliation", DQCheck.Fail)) {
        // the legacy contract deduplicates silently (keep latest), so the invariant is:
        // every unique business key lands in curated or rejects
        val raw = rawTxns.count()
        val rawUnique = rawTxns.select("txn_id").distinct().count()
        val curated = curatedTxns.count()
        val rejected = rejects.count()
        (rawUnique == curated + rejected,
          s"curated=$curated rejects=$rejected raw_unique=$rawUnique", s"raw=$raw (incl. duplicates)")
      },
      result(DQCheck("dedup_completeness", DQCheck.Fail)) {
        val total = curatedTxns.count()
        val distinct = curatedTxns.select("txn_id").distinct().count()
        (total == distinct, s"rows=$total distinct_txn_id=$distinct", "no duplicate business keys")
      },
      result(DQCheck("reject_rate", DQCheck.Fail)) {
        val total = rawTxns.count()
        val rejected = rejects.count()
        val rate = if (total == 0) 0.0 else rejected.toDouble / total
        (rate <= maxRejectRate, f"reject_rate=$rate%.4f", f"max=$maxRejectRate%.4f")
      },
      result(DQCheck("referential_integrity", DQCheck.Fail)) {
        val orphans = curatedTxns
          .join(accounts.select(col("account_id").as("ref_account_id")),
            col("account_id") === col("ref_account_id"), "left_anti")
          .count()
        (orphans == 0, s"orphans=$orphans", "0 transactions without a curated account")
      },
      result(DQCheck("fx_conversion_complete", DQCheck.Quarantine)) {
        val missing = curatedTxns.filter(col("amount_eur").isNull).count()
        (missing == 0, s"missing_amount_eur=$missing", "0 rows without an eur amount")
      },
      result(DQCheck("iban_validity", DQCheck.Quarantine)) {
        val invalid = accounts.filter(!col("iban_valid")).count()
        val total = accounts.count()
        (invalid == 0, s"invalid_ibans=$invalid of $total", "0 invalid ibans")
      }
    )

    def verdictOf(rs: Seq[CheckResult]): String =
      if (rs.exists(r => !r.passed && r.check.severity == DQCheck.Fail)) "FAIL" else "PASS"

    val report = DQReport(date, verdictOf(results), results)

    import spark.implicits._
    spark.createDataset(Seq(report))
      .coalesce(1)
      .write.mode("overwrite")
      .json(S3Locations.dqReports(config))

    report.results.foreach { r =>
      log.info(s"[$date] dq ${r.check.severity}/${r.check.name}: " +
        s"${if (r.passed) "PASS" else "FAIL"} (observed=${r.observed}, expected=${r.expected})")
    }
    log.info(s"[$date] dq verdict: ${report.verdict}")
  }
}
