package io.github.siwarattia.datahub.quality

import io.github.siwarattia.datahub.common.config.RulesConfig
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the check evaluation and the go/no-go verdict: the reconciliation
  * invariant counts unique business keys (the legacy contract deduplicates silently),
  * the reject-rate threshold, and the fail vs quarantine severity semantics.
  */
class DataQualitySpecs extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("dq-tests")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // in-process suites share one jvm: do NOT stop the session here - stopping it
    // poisons the shared context for the suites that follow. The jvm exits at the
    // end of the whole test run and cleans up.
    super.afterAll()
  }

  private def rawRows = {
    val s = spark
    import s.implicits._
    // 10 rows incl. 1 duplicate (T10 twice) and 1 dirty row (T9 negative)
    Seq(
      ("T1", "A1", "out", "100.0", "EUR"), ("T2", "A1", "out", "50.0", "EUR"),
      ("T3", "A2", "out", "70.0", "EUR"), ("T4", "A2", "in", "30.0", "EUR"),
      ("T5", "A1", "out", "20.0", "EUR"), ("T6", "A2", "in", "80.0", "EUR"),
      ("T7", "A1", "out", "10.0", "EUR"), ("T8", "A2", "in", "60.0", "EUR"),
      ("T9", "A1", "out", "-5.0", "EUR"), ("T10", "A2", "in", "40.0", "EUR"),
      ("T10", "A2", "in", "40.0", "EUR")
    ).toDF("txn_id", "account_id", "direction", "amount", "currency")
  }

  private def curatedRows = {
    val s = spark
    import s.implicits._
    // the 9 unique keys minus the dirty one (T9 -> rejects)
    Seq(
      ("T1", "A1", "100.0", "EUR"), ("T2", "A1", "50.0", "EUR"),
      ("T3", "A2", "70.0", "EUR"), ("T4", "A2", "30.0", "EUR"),
      ("T5", "A1", "20.0", "EUR"), ("T6", "A2", "80.0", "EUR"),
      ("T7", "A1", "10.0", "EUR"), ("T8", "A2", "60.0", "EUR"),
      ("T10", "A2", "40.0", "EUR")
    ).toDF("txn_id", "account_id", "amount_eur", "currency")
  }

  private def rejectRows = {
    val s = spark
    import s.implicits._
    Seq(("T9", "A1", "-5.0", "EUR", "invalid_amount"))
      .toDF("txn_id", "account_id", "amount", "currency", "reject_reason")
  }

  private def accounts = {
    val s = spark
    import s.implicits._
    Seq(
      ("A1", "CLT1", "GB82WEST12345698765432", "EUR", "active", true),
      ("A2", "CLT2", "GB27HB77120020020012345", "EUR", "active", true)
    ).toDF("account_id", "client_id", "iban", "currency", "status", "iban_valid")
  }

  private val maxRejectRate = 0.2

  test("reconciliation counts unique business keys: raw == curated + rejects") {
    val results = DataQualityJob.evaluate(rawRows, curatedRows, rejectRows, accounts, maxRejectRate)
    val recon = results.find(_.check.name == "raw_vs_curated_reconciliation").get
    // 11 raw rows, 10 unique keys (T10 twice), 9 curated + 1 reject = 10 unique ✓
    assert(recon.passed)
    assert(recon.observed == "curated=9 rejects=1 raw_unique=10")
  }

  test("reject rate compares against the configured threshold") {
    val results = DataQualityJob.evaluate(rawRows, curatedRows, rejectRows, accounts, maxRejectRate)
    val rate = results.find(_.check.name == "reject_rate").get
    // 1 reject / 11 raw rows = 0.0909 <= 0.2
    assert(rate.passed)
  }

  test("a failing fail-severity check flips the verdict to FAIL") {
    // curated missing T8: the reconciliation finds an unaccounted unique key
    val brokenCurated = curatedRows.filter(col("txn_id") =!= "T8")
    val results = DataQualityJob.evaluate(rawRows, brokenCurated, rejectRows, accounts, maxRejectRate)
    assert(DataQualityJob.verdictOf(results) == "FAIL")
    assert(results.exists(r => !r.passed && r.check.severity == DQCheck.Fail))
  }

  test("a quarantine-only failure keeps the verdict PASS") {
    // account flagged with an invalid iban: quarantine severity, publish not blocked
    val flaggedAccounts = accounts.withColumn("iban_valid", lit(false))
    val results = DataQualityJob.evaluate(rawRows, curatedRows, rejectRows, flaggedAccounts, maxRejectRate)
    val iban = results.find(_.check.name == "iban_validity").get
    assert(!iban.passed && iban.check.severity == DQCheck.Quarantine)
    val failedChecks = results.filter(r => !r.passed).map(r => s"${r.check.name}[${r.check.severity}]")
    assert(DataQualityJob.verdictOf(results) == "PASS", s"failed checks: ${failedChecks.mkString(", ")}")
  }

  test("dedup completeness catches duplicate business keys in the curated layer") {
    val dupCurated = curatedRows.union(curatedRows.filter(col("txn_id") === "T1"))
    val results = DataQualityJob.evaluate(rawRows, dupCurated, rejectRows, accounts, maxRejectRate)
    val dedup = results.find(_.check.name == "dedup_completeness").get
    assert(!dedup.passed)
  }
}
