package io.github.siwarattia.datahub.transform

import io.github.siwarattia.datahub.common.config.RulesConfig
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the curated transformation and the feature computation:
  * in-memory fixtures, local Spark session, no storage, no cluster.
  *
  * The curated fixtures deliberately include every dirty pattern the generator
  * injects (duplicates, negative amounts, unknown currency, ghost accounts) plus
  * the fx fallback case (a value date with no rate of its own).
  */
class TransformSpecs extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("transform-tests")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // in-process suites share one jvm: do NOT stop the session here - stopping it
    // poisons the shared context for the suites that follow. The jvm exits at the
    // end of the whole test run and cleans up.
    super.afterAll()
  }

  private def txnRows = {
    val s = spark
    import s.implicits._
    Seq(
      // valid eur row with its own day rate
      ("T1", "A1", "out", "100.0", "EUR", "sepa", "Acme SARL", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T10:00:00Z"),
      // duplicate business key: the later timestamp (amount 55) must win
      ("T2", "A1", "out", "50.0", "USD", "swift", "Foo LLC", "ZZZZUS20", "US",
        "2026-11-17", "2026-11-17T11:00:00Z"),
      ("T2", "A1", "out", "55.0", "USD", "swift", "Foo LLC", "ZZZZUS20", "US",
        "2026-11-17", "2026-11-17T12:00:00Z"),
      // dirty rows, one reject reason each
      ("T3", "A1", "out", "-10.0", "EUR", "domestic", "Bad", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T13:00:00Z"),
      ("T4", "A1", "out", "20.0", "XX", "swift", "Odd", "ZZZZXX00", "XX",
        "2026-11-17", "2026-11-17T14:00:00Z"),
      ("T5", "GHOST", "out", "30.0", "EUR", "domestic", "Ghost", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T15:00:00Z"),
      // usd row on a date with no rate of its own -> fallback (latest available: 1.1)
      ("T6", "A1", "out", "80.0", "USD", "swift", "Fallback", "ZZZZUS20", "US",
        "2026-11-16", "2026-11-16T09:00:00Z")
    ).toDF("txn_id", "account_id", "direction", "amount", "currency", "channel",
      "counterparty_name", "counterparty_bic", "counterparty_country",
      "value_date", "txn_timestamp")
  }

  private def rates = {
    val s = spark
    import s.implicits._
    Seq(
      ("2026-11-17", "EUR", 1.0),
      ("2026-11-17", "USD", 1.1),
      ("2026-11-15", "USD", 1.2)
    ).toDF("rate_date", "quote_ccy", "rate")
  }

  private def accountsDf = {
    val s = spark
    import s.implicits._
    Seq(("A1", "CLT1", "GB82WEST12345698765432", "EUR", "active"))
      .toDF("account_id", "client_id", "iban", "currency", "status")
  }

  // ---- curated layer -------------------------------------------------------

  test("curate keeps the latest version of a duplicated business key") {
    val (curated, _) = CuratedTransactionsJob.curate(txnRows, rates, accountsDf)
    println("DEBUG curated rows:")
    curated.orderBy("txn_id").collect().foreach(r => println("  " + r.mkString(",")))
    val t2 = curated.filter(col("txn_id") === "T2").collect()
    assert(t2.length == 1)
    assert(math.abs(t2.head.getAs[Double]("amount") - 55.0) < 1e-9)
  }

  test("curate routes each dirty row to the rejects path with a reason") {
    val (_, rejects) = CuratedTransactionsJob.curate(txnRows, rates, accountsDf)
    val reasons = rejects.collect()
      .map(r => (r.getAs[String]("txn_id"), r.getAs[String]("reject_reason")))
      .toMap
    assert(reasons("T3") == "invalid_amount")
    assert(reasons("T4") == "unknown_currency")
    assert(reasons("T5") == "missing_account")
    assert(rejects.count() == 3)
  }

  test("curate converts with the day rate and falls back when the day has no rate") {
    val (curated, _) = CuratedTransactionsJob.curate(txnRows, rates, accountsDf)
    println("DEBUG fx rows:")
    curated.orderBy("txn_id").collect().foreach(r => println("  " + r.mkString(",")))
    val rows = curated.collect()
      .map(r => (r.getAs[String]("txn_id"),
        (r.getAs[Double]("amount_eur"), r.getAs[Boolean]("fx_fallback_used"))))
      .toMap
    // eur: rate 1.0
    assert(math.abs(rows("T1")._1 - 100.0) < 1e-9)
    // usd on 2026-11-17: day rate 1.1 -> 55 / 1.1 = 50
    assert(math.abs(rows("T2")._1 - 50.0) < 1e-9)
    assert(!rows("T2")._2)
    // usd on 2026-11-16: no day rate -> fallback to the latest available rate (1.1)
    assert(math.abs(rows("T6")._1 - 80.0 / 1.1) < 1e-6)
    assert(rows("T6")._2)
  }

  // ---- feature computation --------------------------------------------------

  // statistical knobs tuned for a tiny fixture: same code path as production,
  // only the numbers differ (that is the point of them being configuration)
  private val rules = RulesConfig(
    volumeZscoreThreshold = 3.0,
    maxRejectRate = 0.05,
    baselineWindowDays = 31,
    baselineOffsetDays = 1,
    minVectorLength = 2)

  private def curatedLike(rows: Seq[(String, String, String, Double)]) = {
    val s = spark
    import s.implicits._
    rows.toDF("account_id", "direction", "business_date", "amount_eur")
      .withColumn("business_date", to_date(col("business_date")))
  }

  test("computeFeatures produces a null signal when history is insufficient") {
    val txns = curatedLike(Seq(("A1", "out", "2026-01-01", 100.0)))
    val features = AccountFeaturesJob.computeFeatures(txns, rules)
    println("DEBUG single-day features:")
    features.collect().foreach(r => println("  " + r.mkString(",")))
    val row = features.filter(col("business_date") === "2026-01-01").collect().head
    // no baseline at all: no z-score, no false positive
    assert(row.getAs[Any]("zscore_day_log") == null)
  }

  test("computeFeatures flags a volume spike above the z-score threshold") {
    val txns = curatedLike(Seq(
      ("A1", "out", "2026-01-01", 100.0),
      ("A1", "out", "2026-01-02", 200.0),
      ("A1", "out", "2026-01-03", 300.0),
      ("A1", "out", "2026-01-04", 400.0),
      ("A1", "out", "2026-01-05", 500.0),
      ("A1", "out", "2026-01-06", 5000.0)))
    val features = AccountFeaturesJob.computeFeatures(txns, rules)
    println("DEBUG feature rows:")
    features.orderBy("business_date").collect().foreach(r => println("  " + r.mkString(",")))
    val spike = features.filter(col("business_date") === "2026-01-06").collect().head
    assert(spike.getAs[Double]("zscore_day_log") > 3.0)
    assert(spike.getAs[Boolean]("boxplot_outlier"))
  }
}
