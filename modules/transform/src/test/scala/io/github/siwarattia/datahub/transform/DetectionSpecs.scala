package io.github.siwarattia.datahub.transform

import io.github.siwarattia.datahub.common.config.RulesConfig
import org.apache.spark.sql.functions.{col, to_date, lit}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the rule evaluation and the triage rollup: statistical alerts,
  * list-based alerts, the typology rules (structuring, rapid movement, dormant
  * reactivation), severity thresholds and the per-account risk rating.
  */
class DetectionSpecs extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("detection-tests")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // in-process suites share one jvm: do NOT stop the session here - stopping it
    // poisons the shared context for the suites that follow. The jvm exits at the
    // end of the whole test run and cleans up.
    super.afterAll()
  }

  private def rules = RulesConfig(
    volumeZscoreThreshold = 3.0, maxRejectRate = 0.05, baselineWindowDays = 31,
    baselineOffsetDays = 7, minVectorLength = 15, structuringFloorEur = 9000.0,
    structuringCeilingEur = 10000.0, structuringMinCount = 2, rapidMovementMinEur = 10000.0,
    dormantIdleDays = 30, dormantMinEur = 1000.0)

  private def noDormant = {
    val s = spark
    import s.implicits._
    Seq.empty[String].toDF("account_id")
  }

  private def featureRows = {
    val s = spark
    import s.implicits._
    Seq(
      // A1: log z-score 5.0 + boxplot outlier -> RULE_VOLUME_SPIKE, score 60*min(5/3, 2) = 100
      ("A1", "out", java.sql.Date.valueOf("2026-11-17"), 3, 1000.0, 3.01,
        5, 2.0, 0.5, 2.0, 2.0, 2.0, 5.0, true),
      // A2: quiet account - no statistical alert
      ("A2", "out", java.sql.Date.valueOf("2026-11-17"), 2, 90.0, 1.95,
        5, 1.95, 0.1, 1.9, 2.0, 2.0, 0.0, false)
    ).toDF("account_id", "direction", "business_date", "txn_count", "day_sum_eur", "day_log",
      "baseline_points", "baseline_mean_log", "baseline_std_log", "q1", "q3",
      "boxplot_upper", "zscore_day_log", "boxplot_outlier")
  }

  private def txnRows = {
    val s = spark
    import s.implicits._
    Seq(
      // A2 pays a sanctioned-jurisdiction counterparty -> RULE_RESTRICTED_COUNTRY
      ("T9", "A2", "out", 5000.0, "USD", "swift", "Shady Ltd", "ZZZZIR00", "IR",
        "2026-11-17", "2026-11-17T10:00:00Z"),
      // A1 pays a clean counterparty -> no list alert
      ("T8", "A1", "out", 100.0, "EUR", "sepa", "Clean SARL", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T10:00:00Z")
    ).toDF("txn_id", "account_id", "direction", "amount_eur", "currency", "channel",
      "counterparty_name", "counterparty_bic", "counterparty_country",
      "business_date", "txn_timestamp")
      .withColumn("business_date", to_date(col("business_date")))
  }

  private def restricted = {
    val s = spark
    import s.implicits._
    Seq(("IR", "sanctions_sim"), ("SY", "sanctions_sim")).toDF("country_code", "list_source")
  }

  private def accounts = {
    val s = spark
    import s.implicits._
    Seq(("A1", "CLT1"), ("A2", "CLT2")).toDF("account_id", "client_id")
  }

  private def clients = {
    val s = spark
    import s.implicits._
    Seq(("CLT1", "low"), ("CLT2", "medium")).toDF("client_id", "risk_level")
  }

  test("detect fires the volume spike rule with the severity threshold math") {
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, noDormant, rules)
    val spikes = alerts.filter(col("rule_id") === "RULE_VOLUME_SPIKE").collect()
    assert(spikes.length == 1)
    assert(spikes.head.getAs[String]("account_id") == "A1")
    // score = 60 * min(z / threshold, 2) = 60 * min(5/3, 2) = 100 -> critical
    assert(math.abs(spikes.head.getAs[Double]("score") - 100.0) < 1e-9)
    assert(spikes.head.getAs[String]("severity") == "critical")
  }

  test("detect fires the restricted country rule from the list join") {
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, noDormant, rules)
    val country = alerts.filter(col("rule_id") === "RULE_RESTRICTED_COUNTRY").collect()
    assert(country.length == 1)
    assert(country.head.getAs[String]("txn_id") == "T9")
    // sanctions_sim weighs more than the other lists: score 80 -> high
    assert(math.abs(country.head.getAs[Double]("score") - 80.0) < 1e-9)
    assert(country.head.getAs[String]("severity") == "high")
  }

  test("structuring: cash deposits under the reporting cap aggregate into one alert") {
    val s = spark
    import s.implicits._
    val cash = Seq(
      // three deposits inside the [9000, 10000) window -> the rule fires
      ("S1", "A2", "in", 9300.0, "EUR", "cash", "C1", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T09:15:00Z"),
      ("S2", "A2", "in", 9500.0, "EUR", "cash", "C2", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T11:40:00Z"),
      ("S3", "A2", "in", 9800.0, "EUR", "cash", "C3", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T16:05:00Z"),
      // at the ceiling: a single 11k deposit is NOT in the window (it would be
      // reportable on its own) and one deposit alone is below the min count
      ("S4", "A1", "in", 11000.0, "EUR", "cash", "C4", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T09:00:00Z"),
      ("S5", "A1", "in", 9500.0, "EUR", "cash", "C5", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T09:30:00Z"),
      // a sepa credit in the window is not a cash deposit
      ("S6", "A1", "in", 9600.0, "EUR", "sepa", "C6", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T10:00:00Z")
    ).toDF("txn_id", "account_id", "direction", "amount_eur", "currency", "channel",
      "counterparty_name", "counterparty_bic", "counterparty_country",
      "business_date", "txn_timestamp")
      .withColumn("business_date", to_date(col("business_date")))

    val alerts = DetectionJob.detect(featureRows, cash, restricted, noDormant, rules)
    val hits = alerts.filter(col("rule_id") === "RULE_STRUCTURING").collect()
    assert(hits.length == 1)
    assert(hits.head.getAs[String]("account_id") == "A2")
    // 9300 + 9500 + 9800
    assert(math.abs(hits.head.getAs[Double]("amount_eur") - 28600.0) < 1e-9)
    // score 85 -> high
    assert(hits.head.getAs[String]("severity") == "high")
  }

  test("rapid movement: large in and out the same day alerts, one direction alone does not") {
    val s = spark
    import s.implicits._
    val passThrough = featureRows.union(Seq(
      ("A2", "in", java.sql.Date.valueOf("2026-11-17"), 4, 15000.0, 2.0,
        5, 1.95, 0.1, 1.9, 2.0, 2.0, 0.0, false)
    ).toDF("account_id", "direction", "business_date", "txn_count", "day_sum_eur", "day_log",
      "baseline_points", "baseline_mean_log", "baseline_std_log", "q1", "q3",
      "boxplot_upper", "zscore_day_log", "boxplot_outlier"))

    val alerts = DetectionJob.detect(passThrough, txnRows, restricted, noDormant, rules)
    val rapid = alerts.filter(col("rule_id") === "RULE_RAPID_MOVEMENT").collect()
    // A2: in 15000 + out 90 -> the out leg is below the threshold, no alert.
    // (featureRows has A2 out 90 - not a pass-through pair)
    assert(rapid.length == 0)

    val both = passThrough.union(Seq(
      ("A2", "out", java.sql.Date.valueOf("2026-11-17"), 3, 14500.0, 1.95,
        5, 1.95, 0.1, 1.9, 2.0, 2.0, 0.0, false)
    ).toDF("account_id", "direction", "business_date", "txn_count", "day_sum_eur", "day_log",
      "baseline_points", "baseline_mean_log", "baseline_std_log", "q1", "q3",
      "boxplot_upper", "zscore_day_log", "boxplot_outlier"))
    val alerts2 = DetectionJob.detect(both, txnRows, restricted, noDormant, rules)
    val rapid2 = alerts2.filter(col("rule_id") === "RULE_RAPID_MOVEMENT").collect()
    assert(rapid2.length == 1)
    assert(rapid2.head.getAs[String]("account_id") == "A2")
    // 15000 + 14500
    assert(math.abs(rapid2.head.getAs[Double]("amount_eur") - 29500.0) < 1e-9)
    assert(rapid2.head.getAs[String]("severity") == "high")
  }

  test("dormant reactivation: only accounts beyond the idle window alert") {
    val s = spark
    import s.implicits._
    val dormant = Seq("A2").toDF("account_id")
    val waking = Seq(
      // A2 (dormant) moves 3000 + 6000 -> total above the floor -> one alert
      ("D1", "A2", "in", 3000.0, "EUR", "sepa", "W1", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T10:00:00Z"),
      ("D2", "A2", "in", 6000.0, "EUR", "sepa", "W2", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T12:00:00Z"),
      // A1 is not dormant -> no alert even with a larger sum
      ("D3", "A1", "in", 9000.0, "EUR", "sepa", "W3", "ZZZZFR10", "FR",
        "2026-11-17", "2026-11-17T12:30:00Z")
    ).toDF("txn_id", "account_id", "direction", "amount_eur", "currency", "channel",
      "counterparty_name", "counterparty_bic", "counterparty_country",
      "business_date", "txn_timestamp")
      .withColumn("business_date", to_date(col("business_date")))

    val alerts = DetectionJob.detect(featureRows, waking, restricted, dormant, rules)
    val hits = alerts.filter(col("rule_id") === "RULE_DORMANT_REACTIVATION").collect()
    assert(hits.length == 1)
    assert(hits.head.getAs[String]("account_id") == "A2")
    assert(math.abs(hits.head.getAs[Double]("amount_eur") - 9000.0) < 1e-9)
    // score 55 -> medium
    assert(hits.head.getAs[String]("severity") == "medium")
  }

  test("triage combines the day's signal with the client baseline risk") {
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, noDormant, rules)
    val ratings = DetectionJob.triage(alerts, accounts, clients).collect()
      .map(r => (r.getAs[String]("account_id"), r.getAs[String]("final_rating")))
      .toMap
    // A1: critical day signal (score 100) - wins over the low-risk baseline
    assert(ratings("A1") == "critical")
    // A2: high day signal (score 80) over a medium-risk baseline
    assert(ratings("A2") == "high")
  }

  test("quiet groups produce no alerts and no ratings") {
    val quietFeatures = featureRows.filter(col("account_id") === "A2")
    val quietTxns = txnRows.filter(col("counterparty_country") === "FR")
    val alerts = DetectionJob.detect(quietFeatures, quietTxns, restricted, noDormant, rules)
    assert(alerts.count() == 0)
    val ratings = DetectionJob.triage(alerts, accounts, clients)
    assert(ratings.count() == 0)
  }
}
