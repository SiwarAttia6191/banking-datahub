package io.github.siwarattia.datahub.transform

import io.github.siwarattia.datahub.common.config.RulesConfig
import org.apache.spark.sql.functions.{col, to_date, lit}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the rule evaluation and the triage rollup: statistical alerts,
  * list-based alerts, severity thresholds and the per-account risk rating.
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
      ("T9", "A2", "out", "5000.0", "USD", "swift", "Shady Ltd", "ZZZZIR00", "IR",
        "2026-11-17", "2026-11-17T10:00:00Z"),
      // A1 pays a clean counterparty -> no list alert
      ("T8", "A1", "out", "100.0", "EUR", "sepa", "Clean SARL", "ZZZZFR10", "FR",
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
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, zThreshold = 3.0)
    val spikes = alerts.filter(col("rule_id") === "RULE_VOLUME_SPIKE").collect()
    assert(spikes.length == 1)
    assert(spikes.head.getAs[String]("account_id") == "A1")
    // score = 60 * min(z / threshold, 2) = 60 * min(5/3, 2) = 100 -> critical
    assert(math.abs(spikes.head.getAs[Double]("score") - 100.0) < 1e-9)
    assert(spikes.head.getAs[String]("severity") == "critical")
  }

  test("detect fires the restricted country rule from the list join") {
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, zThreshold = 3.0)
    val country = alerts.filter(col("rule_id") === "RULE_RESTRICTED_COUNTRY").collect()
    assert(country.length == 1)
    assert(country.head.getAs[String]("txn_id") == "T9")
    // sanctions_sim weighs more than the internal watchlist: score 80 -> high
    assert(math.abs(country.head.getAs[Double]("score") - 80.0) < 1e-9)
    assert(country.head.getAs[String]("severity") == "high")
  }

  test("triage combines the day's signal with the client baseline risk") {
    val alerts = DetectionJob.detect(featureRows, txnRows, restricted, zThreshold = 3.0)
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
    val alerts = DetectionJob.detect(quietFeatures, quietTxns, restricted, zThreshold = 3.0)
    assert(alerts.count() == 0)
    val ratings = DetectionJob.triage(alerts, accounts, clients)
    assert(ratings.count() == 0)
  }
}
