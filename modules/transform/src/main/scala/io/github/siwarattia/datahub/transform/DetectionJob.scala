package io.github.siwarattia.datahub.transform

import io.github.siwarattia.datahub.common.config.PipelineConfig
import io.github.siwarattia.datahub.common.config.RulesConfig
import io.github.siwarattia.datahub.common.io.S3Locations
import io.github.siwarattia.datahub.common.spark.SparkSessions
import io.github.siwarattia.datahub.common.util.Args
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/** Detection rules -> alerts -> triage scoring -> per-account risk rating.
  *
  * The rules consume the same curated features an external ML engine would - detection
  * happens entirely in-house, with no model dependency, and every alert carries the
  * signal that triggered it (fully explainable to the investigating analyst).
  *
  * Two rule families, deliberately simple but complete (sources per rule in
  * docs/detection-rules.md):
  *  - RULE_VOLUME_SPIKE (statistical, per account-day): the day's volume is a z-score
  *    outlier or a boxplot outlier - abnormal transaction volumes
  *  - RULE_RESTRICTED_COUNTRY (list-based, per transaction): counterparty country on a
  *    restricted list (FATF (Financial Action Task Force) high-risk jurisdictions and
  *    sanctions screening - the generator grounds the lists in the public FATF snapshot)
  *  - RULE_STRUCTURING (typology, per account-day): several cash deposits just under
  *    the reporting threshold on the same day - the classic reporting-cap evasion
  *  - RULE_RAPID_MOVEMENT (typology, per account-day): a large amount arrives and
  *    leaves the same account the same day (pass-through / layering)
  *  - RULE_DORMANT_REACTIVATION (typology, per account-day): an account idle for
  *    a long period suddenly moves money
  *
  * The rule logic lives in `detect` and the rollup in `triage` - pure functions
  * (DataFrames in, DataFrames out) so they can be unit tested without storage.
  */
object DetectionJob {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val opts = Args.parse(args)
    val date = opts.getOrElse("date",
      throw new IllegalArgumentException("usage: DetectionJob --date yyyy-MM-dd"))

    val config = PipelineConfig.load()
    val spark = SparkSessions.app(config, s"detection-$date", opts.get("master"))

    try run(spark, config, date)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: PipelineConfig, date: String): Unit = {
    // read the base path, not the partition directory: a partition dir read does not
    // carry the partition column (it lives in the folder name), and downstream rules
    // filter on it
    val features = spark.read.parquet(S3Locations.curatedDomain(config, "account_day_features"))
      .filter(col("business_date") === lit(date).cast("date"))
    val txns = spark.read.parquet(S3Locations.curatedDomain(config, "transactions"))
      .filter(col("business_date") === lit(date).cast("date"))
    val restrictedCodes = spark.read.parquet(S3Locations.curatedDomain(config, "restricted_countries"))
      .select(col("country_code"), col("country_name"), col("list_source"))
    val accounts = spark.read.parquet(S3Locations.curatedDomain(config, "accounts"))
      .select(col("account_id"), col("client_id"))
    val clients = spark.read.parquet(S3Locations.curatedDomain(config, "clients"))
      .select(col("client_id"), col("risk_level"))
    // dormancy needs history: the last activity strictly BEFORE the evaluated day.
    // accounts with NO history at all are dormant too (never used) - so the last-active
    // aggregation is left-joined onto the account dimension, not the other way round
    val history = spark.read.parquet(S3Locations.curatedDomain(config, "transactions"))
      .filter(col("business_date") < lit(date).cast("date"))
    val lastActive = history.groupBy(col("account_id"))
      .agg(max(col("business_date")).as("last_active"))
    val dormant = spark.read.parquet(S3Locations.curatedDomain(config, "accounts"))
      .select(col("account_id"))
      .join(lastActive, Seq("account_id"), "left")
      .filter(col("last_active").isNull ||
        col("last_active") < date_sub(lit(date).cast("date"), config.rules.dormantIdleDays))
      .select(col("account_id"))

    val alerts = detect(features, txns, restrictedCodes, dormant, config.rules)
    val ratings = triage(alerts, accounts, clients)

    alerts.write.mode("overwrite").partitionBy("business_date")
      .parquet(S3Locations.curatedDomain(config, "alerts"))
    ratings.write.mode("overwrite").partitionBy("business_date")
      .parquet(S3Locations.curatedDomain(config, "risk_ratings"))

    log.info(s"[$date] alerts: ${alerts.count()}, risk ratings: ${ratings.count()}")
  }

  /** Pure rule evaluation: returns the alert stream (rule_id, business_date, account_id,
    * txn_id, amount_eur, detail, score, severity). Deterministic alert ids (sha2 of the
    * identity), weighted scores and severity thresholds. `dormant` carries the accounts
    * idle beyond the configured window (computed in run() from curated history).
    */
  def detect(features: DataFrame, txns: DataFrame, restrictedCodes: DataFrame,
             dormant: DataFrame, rules: RulesConfig): DataFrame = {
    val zThreshold = rules.volumeZscoreThreshold
    val spikeAlerts = features
      .filter(col("zscore_day_log") > zThreshold || col("boxplot_outlier"))
      .select(
        lit("RULE_VOLUME_SPIKE").as("rule_id"),
        col("business_date"), col("account_id"),
        lit(null).cast("string").as("txn_id"),
        col("day_sum_eur").as("amount_eur"),
        concat(lit("log-zscore="),
          round(coalesce(col("zscore_day_log"), lit(0.0)), 2).cast("string")).as("detail"),
        (lit(60.0) * least(coalesce(col("zscore_day_log"), lit(0.0)) / lit(zThreshold), lit(2.0))).as("score"))

    val countryAlerts = txns
      .join(broadcast(restrictedCodes), col("counterparty_country") === col("country_code"))
      .select(
        lit("RULE_RESTRICTED_COUNTRY").as("rule_id"),
        col("business_date"), col("account_id"), col("txn_id"),
        col("amount_eur"),
        concat(lit("list="), col("list_source"), lit(" country="),
          col("country_name")).as("detail"),
        when(col("list_source") === "sanctions_sim", lit(80.0)).otherwise(lit(65.0)).as("score"))

    // several cash deposits just under the reporting threshold, same account, same day:
    // each deposit alone is reportable-adjacent, together they cross the cap
    val structuringAlerts = txns
      .filter(col("channel") === "cash" && col("direction") === "in")
      .filter(col("amount_eur") >= rules.structuringFloorEur &&
        col("amount_eur") < rules.structuringCeilingEur)
      .groupBy(col("account_id"), col("business_date"))
      .agg(count(lit(1)).as("n"), sum(col("amount_eur")).as("total"))
      .filter(col("n") >= rules.structuringMinCount)
      .select(
        lit("RULE_STRUCTURING").as("rule_id"),
        col("business_date"), col("account_id"),
        lit(null).cast("string").as("txn_id"),
        col("total").as("amount_eur"),
        concat(lit("cash deposits in ["), lit(rules.structuringFloorEur).cast("string"),
          lit(","), lit(rules.structuringCeilingEur).cast("string"), lit("): n="),
          col("n").cast("string")).as("detail"),
        lit(85.0).as("score"))

    // a large amount in AND a large amount out on the same account-day: the funds
    // pass through rather than stay (layering / pass-through typology)
    val inbound = features.filter(col("direction") === "in")
      .select(col("account_id"), col("business_date"), col("day_sum_eur").as("in_sum"))
    val outbound = features.filter(col("direction") === "out")
      .select(col("account_id"), col("business_date"), col("day_sum_eur").as("out_sum"))
    val rapidAlerts = inbound.join(outbound, Seq("account_id", "business_date"))
      .filter(col("in_sum") >= rules.rapidMovementMinEur &&
        col("out_sum") >= rules.rapidMovementMinEur)
      .select(
        lit("RULE_RAPID_MOVEMENT").as("rule_id"),
        col("business_date"), col("account_id"),
        lit(null).cast("string").as("txn_id"),
        col("in_sum").plus(col("out_sum")).as("amount_eur"),
        concat(lit("in="), col("in_sum").cast("string"),
          lit(" out="), col("out_sum").cast("string")).as("detail"),
        lit(75.0).as("score"))

    // first movement after a long idle period: a sleeping account waking up with
    // a transfer is a classic reactivation scenario
    val dormantAlerts = txns
      .join(broadcast(dormant), Seq("account_id"))
      .groupBy(col("account_id"), col("business_date"))
      .agg(sum(col("amount_eur")).as("total"), count(lit(1)).as("n"))
      .filter(col("total") >= rules.dormantMinEur)
      .select(
        lit("RULE_DORMANT_REACTIVATION").as("rule_id"),
        col("business_date"), col("account_id"),
        lit(null).cast("string").as("txn_id"),
        col("total").as("amount_eur"),
        concat(lit("idle > "), lit(rules.dormantIdleDays).cast("string"),
          lit("d, n="), col("n").cast("string")).as("detail"),
        lit(55.0).as("score"))

    spikeAlerts
      .unionByName(countryAlerts)
      .unionByName(structuringAlerts)
      .unionByName(rapidAlerts)
      .unionByName(dormantAlerts)
      .withColumn("alert_id",
        substring(sha2(concat_ws("|", col("rule_id"),
          coalesce(col("txn_id"), lit("-")), col("account_id"), col("business_date")), 256), 1, 16))
      .withColumn("severity",
        when(col("score") >= 100, "critical")
          .when(col("score") >= 70, "high")
          .when(col("score") >= 40, "medium")
          .otherwise("low"))
  }

  /** Triage rollup: per account-day, the strongest alert score + the client's baseline
    * risk level -> the final aml risk rating.
    */
  def triage(alerts: DataFrame, accounts: DataFrame, clients: DataFrame): DataFrame = {
    alerts.groupBy("account_id", "business_date")
      .agg(
        max("score").as("max_alert_score"),
        count(lit(1)).as("alert_count"))
      .join(accounts, Seq("account_id"), "left")
      .join(clients, Seq("client_id"), "left")
      .withColumn("aml_risk_rating",
        when(col("max_alert_score") >= 100, "critical")
          .when(col("max_alert_score") >= 70, "high")
          .when(col("max_alert_score") >= 40, "medium")
          .otherwise("low"))
      .withColumn("final_rating",
        when(col("aml_risk_rating").isin("critical", "high"), col("aml_risk_rating"))
          .when(col("risk_level") === "high", lit("high"))
          .when(col("aml_risk_rating") === "medium", lit("medium"))
          .otherwise(lit("low")))
  }
}
