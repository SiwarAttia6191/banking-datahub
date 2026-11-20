package io.github.siwarattia.datahub.common.config

import com.typesafe.config.{Config, ConfigFactory}

final case class SparkConfig(master: String)

final case class RulesConfig(
    volumeZscoreThreshold: Double,
    maxRejectRate: Double,
    baselineWindowDays: Int,
    baselineOffsetDays: Int,
    minVectorLength: Int,
    // typology rules - thresholds documented against public sources in
    // docs/detection-rules.md
    structuringFloorEur: Double,
    structuringCeilingEur: Double,
    structuringMinCount: Int,
    rapidMovementMinEur: Double,
    dormantIdleDays: Int,
    dormantMinEur: Double
)

final case class S3Config(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    rawBucket: String,
    curatedBucket: String,
    legacyBucket: String,
    pathStyleAccess: Boolean
)

final case class ServingConfig(url: String, user: String, password: String)

final case class PipelineConfig(environment: String, spark: SparkConfig, rules: RulesConfig,
                                s3: S3Config, serving: ServingConfig)

object PipelineConfig {

  /** Loads application.conf from the classpath. Environment differences are handled two ways:
    *  - a full config override: -Dconfig.file=/path/to/other.conf
    *  - single-value overrides via environment variables (see the ${?VAR} fallbacks in
    *    application.conf), which is how the docker-compose Spark container points at minio
    *    instead of localhost.
    */
  def load(): PipelineConfig = load(ConfigFactory.load())

  def load(config: Config): PipelineConfig = {
    val s3 = config.getConfig("datahub.s3")
    val spark = config.getConfig("datahub.spark")
    val rules = config.getConfig("datahub.rules")
    val serving = config.getConfig("datahub.serving")
    PipelineConfig(
      environment = config.getString("datahub.environment"),
      spark = SparkConfig(
        master = spark.getString("master")
      ),
      rules = RulesConfig(
        volumeZscoreThreshold = rules.getDouble("volume-zscore-threshold"),
        maxRejectRate = rules.getDouble("max-reject-rate"),
        baselineWindowDays = rules.getInt("baseline-window-days"),
        baselineOffsetDays = rules.getInt("baseline-offset-days"),
        minVectorLength = rules.getInt("min-vector-length"),
        structuringFloorEur = rules.getDouble("structuring-floor-eur"),
        structuringCeilingEur = rules.getDouble("structuring-ceiling-eur"),
        structuringMinCount = rules.getInt("structuring-min-count"),
        rapidMovementMinEur = rules.getDouble("rapid-movement-min-eur"),
        dormantIdleDays = rules.getInt("dormant-idle-days"),
        dormantMinEur = rules.getDouble("dormant-min-eur")
      ),
      serving = ServingConfig(
        url = serving.getString("url"),
        user = serving.getString("user"),
        password = serving.getString("password")
      ),
      s3 = S3Config(
        endpoint        = s3.getString("endpoint"),
        accessKey       = s3.getString("access-key"),
        secretKey       = s3.getString("secret-key"),
        rawBucket       = s3.getString("raw-bucket"),
        curatedBucket   = s3.getString("curated-bucket"),
        legacyBucket    = s3.getString("legacy-bucket"),
        pathStyleAccess = s3.getBoolean("path-style-access")
      )
    )
  }
}
