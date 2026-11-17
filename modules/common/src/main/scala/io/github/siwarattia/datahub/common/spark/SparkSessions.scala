package io.github.siwarattia.datahub.common.spark

import io.github.siwarattia.datahub.common.config.PipelineConfig
import org.apache.spark.sql.SparkSession

/** Central SparkSession factory for all jobs.
  *
  * Choices worth being able to explain:
  *  - s3a settings are passed as spark.hadoop.* configs so they land in the Hadoop
  *    filesystem layer; bare fs.s3a.* keys set on the session builder do not propagate.
  *  - dynamic partition overwrite is set once here: every write in overwrite mode then
  *    replaces only the partitions present in the DataFrame, which is what makes
  *    single-date reruns safe.
  *  - the master comes from config (local[*] by default, SPARK_MASTER env var or --master
  *    to override) so the same code runs locally and on a cluster.
  */
object SparkSessions {

  def app(config: PipelineConfig, appName: String, masterOverride: Option[String] = None): SparkSession = {
    val s3 = config.s3
    SparkSession.builder()
      .appName(appName)
      .master(masterOverride.getOrElse(config.spark.master))
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.fs.s3a.endpoint", s3.endpoint)
      .config("spark.hadoop.fs.s3a.access.key", s3.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", s3.secretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", s3.pathStyleAccess.toString)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
  }
}
