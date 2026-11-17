package io.github.siwarattia.datahub.common.io

import io.github.siwarattia.datahub.common.config.PipelineConfig

/** Layout of the lake on object storage.
  *
  * inbox/   : where source feeds land before ingestion (uploaded by scripts/upload_sources.py)
  * raw/     : data exactly as landed, partitioned by ingestion_date, immutable
  * curated/ : validated and conformed data, partitioned by business_date
  *
  * Jobs write to the base path with the partition column added on write, so a rerun
  * replaces exactly one partition (dynamic overwrite) and touches nothing else.
  */
object S3Locations {

  /** inbox: source feeds before ingestion */
  def inbox(cfg: PipelineConfig, domain: String): String =
    s"s3a://${cfg.s3.rawBucket}/inbox/$domain"

  def inboxDaily(cfg: PipelineConfig, domain: String, businessDate: String): String =
    s"${inbox(cfg, domain)}/business_date=$businessDate"

  /** base path of a domain in the raw zone */
  def rawDomain(cfg: PipelineConfig, domain: String): String =
    s"s3a://${cfg.s3.rawBucket}/raw/$domain"

  /** read path for one raw partition */
  def rawPartition(cfg: PipelineConfig, domain: String, ingestionDate: String): String =
    s"${rawDomain(cfg, domain)}/ingestion_date=$ingestionDate"

  /** base path of a domain in the curated zone */
  def curatedDomain(cfg: PipelineConfig, domain: String): String =
    s"s3a://${cfg.s3.curatedBucket}/curated/$domain"

  /** read path for one curated partition */
  def curatedPartition(cfg: PipelineConfig, domain: String, businessDate: String): String =
    s"${curatedDomain(cfg, domain)}/business_date=$businessDate"

  /** rows that failed the curated business rules, kept for audit and reprocessing */
  def rejectsDomain(cfg: PipelineConfig, domain: String): String =
    s"s3a://${cfg.s3.curatedBucket}/rejects/$domain"

  /** read path for one rejects partition */
  def rejectsPartition(cfg: PipelineConfig, domain: String, businessDate: String): String =
    s"${rejectsDomain(cfg, domain)}/business_date=$businessDate"

  /** per-run data-quality reports (json) */
  def dqReports(cfg: PipelineConfig): String =
    s"s3a://${cfg.s3.curatedBucket}/reports/dq"
}
