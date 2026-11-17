package io.github.siwarattia.datahub.quality

/** The data-quality model: small on purpose, so every mechanic is visible and testable.
  *
  * severity:
  *  - "fail"       : a failing check blocks the publish decision (go/no-go)
  *  - "quarantine" : a failing check documents rows already routed to rejects/flags
  */
object DQCheck {
  val Fail = "fail"
  val Quarantine = "quarantine"
}

final case class DQCheck(name: String, severity: String)

final case class CheckResult(check: DQCheck, passed: Boolean, observed: String, expected: String)

final case class DQReport(business_date: String, verdict: String, results: Seq[CheckResult])
