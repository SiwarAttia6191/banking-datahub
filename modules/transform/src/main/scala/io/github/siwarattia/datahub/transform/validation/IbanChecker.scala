package io.github.siwarattia.datahub.transform.validation

import org.apache.spark.sql.functions.udf

/** ISO-13616 mod-97 IBAN check-digit validation.
  *
  * A UDF is the honest tool here: there is no built-in expression for mod-97 over a
  * normalized string, and this is one of the few places where the Catalyst cost of a
  * UDF is acceptable. Everything else in the curated jobs uses built-in functions.
  */
object IbanChecker {

  /** true when the check digits recomputed from the body match the stored ones. */
  val isValid: (String => Boolean) = (iban: String) => {
    def numeric(c: Char): Int = Character.getNumericValue(c)

    if (iban == null || iban.length < 5) false
    else if (!iban.take(2).forall(_.isLetter)) false
    else if (!iban.substring(2, 4).forall(_.isDigit)) false
    else {
      // same arithmetic as the generator: body + country + "00", mod 97
      val mod97 = (iban.substring(4) + iban.take(2) + "00")
        .foldLeft(0L) { (rem, c) =>
          val v = numeric(c)
          if (v < 10) (rem * 10 + v) % 97
          else (rem * 100 + v) % 97
        }
      f"${98 - mod97}%02d" == iban.substring(2, 4)
    }
  }

  val isValidUdf = udf(isValid)
}
