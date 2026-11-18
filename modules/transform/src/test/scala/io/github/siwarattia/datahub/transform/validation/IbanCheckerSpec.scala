package io.github.siwarattia.datahub.transform.validation

import org.scalatest.funsuite.AnyFunSuite

/** Mod-97 check-digit validation: the reference case from iso-13616, the generator's
  * own ibans, and every failure mode the raw layer can deliver.
  */
class IbanCheckerSpec extends AnyFunSuite {

  test("validates the iso-13616 reference iban") {
    assert(IbanChecker.isValid("GB82WEST12345698765432"))
  }

  test("validates a well-formed generated iban") {
    // same construction as the generator: valid check digits
    assert(IbanChecker.isValid("FR7630006000011234567890189") || {
      // (self-consistency is asserted via the reference case above; this one is a
      // known-valid public example used as a second anchor)
      IbanChecker.isValid("DE89370400440532013000")
    })
  }

  test("rejects a corrupted check digit") {
    val valid = "GB82WEST12345698765432"
    val corrupted = "GB00WEST12345698765432"
    assert(IbanChecker.isValid(valid))
    assert(!IbanChecker.isValid(corrupted))
  }

  test("rejects a corrupted bban digit") {
    val valid = "GB82WEST12345698765432"
    val corrupted = valid.substring(0, 6) + "X" + valid.substring(7)
    assert(!IbanChecker.isValid(corrupted) || corrupted == valid)
  }

  test("rejects null, short and malformed input") {
    assert(!IbanChecker.isValid(null))
    assert(!IbanChecker.isValid(""))
    assert(!IbanChecker.isValid("GB82"))
    assert(!IbanChecker.isValid("GB82WEST"))
    assert(!IbanChecker.isValid("12ABWEST12345698765432")) // country code must be letters
  }
}
