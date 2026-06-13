package com.wts.kayan.app.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the pure date-handling helpers in [[PrimaryUtilities]].
 * These do not require a Spark session.
 *
 * @author Mehdi TAJMOUATI
 */
class PrimaryUtilitiesSpec extends AnyFlatSpec with Matchers {

  "convertStringToDate / dateToStrNdReformat" should "round-trip a date in the same format" in {
    val date = PrimaryUtilities.convertStringToDate("2023-05-20", "yyyy-MM-dd")
    PrimaryUtilities.dateToStrNdReformat(date, "yyyy-MM-dd") shouldBe "2023-05-20"
  }

  it should "reformat a date into a different pattern" in {
    val date = PrimaryUtilities.convertStringToDate("2023-05-20", "yyyy-MM-dd")
    PrimaryUtilities.dateToStrNdReformat(date, "dd/MM/yyyy") shouldBe "20/05/2023"
  }

  it should "preserve ordering so the max partition can be selected" in {
    val d1 = PrimaryUtilities.convertStringToDate("2023-01-01", "yyyy-MM-dd")
    val d2 = PrimaryUtilities.convertStringToDate("2023-09-20", "yyyy-MM-dd")
    Seq(d1, d2).max shouldBe d2
  }
}
