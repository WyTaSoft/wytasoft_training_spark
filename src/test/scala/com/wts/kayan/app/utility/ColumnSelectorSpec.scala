package com.wts.kayan.app.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for [[ColumnSelector]]. These tests are pure (no Spark session required)
 * and verify the column projection returned for each known dataset.
 *
 * @author Mehdi TAJMOUATI
 */
class ColumnSelectorSpec extends AnyFlatSpec with Matchers {

  "getColumnSequence" should "return the order columns in the expected order" in {
    ColumnSelector.getColumnSequence(PrimaryConstants.ORDERS) shouldBe
      Array("orderid", "clientid", "amount", "date")
  }

  it should "return the client columns in the expected order" in {
    ColumnSelector.getColumnSequence(PrimaryConstants.CLIENTS) shouldBe
      Array("clientid", "name", "location")
  }

  it should "be case-insensitive on the table name" in {
    ColumnSelector.getColumnSequence("ORDERS") shouldBe
      ColumnSelector.getColumnSequence("orders")
  }

  it should "throw a MatchError for an unknown table name" in {
    a[MatchError] should be thrownBy ColumnSelector.getColumnSequence("unknown_table")
  }
}
