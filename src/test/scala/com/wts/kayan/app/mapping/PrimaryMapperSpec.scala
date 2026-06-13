package com.wts.kayan.app.mapping

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Spark-based tests for [[PrimaryMapper]] which exercise the temporary-view registration
 * and the left-join enrichment defined in [[PrimaryView]].
 *
 * Uses spark-testing-base's [[DataFrameSuiteBase]] to provide a local SparkSession.
 *
 * @author Mehdi TAJMOUATI
 */
class PrimaryMapperSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  private def clientsDf(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._
    Seq(
      (1, "Alice", "NYC"),
      (2, "Bob", "LA")
    ).toDF("clientId", "name", "location")
  }

  private def ordersDf(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._
    Seq(
      (100, 1, 50.0, "2023-01-01"),
      (101, 3, 75.0, "2023-01-01") // clientId 3 has no matching client
    ).toDF("orderId", "clientId", "amount", "date")
  }

  "enrichDataFrame" should "project the expected enriched columns" in {
    implicit val ss: SparkSession = spark
    val result = new PrimaryMapper(clientsDf, ordersDf).enrichDataFrame

    result.columns should contain theSameElementsAs
      Seq("clientId", "name", "location", "orderId", "amount", "date")
  }

  it should "keep every order (left join) and null client fields when unmatched" in {
    implicit val ss: SparkSession = spark
    val result = new PrimaryMapper(clientsDf, ordersDf).enrichDataFrame

    result.count() shouldBe 2

    val byOrder = result.collect().map(r => r.getAs[Int]("orderId") -> r).toMap

    byOrder(100).getAs[String]("name") shouldBe "Alice"
    byOrder(100).getAs[String]("location") shouldBe "NYC"

    // Unmatched order: client fields are null under the left join.
    byOrder(101).getAs[String]("name") shouldBe null
  }
}
