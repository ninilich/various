package myсapstoneproject

import org.scalatest.funsuite.AnyFunSuite
import myсapstoneproject.Helpers._
import org.apache.spark.sql.SparkSession

class MainTest extends AnyFunSuite {

  val clicksPath = getClass.getResource("/clicks.csv").getPath
  val purchasesPath = getClass.getResource("/purchases.csv").getPath

  test("1. Check quantity of rows in test-CSV-file") {
    implicit val spark: SparkSession = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    val df = readCsvFile(clicksPath, clicksSchema)
    val expected = 16
    val actual = df.count()
    assert(actual == expected, s"Correct qnt of rows should be $expected, given $actual")
    spark.close()
  }

  test("2. Test sessionID") {
    implicit val spark: SparkSession = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val clicks = readCsvFile(clicksPath, clicksSchema)
    val actual1 = addSessionId(clicks)
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-1")
      .count()
    val actual2 = addSessionId(clicks)
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-2")
      .count()
    val actual3 = addSessionId(clicks)
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-3")
      .count()
    val actual4 = addSessionId(clicks)
      .filter($"sessionId" === "ba192cc2-f3e8-4871-9024-426da37bfafc-1")
      .count()
    spark.close()
    assert(actual1 == 4, s"SessionId isn't correct. SessionId = f6e8252f-...-1")
    assert(actual2 == 3, s"SessionId isn't correct. SessionId = f6e8252f-...-2")
    assert(actual3 == 5, s"SessionId isn't correct. SessionId = f6e8252f-...-3")
    assert(actual4 == 4, s"SessionId isn't correct. SessionId = ba192cc2-...-1")
  }

  test("3. Test prepareClickStreams"){
    implicit val spark: SparkSession = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val clicks = readCsvFile(clicksPath, clicksSchema)
    val actual = prepareClickStreams(addSessionId(clicks))
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-1")
      .select("campaignId")
      .collect()
      .map(_(0))
      .toList
      .head
      .toString
    spark.close()
    val expected = "478"
    assert(actual == expected, s"Campaign_id isn't correct")
  }

  test("4. Different solutions lead to the same output result"){
    implicit val spark: SparkSession = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")

    // Read & Pre-process input data
    val purchasesDf = readCsvFile(purchasesPath, purchasesSchema)
    val clicksDf = readCsvFile(clicksPath, clicksSchema)
    val preSessionDf = addSessionId(clicksDf)

    // Task 1.1 & 1.2
    val sessionsDf1 = prepareClickStreams(preSessionDf)
    val df1 = sessionsDf1.join(purchasesDf, Seq("purchaseId"))
    val sessionsDf2 = prepareClickStreams2(preSessionDf)
    val df2 = sessionsDf2.join(purchasesDf, Seq("purchaseId"))
    val actual1 = df1.except(df2).count() + df2.except(df1).count()

    // Task 2.1
    val PurchasesAttributionProjection = sessionsDf1.join(purchasesDf, Seq("purchaseId"))

    val df3 = top10campaignSql(PurchasesAttributionProjection)
    val df4 = top10campaignWithoutSql(PurchasesAttributionProjection)
    val actual2 = df3.except(df4).count() + df4.except(df3).count()

    // Task 2.2
    val df5 = popularChannelSql(PurchasesAttributionProjection)
    val df6 = popularChannelWithoutSql(PurchasesAttributionProjection)
    val actual3= df5.except(df6).count() + df6.except(df5).count()

    spark.close()

    assert(actual1 == 0, s"Output of Task 1.1 <> output of Task 1.2")
    assert(actual2 == 0, s"Task 2.1: Output of SQL-solution <> output of without-SQL-solution")
    assert(actual3 == 0, s"Task 2.2: Output of SQL-solution <> output of without-SQL-solution")
  }
}
