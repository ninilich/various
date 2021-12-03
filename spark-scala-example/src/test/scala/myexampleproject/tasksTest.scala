package myexampleproject

import org.junit.Test
import myexampleproject.helpers._
import org.apache.spark.sql.types.DoubleType

class tasksTest {

  @Test def `Check quantity of rows in CSV`: Unit = {
    implicit val spark = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    val df = readCsvFile("src/test/test_data/clicks.csv")
    val correct = 18
    val actual = df.count()
    spark.close()
    val res = actual == correct
    assert(res, s"Correct qnt of rows should be $correct, given $actual")
  }

  @Test def `Test sessionId`: Unit = {
    implicit val spark = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val clicks = readCsvFile("src/test/test_data/clicks.csv")
    val actual = addSessionId(clicks)
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-3")
      .count()
    spark.close()
    val correct = 5
    val res = actual == correct
    assert(res, s"SessionId is not correct. Found $actual rows, correct count of rows is $correct")
  }

  @Test def `Test "prepareClickStreams" function`: Unit = {
    implicit val spark = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val clicks = readCsvFile("src/test/test_data/clicks.csv")
    val actual = prepareClickStreams(addSessionId(clicks))
      .filter($"sessionId" === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e-1")
      .select("campaignId")
      .collect()
      .map(_(0))
      .toList
      .head
      .toString
    spark.close()
    val correct = "478"
    val res = actual == correct
    assert(res, s"Campaign_id is not correct. Found $actual, correct is $correct")
  }

  @Test def `Different solutions lead to the same output result`: Unit = {
    implicit val spark = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")

    // Read & Pre-process input data
    val _purchasesDf = readCsvFile("src/test/test_data/purchases.csv")
    val clicksDf = readCsvFile("src/test/test_data/clicks.csv")
    val purchasesDf = changeColType(_purchasesDf, "billingCost", DoubleType)
    val preSessionDf = addSessionId(clicksDf)

    // Task 1.1 & 1.2
    val sessionsDf1 = prepareClickStreams(preSessionDf)
    val df11 = sessionsDf1.join(purchasesDf, Seq("purchaseId"))
    val sessionsDf2 = prepareClickStreams2(preSessionDf)
    val df12 = sessionsDf2.join(purchasesDf, Seq("purchaseId"))
    val t11 = df11.except(df12).count()
    val t12 = df12.except(df11).count()

    // Task 2.1
    val PurchasesAttributionProjection = sessionsDf1.join(purchasesDf, Seq("purchaseId"))

    val df211 = top10campaignSql(PurchasesAttributionProjection)
    val df212 = top10campaignWithOutSql(PurchasesAttributionProjection)
    val t211 = df211.except(df212).count()
    val t212 = df212.except(df211).count()

    // Task 2.2
    val df221 = popularChannelSql(PurchasesAttributionProjection)
    val df222 = popularChannelWithOutSql(PurchasesAttributionProjection)
    val t221 = df221.except(df222).count()
    val t222 = df222.except(df221).count()

    spark.close()
    val correct = 0
    val actual = t11 + t12 + t211 + t212 + t221 + t222
    val res = actual == correct
    assert(res, s"Task 1: ${t11 == t12}, Task 2.1: ${t211 == t212}, Task 2.2: ${t221 == t222}")
  }
}
