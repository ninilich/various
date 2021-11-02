package myсapstoneproject

import myсapstoneproject.Helpers._
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime

object Main extends App {

 val inputPath = args(0)  // path for folder with the input files
 val outputPath = args(1) // path for output files

 implicit val spark: SparkSession = getSparkSession
 spark.sparkContext.setLogLevel("ERROR")

 // Read input data
 val purchasesDf = readCsvFile(s"$inputPath/user_purchases/", purchasesSchema)
 val clicksDf = readCsvFile(s"$inputPath/mobile_app_clickstream/", clicksSchema)
 println(s"${LocalDateTime.now()}   Files read")

  // Pre-process data
  val preSessionDf = addSessionId(clicksDf).cache()

  // Task 1.1
  val sessionsDf = prepareClickStreams(preSessionDf)
  val PurchasesAttributionProjection = sessionsDf.join(purchasesDf, Seq("purchaseId")).cache()
  saveDF(PurchasesAttributionProjection, s"$outputPath/PurchasesAttributionProjection1")
  println(s"${LocalDateTime.now()}   Task 1.1 finished")

  // Task 1.2
  val sessionsDf2 = prepareClickStreams2(preSessionDf)
  val PurchasesAttributionProjection2 = sessionsDf2.join(purchasesDf, Seq("purchaseId"))
  saveDF(PurchasesAttributionProjection2, s"$outputPath/PurchasesAttributionProjection2")
  println(s"${LocalDateTime.now()}   Task 1.2 finished")

  // Task 2.1
  val top10CampaignsSqlDf = top10campaignSql(PurchasesAttributionProjection)
  saveDF(top10CampaignsSqlDf, s"$outputPath/top10CampaignsSQL")

  val top10campaignWithoutSqlDf = top10campaignWithoutSql(PurchasesAttributionProjection)
  saveDF(top10CampaignsSqlDf, s"$outputPath/top10campaignWithoutSql")
  println(s"${LocalDateTime.now()}   Task 2.1 finished")

  // Task 2.2
  val popularChannelSqlDf = popularChannelSql(PurchasesAttributionProjection)
  saveDF(popularChannelSqlDf, s"$outputPath/popularChannelSql")

  val popularChannelWithoutSql = popularChannelSql(PurchasesAttributionProjection)
  saveDF(popularChannelWithoutSql, s"$outputPath/popularChannelWithoutSql")
  println(s"${LocalDateTime.now()}   Task 2.2 finished")

 spark.close()
}
