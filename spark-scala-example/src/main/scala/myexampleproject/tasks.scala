package myexampleproject

import myexampleproject.helpers._
import org.apache.spark.sql.types.DoubleType

object tasks extends App {

 val inputPath = args(0)  // path for folder with the input files
 val outputPath = args(1) // path for output files

 implicit val spark = getSparkSession
 spark.sparkContext.setLogLevel("ERROR")

 // Read input data
 val _purchasesDf = readCsvFile(s"$inputPath/user_purchases/")
 val clicksDf = readCsvFile(s"$inputPath/mobile_app_clickstream/")

 // Pre-process data
 val purchasesDf = changeColType(_purchasesDf, "billingCost", DoubleType)
 val preSessionDf = addSessionId(clicksDf).persist()

 // Task 1.1
 val sessionsDf = prepareClickStreams(preSessionDf)
 val PurchasesAttributionProjection = sessionsDf.join(purchasesDf, Seq("purchaseId")).persist()
 saveDF(PurchasesAttributionProjection, s"$outputPath/PurchasesAttributionProjection1")
 println("Task 1.1 finished")

 // Task 1.2
 val sessionsDf2 = prepareClickStreams2(preSessionDf)
 val PurchasesAttributionProjection2 = sessionsDf2.join(purchasesDf, Seq("purchaseId"))
 saveDF(PurchasesAttributionProjection2, s"$outputPath/PurchasesAttributionProjection2")
 println("Task 1.2 finished")

 // Task 2.1
 val top10CampaignsSqlDf = top10campaignSql(PurchasesAttributionProjection)
 saveDF(top10CampaignsSqlDf, s"$outputPath/top10CampaignsSQL")

 val top10campaignWithOutSqlDf = top10campaignWithOutSql(PurchasesAttributionProjection)
 saveDF(top10CampaignsSqlDf, s"$outputPath/top10campaignWithOutSql")
 println("Task 2.1 finished")

 // Task 2.2
 val popularChannelSqlDf = popularChannelSql(PurchasesAttributionProjection)
 saveDF(popularChannelSqlDf, s"$outputPath/popularChannelSql")

 val popularChannelWithOutSql = popularChannelSql(PurchasesAttributionProjection)
 saveDF(popularChannelWithOutSql, s"$outputPath/popularChannelWithOutSql")
 println("Task 2.2 finished")

 spark.close()
}
