package myсapstoneproject

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{concat, countDistinct, element_at, from_json, lit, max, rank, sum, when}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, MapType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession}

/**
 * Provides  functions, which contains the main logic for project
 * For more information - see description below for each function
 */
object Helpers {
  /**
   * Schema for data-file with purchases
   */
  val purchasesSchema = new StructType()
    .add("purchaseId", StringType, true)
    .add("purchaseTime", TimestampType, true)
    .add("billingCost", DoubleType, true)
    .add("isConfirmed", BooleanType, true)

  /**
   * Schema for data-file with clicks
   */
  val clicksSchema = new StructType()
    .add("userId", StringType, true)
    .add("eventId", StringType, true)
    .add("eventType", StringType, true)
    .add("eventTime", TimestampType, true)
    .add("attributes", StringType, true)

  // --------- Common Helpers ---------- //
  def getSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("my-capstone-project")
      .master("local")
      .getOrCreate()

  def readCsvFile(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame =
    spark.read
      .schema(schema)
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(path)

  /**
   * Save a DataFrame as parquet-file
   */
  def saveDF(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1) // we want to have 1 output file
      .write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath)
  }

  /**
   * Changes a type of column to necessary
   */
  def changeColType(df: DataFrame, columnName: String, columnType: DataType): DataFrame = {
    df.withColumn("tmpColumn", df(columnName).cast(columnType))
      .drop(columnName)
      .withColumnRenamed("tmpColumn", columnName)
  }

  /**
   * Calculates and adding sessionId & extracts data from json-string into new map-field
   *
   * In the our data actually we can use sessionID = userID because of one user has only one session.
   * But we suppose that it isn't so and will calculate sessionId "honestly"
   */
def addSessionId(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
  import spark.implicits._

  val eventTypeFlg: Column = {
    when ($"eventType" === "app_open", 1)
      .otherwise(0)
      .as("eventTypeFlg")
  }
  val windowSession = Window.partitionBy("userId").orderBy("eventTime")

  df.withColumn("eventTypeFlg", eventTypeFlg)
    .withColumn("userSessionId",sum($"eventTypeFlg").over(windowSession))
    .withColumn("sessionId", concat($"userId", lit("-"), $"userSessionId"))
    .withColumn("attr", from_json($"attributes",MapType(StringType, StringType)))
  }

  // --------- For Task 1.1 ---------- //

  /**
   * Prepare a DF for further processing (task 1.1):
   *  1) Takes data into separate columns from a single map-column
   *  2) Returns one row per each sessionId and only necessary columns
   */
  def prepareClickStreams(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowAttr = Window.partitionBy("sessionId")
    df
      //  extracting attributes from JSON-like string into columns
      .withColumn("pre_campaignId", element_at($"attr", "campaign_id"))
      .withColumn("pre_channelId", element_at($"attr", "channel_id"))
      .withColumn("pre_purchaseId", element_at($"attr", "purchase_id"))
      // getting 1 row for each session
      .withColumn("campaignId",max($"pre_campaignId").over(windowAttr))
      .withColumn("channelId",max($"pre_channelId").over(windowAttr))
      .withColumn("purchaseId",max($"pre_purchaseId").over(windowAttr))
      .filter($"eventTypeFlg" === 1)
      // select only necessary columns
      .select($"sessionId", $"campaignId", $"channelId", $"purchaseId")
  }

  // --------- For Task 1.2 ---------- //

  /**
   * Custom aggregator for task 1.2
   */
  type StringMap = Map[String, String]
  case class SessionAttrs(sessionID: String, attr: StringMap)

  val MapAggregator =  new Aggregator[SessionAttrs, StringMap, StringMap] {
    def zero: StringMap = Map.empty[String, String]
    def reduce(accum: StringMap, a: SessionAttrs): StringMap = accum ++ a.attr
    def merge(map1: StringMap, map2: StringMap): StringMap = map1 ++ map2
    def finish(result: StringMap): StringMap = result
    def bufferEncoder: Encoder[StringMap] = ExpressionEncoder()
    def outputEncoder: Encoder[StringMap] = ExpressionEncoder()
  }.toColumn

  /**
   * Prepare a DF for further processing (task 1.2)
   * Does the same as prepareClickStreams, but using custom aggregator
   */
  def prepareClickStreams2(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ds = df.select($"sessionId", $"attr")
      .na.drop()
      .as[SessionAttrs]

    ds.groupByKey(_.sessionID)
      .agg(MapAggregator.name("attrs"))
      .withColumn("campaignId", element_at($"attrs", "campaign_id"))
      .withColumn("channelId", element_at($"attrs", "channel_id"))
      .withColumn("purchaseId", element_at($"attrs", "purchase_id"))
      .withColumnRenamed("value", "sessionId")
      .select($"sessionId", $"campaignId", $"channelId", $"purchaseId")
  }

  // --------- For Task 2.1 ---------- //

  def top10campaignSql(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("PurchasesAttributionProjection")
    spark.sql("""
                Select campaignId, sum(billingCost) as total
                  from PurchasesAttributionProjection
                  where isConfirmed = true
                group by campaignId
                order by total desc
                limit 10
                """
    )
  }

  def top10campaignWithoutSql(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.filter($"isConfirmed" === true)
    .groupBy("campaignId")
    .sum("billingCost")
    .withColumnRenamed("sum(billingCost)", "total")
    .orderBy($"total".desc)
    .limit(10)
  }

  // --------- For Task 2.2 ---------- //

  def popularChannelSql(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("PurchasesAttributionProjection")
    spark.sql(  """
      with wt_sessionQnt
      as (
        Select campaignId, channelId,
               count(distinct(sessionId)) as sessionQnt
          from PurchasesAttributionProjection
        group by campaignId, channelId
        order by sessionQnt desc
      ),
      wt_ranking as (
        select campaignId, channelId, sessionQnt,
               ROW_NUMBER() over (partition by campaignId order by sessionQnt desc) as rnk
          from wt_sessionQnt
      )
      select campaignId,
             channelId as MostPopularChannel
        from wt_ranking
      where rnk = 1
      order by campaignId
      """)
  }

  def popularChannelWithoutSql(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window = Window.partitionBy($"campaignId").orderBy($"sessionQnt".desc)
    df.groupBy($"campaignId", $"channelId")
      .agg(countDistinct("sessionId").as("sessionQnt"))
      .withColumn("rnk", rank().over(window))
      .filter($"rnk" === 1)
      .select("campaignId", "channelId")
      .orderBy($"campaignId")
  }

}
