## Introduction

BigData Tasks: Marketing Analytics

There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
Now the business wants to know the efficiency of the campaigns and channels.
Let’s help them out!

### Given datasets
- Mobile App clickstream data
```
userId: String
eventId: String
eventTime: Timestamp
eventType: String
attributes: Option[Map[String, String]]

// event types:
 app_open
 search_product
 view_product_details
 purchase 
 app_close

```

- Purchases

```
purchaseId: String
purchaseTime: Timestamp
billingCost: Double
isConfirmed: Boolean
```

### Task #1:

Build Purchases Attribution Projection. The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. 

The target schema:

```
purchaseId: String,
purchaseTime: Timestamp,
billingCost: Double,
isConfirmed: Boolean,

// a session starts with app_open event and finishes with app_close 
sessionId: String,
campaignId: String,  // derived from app_open#attributes#campaign_id
channelIid: String,    // derived from app_open#attributes#channel_id
```

Requirements for implementation of the projection building logic:
- Task 1.1: Implement it by utilizing default Spark SQL capabilities.
- Task 1.2: Implement it by using a custom Aggregator or UDAF.

### Task #2:
Use the Purchases Attribution Projection (see task 1) to build aggregates that provide the following insights:
- Task 2.1: Top Campaigns: what are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
- Task 2.2: Channels engagement performance: what is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?

Requirements for task #2:
- Should be implemented by using plain SQL on top of Spark DataFrame API
- Will be a plus: an additional alternative implementation of the same tasks by using Spark Scala DataFrame / Datasets  API only (without plain SQL)

## Solution

1. Generate dataset - see [this repo](https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS) and follow instructions there.
2. Compile and run main-method 

When running main-method, you need to pass 2 parameters:
* inputPath - path with input data
* outputPath - path to save output data

F.e. running with sbt-shell:
```
run /input/path /output/path
```
InputPath has to contain 2 folders with input data:
```
/input/Path/
|-- mobile_app_clickstream
|-- user_purchases
```

Output data will be saved in outputPath:
* Task 1.1 - folder "PurchasesAttributionProjection1"
* Task 1.2 - folder "PurchasesAttributionProjection2"
* Task 2.1 - folders "top10CampaignsSQL" and "top10campaignWithOutSql"
* Task 2.2 - folder "popularChannelSql" and "popularChannelWithOutSql"
