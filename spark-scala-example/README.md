# Introduction

BigData Tasks: Marketing Analytics

You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
Now the business wants to know the efficiency of the campaigns and channels.
Let’s help them out!
Given datasets:

- Mobile App clickstream projection
```
userId: String
eventId: String
eventTime: Timestamp
eventType: String
attributes: Option[Map[String, String]]
```
   - event types:
```
app_open
search_product
view_product_details
purchase 
app_close
```

## Task #1:

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
- Task 1.1: Implement it by using a custom Aggregator

## Task #2:
Calculate Marketing Campaigns And Channels Statistics: 
 - Top Campaigns:  What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
 - Channels engagement performance: What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?

Requirements for task #2:
- Should be implemented by using plain SQL on top of Spark DataFrame API
- Will be a plus: an additional alternative implementation of the same tasks by using Spark Scala DataFrame / Datasets  API only (without plain SQL)

Full task description you can find [here](
https://docs.google.com/document/d/e/2PACX-1vTnYKo-FpJQ2GL_YgVIfZeTMiu5bnnH-aTbBaOyxdzl43T2zm5nhe4YYPd7c44xItTk8Ot8teVfuFqf/pub)

## General requirements

The requirements that apply to all tasks:
- Use Spark version 2.4.5 or higher
- The logic should be covered with unit tests
- The output should be saved as PARQUET files.
- Configurable input and output for both tasks

# Solution 

## Local run

1. Generate dataset - see [this repo](https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS) and follow instructions there.
2. Compile and run main-method 

When running main-method, you need to pass 2 parameters:
* inputPath - path with input data
* outputPath - path to save output data

F.e. running with sbt-shell:
```
run /input/path/ /output/path
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


## Dataproc run

1. Open Google Cloud shell and run script [GCP-prepare.sh]()
2. Package project and upload output jar-file "example-project_2.12-0.1.jar" to Coogle Cloud Storage bucket 
3. Run Spark job
   ```
   gcloud dataproc jobs submit spark \
   --jar=$BUCKET_NAME/example-project_2.12-0.1.jar \
   --cluster=$CLUSTER_NAME \
   --region=$REGION \
   -- \
   $BUCKET_NAME/capstone-dataset \
   $BUCKET_NAME/output
   ```
4. Check $BUCKET_NAME/output after job finished - here output data will be saved 
